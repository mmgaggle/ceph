// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/oob_read_delivery.h"

#include <algorithm>
#include <utility>

#include "common/ceph_context.h"
#include "common/crc64nvme.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "messages/MOSDOpReply.h"
#include "osd/oob_executor.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "oob_read: "

namespace ceph::osd::oob {

std::optional<read_push> plan_read_push(
  size_t idx, const OSDOp& op, const ceph::rdma::delivery_t& d,
  bool ec_direct, const std::optional<ec_direct_geometry>& ec)
{
  if (d.flags & ~ceph::rdma::delivery_t::KNOWN_FLAGS) {
    // flag bits we do not implement: deliver inline so future
    // semantics degrade safely
    return std::nullopt;
  }
  // only data-bearing reads go out of band; a descriptor on anything
  // else (guards, version reads, ...) is ignored and the op stays inline
  switch (op.op.op) {
  case CEPH_OSD_OP_READ:
  case CEPH_OSD_OP_SYNC_READ:
  case CEPH_OSD_OP_SPARSE_READ:
    break;
  default:
    return std::nullopt;
  }
  if (op.rval < 0 || !op.outdata.length()) {
    return std::nullopt;
  }

  read_push p;
  p.idx = idx;
  if (op.op.op == CEPH_OSD_OP_SPARSE_READ) {
    if (ec_direct) {
      // the fiemap extent map is in shard-offset space; interleaving
      // sparse data is a follow-up - deliver inline
      return std::nullopt;
    }
    bufferlist databl;
    try {
      auto bp = op.outdata.cbegin();
      decode(p.sparse_extents, bp);
      decode(databl, bp);
    } catch (const ceph::buffer::error&) {
      return std::nullopt;
    }
    p.plan = sparse_plan(d.base_offset, op.op.extent.offset,
			 p.sparse_extents, databl.length());
    p.payload = std::move(databl);
    p.sparse = true;
  } else if (ec_direct) {
    if (!ec) {
      return std::nullopt;
    }
    // the reply holds this shard's chunks in ascending stripe order;
    // scatter them to their logical positions in the client window
    p.plan = ec_direct_plan(d.base_offset, op.op.extent.offset,
			    op.op.extent.length, ec->chunk_size, ec->k,
			    ec->raw_shard, op.outdata.length());
    p.payload = op.outdata;
  } else {
    p.plan = linear_plan(d.base_offset, op.outdata.length());
    p.payload = op.outdata;
  }
  if (p.plan.empty()) {
    return std::nullopt;
  }
  p.want64 = d.flags & ceph::rdma::delivery_t::FLAG_CRC64NVME;
  p.want32c = d.flags & ceph::rdma::delivery_t::FLAG_CRC32C;
  return p;
}

void finish_op(OSDOp& op, ceph::rdma::oob_result_t& res,
	       const read_push& p, const push_result& r)
{
  if (r.r < 0) {
    // inline: declined if nothing of it went out
    res.flags |= ceph::rdma::attempt_flags(false, r.started, false);
    return;
  }
  // strip the delivered data from the reply; sparse reads keep their
  // extent map inline with an empty data blob
  op.outdata.clear();
  if (p.sparse) {
    encode(p.sparse_extents, op.outdata);
    encode(bufferlist(), op.outdata);
  }
  res.bytes = static_cast<uint64_t>(r.r);
  // with delivery-complete writes every byte is in the window: nothing of
  // this op lands after the reply, which follows
  res.flags |= ceph::rdma::attempt_flags(true, true, r.delivery_complete);
  if (p.want64 || p.want32c) {
    // checksum each placed range at the storage node, after it
    // crossed the fabric. Every triple is one contiguous logical
    // extent, so a caller holding all of a window's ranges can fold
    // them in offset order however they were interleaved across
    // shards; the whole-payload value comes from the same pass, since
    // the builders emit triples contiguous and ascending in payload
    // order.
    res.ranges.reserve(p.plan.size());
    uint64_t whole64 = 0;
    uint32_t whole32c = 0;
    for (const auto& t : p.plan) {
      bufferlist part;
      part.substr_of(p.payload, t.local_ofs, t.len);
      ceph::rdma::crc_range_t c{t.client_ofs, t.len};
      if (p.want64) {
	c.crc64 = ceph::crc64nvme(part);
	whole64 = res.ranges.empty() ? c.crc64
		: ceph::crc64nvme_combine(whole64, c.crc64, t.len);
      }
      if (p.want32c) {
	c.crc32c = ceph::rdma::crc32c_canonical(part);
	whole32c = res.ranges.empty() ? c.crc32c
		 : ceph::rdma::crc32c_combine(whole32c, c.crc32c, t.len);
      }
      res.ranges.push_back(c);
    }
    res.crc64 = whole64;
    res.crc32c = whole32c;
    res.flags |= ceph::rdma::oob_result_t::FLAG_RANGES |
      (p.want64 ? ceph::rdma::oob_result_t::FLAG_CRC64NVME : 0) |
      (p.want32c ? ceph::rdma::oob_result_t::FLAG_CRC32C : 0);
    if (p.plan.size() == 1) {
      // one contiguous logical extent: crc64 and crc32c themselves fold
      // with adjacent stripes, so a caller needs no ranges. This is the
      // plain read, and also the EC-direct read whose range fits in
      // one chunk of this shard.
      res.flags |= ceph::rdma::oob_result_t::FLAG_COMBINABLE;
    }
  }
}

oob_reply::oob_reply(MOSDOpReply* reply, std::vector<OSDOp>&& rops,
		     std::vector<ceph::rdma::oob_result_t>&& oob,
		     send_fn send)
  : reply(reply), rops(std::move(rops)), oob(std::move(oob)),
    send(std::move(send))
{
  ceph_assert(this->oob.size() == this->rops.size());
}

oob_reply::~oob_reply()
{
  if (reply) {
    // the guard was never dropped: nothing sends it now, and the client
    // resends
    reply->put();
  }
}

void oob_reply::get()
{
  // only while the guard or a push holds it: past the last put() the
  // reply is gone
  const unsigned before = pending.fetch_add(1, std::memory_order_relaxed);
  ceph_assert(before > 0);
}

void oob_reply::finish(const read_push& p, const push_result& r)
{
  ceph_assert(p.idx < rops.size());
  finish_op(rops[p.idx], oob[p.idx], p, r);
  put();
}

void oob_reply::put()
{
  // acq_rel: the last drop sees every push's slot
  const unsigned before = pending.fetch_sub(1, std::memory_order_acq_rel);
  ceph_assert(before > 0);
  if (before == 1) {
    send_reply();
  }
}

void oob_reply::send_reply()
{
  ceph_assert(reply);
  // report what was delivered, and what was declined: a client that
  // learns no transfer started for any op need not fence its window
  if (std::any_of(oob.begin(), oob.end(), [](const auto& r) {
	return r.bytes || r.flags;
      })) {
    reply->set_oob_results(std::move(oob));
  }
  reply->claim_ops(rops);
  send(std::exchange(reply, nullptr));
}

push_job::push_job(CephContext* cct, std::shared_ptr<oob_reply> join,
		   std::string key, read_push push, bool here)
  : cct(cct), join(std::move(join)), key(std::move(key)),
    push(std::move(push)), here(here)
{
  ceph_assert(this->push.exec);
  this->join->get();
}

push_job::~push_job()
{
  if (!finished) {
    // dropped without running: nothing of it started
    finish({-ECANCELED, false, false});
  }
}

void push_job::run()
{
  // an executor that cannot say counts as having started
  bool started = true;
  const ssize_t r = push.exec->execute_plan(key, push.token, push.payload,
					    push.plan, budget_left(push.bounds),
					    &started);
  debug_delay_done(cct);
  if (r < 0) {
    dout(10) << "op " << push.idx << " of " << key
	     << ": plan execution failed (" << r << "), delivering inline"
	     << dendl;
  }
  finish({r, started, push.exec->delivery_complete()});
}

void push_job::expire(int r)
{
  dout(10) << "op " << push.idx << " of " << key << ": not started ("
	   << r << "), delivering inline" << dendl;
  finish({r, false, false});
}

void push_job::finish(const push_result& r)
{
  ceph_assert(!finished);
  finished = true;
  join->finish(push, r);
}

void push_job::dump(ceph::Formatter* f) const
{
  f->dump_string("object", key);
  f->dump_unsigned("op", push.idx);
  f->dump_unsigned("bytes", push.payload.length());
  f->dump_unsigned("ranges", push.plan.size());
}

bool submit_pushes(CephContext* cct, Transfers* x,
		   const std::shared_ptr<oob_reply>& join,
		   const std::string& key, std::vector<read_push>&& pushes,
		   const std::function<bool(const OSDOobExecutor*)>& runs_inline)
{
  bool handed_off = false;
  for (auto& p : pushes) {
    const size_t idx = p.idx;
    // with transfer threads the push runs on one of them, unless its
    // executor keeps its transfers on the op threads (cuObject without
    // osd_oob_cuobj_transfer_threads)
    const bool here = runs_inline(p.exec);
    std::unique_ptr<xfer_job> job =
      std::make_unique<push_job>(cct, join, key, std::move(p), here);
    const int r = x ? x->submit(job) : -ESHUTDOWN;
    if (r < 0) {
      // refused, so nothing of it started: declined
      dout(10) << "op " << idx << " of " << key << ": push refused ("
	       << cpp_strerror(r) << "), delivering inline" << dendl;
      job->expire(r);
    } else if (!here && x->async()) {
      handed_off = true;
    }
  }
  return handed_off;
}

void hand_off(const std::shared_ptr<oob_reply>& join,
	      const std::function<void()>& let_go)
{
  let_go();
  // the guard goes last: the reply waits for it, so it cannot leave
  // while the request still holds anything
  join->put();
}

} // namespace ceph::osd::oob

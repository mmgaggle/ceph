// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/oob_pull.h"

#include <optional>
#include <utility>

#include "common/ceph_context.h"
#include "common/crc64nvme.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"
#include "osd/oob_executor.h"
#include "osd/oob_placement.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "oob_pull: "

namespace ceph::osd::oob {

namespace {

/// the payloads of pulls, together
uint64_t total_len(const std::vector<pull_op>& pulls)
{
  uint64_t n = 0;
  for (const auto& p : pulls) {
    n += p.len;
  }
  return n;
}

/**
 * Check the pulls of a request's ops, every one before any starts, and
 * fix each: 0, with the pulls in *pulls (none when no op pulls), or the
 * errno that fails the request with nothing started. The ops are
 * checked in order, each as the pull of one op always was, so the
 * first op that fails decides the errno.
 */
int plan_pulls(CephContext* cct, const std::string& key,
	       const std::vector<OSDOp>& ops,
	       const std::vector<ceph::rdma::delivery_t>& deliveries,
	       uint64_t max_write, const bounds_t& bounds,
	       const std::function<OSDOobExecutor*(const std::string&)>& puller_for,
	       std::vector<pull_op>* pulls)
{
  ceph_assert(deliveries.size() == ops.size());
  std::vector<pull_op> planned;
  for (size_t i = 0; i < ops.size(); i++) {
    const auto& d = deliveries[i];
    if (d.empty() || !d.is_pull()) {
      continue;
    }
    const OSDOp& osd_op = ops[i];
    const uint64_t len = osd_op.op.extent.length;
    if (osd_op.op.op != CEPH_OSD_OP_WRITE &&
	osd_op.op.op != CEPH_OSD_OP_WRITEFULL) {
      dout(5) << "op " << i << " " << osd_op << " of " << key
	      << " carries a pull descriptor, which only writes take" << dendl;
      return -EINVAL;
    }
    if (d.flags & ~ceph::rdma::delivery_t::PULL_KNOWN_FLAGS) {
      return -EOPNOTSUPP;
    }
    if (osd_op.indata.length() != 0) {
      return -EINVAL;
    }
    if (len == 0) {
      continue;
    }
    if (len > max_write) {
      return -EFBIG;
    }
    OSDOobExecutor* exec = puller_for(d.token);
    if (!exec) {
      dout(10) << "op " << i << " of " << key << ": no transport pulls "
	       << "from this token" << dendl;
      return -EOPNOTSUPP;
    }
    if (ceph::mono_clock::now() >= bounds.land_by) {
      return -ETIMEDOUT;
    }
    pull_op p;
    p.idx = i;
    p.len = len;
    p.base_offset = d.base_offset;
    p.want64 = d.flags & ceph::rdma::delivery_t::FLAG_CRC64NVME;
    p.want32c = d.flags & ceph::rdma::delivery_t::FLAG_CRC32C;
    p.exec = exec;
    p.token = d.token;
    planned.push_back(std::move(p));
  }
  *pulls = std::move(planned);
  return 0;
}

} // anonymous namespace

pull_set::pull_set(std::vector<size_t> pulled)
  : pulled(std::move(pulled))
{}

bool pull_set::settled() const
{
  std::lock_guard l(lock);
  return state != state_t::in_flight;
}

bool pull_set::consumed() const
{
  std::lock_guard l(lock);
  return state == state_t::consumed;
}

void pull_set::clear_declined(
  std::vector<ceph::rdma::oob_result_t>& oob) const
{
  std::lock_guard l(lock);
  auto clear = [&oob](size_t idx) {
    if (idx < oob.size()) {
      oob[idx].flags &= ~ceph::rdma::oob_result_t::FLAG_DECLINED;
    }
  };
  if (state == state_t::in_flight) {
    // any pull of the job may be reading its window now
    for (const size_t idx : pulled) {
      clear(idx);
    }
    return;
  }
  for (const auto& r : results) {
    if (r.started) {
      clear(r.idx);
    }
  }
}

void pull_set::settle(std::vector<pull_result>&& r)
{
  std::lock_guard l(lock);
  ceph_assert(state == state_t::in_flight);
  results = std::move(r);
  state = state_t::settled;
}

int pull_set::consume(std::vector<OSDOp>& ops,
		      std::vector<ceph::rdma::oob_result_t>& pulled)
{
  std::lock_guard l(lock);
  ceph_assert(state == state_t::settled);
  for (const auto& r : results) {
    if (r.r < 0) {
      // the request fails, and no write of it runs with its data
      return r.r;
    }
  }
  for (auto& r : results) {
    ceph_assert(r.idx < ops.size() && r.idx < pulled.size());
    ops[r.idx].indata = std::move(r.payload);
    pulled[r.idx] = std::move(r.oob);
  }
  state = state_t::consumed;
  return 0;
}

pull_job::pull_job(CephContext* cct, std::shared_ptr<pull_set> set,
		   std::string key, std::vector<pull_op> pulls,
		   bounds_t bounds, bool here)
  : cct(cct), set(std::move(set)), key(std::move(key)),
    ops(std::move(pulls)), bounds(bounds), here(here),
    bytes(total_len(ops))
{
  ceph_assert(this->set);
}

pull_job::~pull_job()
{
  if (!settled) {
    // dropped without running: nothing of it started
    fail(-EOPNOTSUPP);
  }
}

void pull_job::run()
{
  std::vector<pull_result> results(ops.size());
  bool failed = false;
  for (size_t k = 0; k < ops.size(); k++) {
    const auto& p = ops[k];
    auto& res = results[k];
    res.idx = p.idx;
    if (failed) {
      // all or nothing: the request fails, and no later pull starts
      continue;
    }
    if (ceph::mono_clock::now() >= bounds.post_by) {
      // a read that could land after the window's owner may reuse the
      // window does not start
      dout(5) << "op " << p.idx << " of " << key << ": too late to pull "
	      << p.len << " bytes" << dendl;
      res.r = -ETIMEDOUT;
      failed = true;
      continue;
    }
    pull(p, res);
    failed = res.r < 0;
  }
  debug_delay_done(cct);
  settle(std::move(results));
}

void pull_job::pull(const pull_op& p, pull_result& res)
{
  const auto plan = linear_plan(p.base_offset, p.len);
  ceph::buffer::list bl;
  // an executor that cannot say counts as having started
  bool started = true;
  const ssize_t r = p.exec->execute_pull(key, p.token, plan, p.len, &bl,
					 budget_left(bounds), &started);
  res.started = started;
  if (r < 0) {
    dout(5) << "op " << p.idx << " of " << key << ": pulling " << p.len
	    << " bytes failed: " << cpp_strerror(r)
	    << (started ? "" : ", nothing sent") << dendl;
    // a payload larger than the transport stages is not one it can pull
    res.r = r == -E2BIG ? -EOPNOTSUPP : static_cast<int>(r);
    return;
  }
  ceph_assert(bl.length() == p.len);
  auto& oob = res.oob;
  oob.bytes = p.len;
  if (p.want64 || p.want32c) {
    // the bytes as they arrived here, before the write encodes them
    if (p.want64) {
      oob.crc64 = ceph::crc64nvme(bl);
    }
    if (p.want32c) {
      oob.crc32c = ceph::rdma::crc32c_canonical(bl);
    }
    oob.ranges.push_back({p.base_offset, p.len, oob.crc64, oob.crc32c});
    oob.flags |= ceph::rdma::oob_result_t::FLAG_COMBINABLE |
      ceph::rdma::oob_result_t::FLAG_RANGES |
      (p.want64 ? ceph::rdma::oob_result_t::FLAG_CRC64NVME : 0) |
      (p.want32c ? ceph::rdma::oob_result_t::FLAG_CRC32C : 0);
  }
  dout(20) << "op " << p.idx << " of " << key << ": pulled " << p.len
	   << " bytes" << dendl;
  res.r = 0;
  res.payload = std::move(bl);
}

void pull_job::expire(int r)
{
  // nothing of it started. One that waited past its post_by can no
  // longer land in time; on any other the client sends the payloads
  // inline
  dout(10) << "pulls of " << key << " not started (" << cpp_strerror(r)
	   << ")" << dendl;
  fail(r == -ETIMEDOUT ? -ETIMEDOUT : -EOPNOTSUPP);
}

void pull_job::fail(int r)
{
  std::vector<pull_result> results(ops.size());
  for (size_t k = 0; k < ops.size(); k++) {
    results[k].idx = ops[k].idx;
    results[k].r = r;
  }
  settle(std::move(results));
}

void pull_job::settle(std::vector<pull_result>&& r)
{
  // a job runs or expires once
  ceph_assert(!settled);
  settled = true;
  set->settle(std::move(r));
}

void pull_job::dump(ceph::Formatter* f) const
{
  f->dump_string("object", key);
  f->dump_unsigned("ops", ops.size());
  f->dump_unsigned("bytes", bytes);
}

int pull_payloads(const pull_env& env, OpRequest& op,
		  std::vector<OSDOp>& ops)
{
  CephContext* cct = env.cct;
  auto m = op.get_req<MOSDOp>();
  const auto& deliveries = m->get_rdma_deliveries();
  if (deliveries.size() != ops.size()) {
    // the descriptor vector must mirror the ops; a write that needed a
    // pull then fails for its missing data
    return 0;
  }
  if (op.oob_pulled.size() != ops.size()) {
    op.oob_pulled.assign(ops.size(), {});
  }
  if (const auto& set = op.oob_pull; set) {
    // a run of the request after its pulls started. What they did is in
    // the set; the ops cannot tell, since a run may have changed them
    // (a write that a truncation overtook is cut short, its data too)
    if (set->consumed()) {
      return 0;
    }
    // a pull settles before its request runs on
    ceph_assert(set->settled());
    return set->consume(ops, op.oob_pulled);
  }
  const std::string& key = m->get_hobj().oid.name;
  // A pull places nothing in client memory, so the lease that bounds
  // read delivery does not protect anything here: a pull may start as
  // long as it can land. The same bound keeps the request from waiting
  // on a client that gave it up.
  auto bounds = make_bounds(m->get_recv_stamp(), env.lease, env.drain,
			    std::nullopt, ceph::signedspan::zero());
  bounds.post_by = bounds.land_by;
  std::vector<pull_op> pulls;
  if (int r = plan_pulls(cct, key, ops, deliveries, env.max_write, bounds,
			 env.puller_for, &pulls);
      r < 0) {
    return r;
  }
  if (pulls.empty()) {
    return 0;
  }
  // no payload exceeds max_write, and do_op fails a request whose
  // payloads and inline data together do: the sum does not wrap
  const uint64_t total = total_len(pulls);
  // The payloads are charged to the client message throttle as inline
  // ones are, all of them before any is pulled, so that what this and
  // the requests the messenger has already read pull stays within its
  // limit. With no room now the client sends the payloads inline, which
  // the messenger throttles before it reads them. The request gives the
  // bytes back when it is done, pulled or not.
  if (!op.reserve_pulled(env.client_throttle ? env.client_throttle() :
			 nullptr, total)) {
    dout(5) << key << ": the client message throttle has no room for "
	    << total << " pulled bytes" << dendl;
    return -EOPNOTSUPP;
  }
  std::vector<size_t> pulled;
  for (const auto& p : pulls) {
    pulled.push_back(p.idx);
  }
  auto set = std::make_shared<pull_set>(std::move(pulled));
  op.oob_pull = set;
  // the pulls run on this thread, whatever threads the service has, and
  // the request goes on with its payloads once they settled
  std::unique_ptr<xfer_job> job = std::make_unique<pull_job>(
    cct, set, key, std::move(pulls), bounds, true);
  if (const int r = env.xfer ? env.xfer->submit(job) : -ESHUTDOWN; r < 0) {
    dout(10) << "pulls of " << key << " refused (" << cpp_strerror(r)
	     << ")" << dendl;
    job->expire(r);
  }
  ceph_assert(set->settled());
  return set->consume(ops, op.oob_pulled);
}

} // namespace ceph::osd::oob

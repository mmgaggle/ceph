// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <sys/types.h>
#include <vector>

#include "common/rdma_token.h"
#include "include/buffer.h"
#include "include/common_fwd.h"
#include "osd/oob_placement.h"
#include "osd/oob_xfer.h"
#include "osd/osd_types.h"

class MOSDOpReply;
class OSDOobExecutor;
namespace ceph { class Formatter; }

/**
 * The read push: out-of-band delivery of a read reply's data into the
 * client windows its ops' descriptors name, apart from what needs the
 * PG.
 *
 * Under the PG lock, PrimaryLogPG::deliver_oob_prepare() checks the
 * request, plans each descriptor-bearing op's push (plan_read_push())
 * and fixes the bounds of its transfer. Each push then runs as a
 * push_job, and the reply waits in an oob_reply until every push is
 * over: the last one to finish sends it. PrimaryLogPG runs each push
 * on the thread that submits it, so the reply still leaves before the
 * op thread lets go of the object, as it always has.
 */
namespace ceph::osd::oob {

/// the stripe geometry the plan of an EC shard-direct read needs; known
/// only when the pool allows direct reads
struct ec_direct_geometry {
  uint64_t chunk_size = 0;
  uint32_t k = 0;
  uint32_t raw_shard = 0;  ///< this OSD's raw (data-order) shard index
};

/**
 * One descriptor-bearing op of a read, ready to push.
 * plan_read_push() fills in the plan; the PG adds the executor, the
 * token and the bounds.
 */
struct read_push {
  size_t idx = 0;  ///< the op's index in the request
  placement_plan plan;
  ceph::buffer::list payload;  ///< the bytes the plan indexes
  /// a SPARSE_READ: a placed one keeps its extent map in the reply
  bool sparse = false;
  std::map<uint64_t, uint64_t> sparse_extents;
  bool want64 = false;   ///< delivery_t::FLAG_CRC64NVME
  bool want32c = false;  ///< delivery_t::FLAG_CRC32C

  OSDOobExecutor* exec = nullptr;
  std::string token;
  bounds_t bounds;
};

/**
 * The push of read op idx of a request to the window that descriptor d
 * names, or nullopt when the op stays inline: d has flags we do not
 * implement, the op carries no read data, its extent map does not
 * decode, it is an EC shard-direct read that cannot be placed (sparse,
 * or ec is not given), or its plan places nothing. ec_direct says the
 * request reads one shard of an EC pool directly.
 */
std::optional<read_push> plan_read_push(
  size_t idx, const OSDOp& op, const ceph::rdma::delivery_t& d,
  bool ec_direct, const std::optional<ec_direct_geometry>& ec);

/// what the push of one op did
struct push_result {
  ssize_t r = -ECANCELED;  ///< the bytes placed, or a negative errno
  /// a write of it was handed to the transport
  bool started = false;
  /// OSDOobExecutor::delivery_complete() of the executor that placed it
  bool delivery_complete = false;
};

/**
 * Put the result of an op's push in the op and its oob result. A placed
 * op's data leaves the reply (a sparse read keeps its extent map, with
 * an empty data blob); res gets the bytes placed, landed when the
 * executor is delivery-complete, and the checksums of the placed
 * ranges that p wants. An op that was not placed keeps its data inline,
 * and is declined when nothing of it was started.
 */
void finish_op(OSDOp& op, ceph::rdma::oob_result_t& res,
	       const read_push& p, const push_result& r);

/**
 * The reply to a read whose ops are being pushed. It holds the reply,
 * with its ops and oob results swapped out, until every push is over,
 * and then sends it, exactly once.
 *
 * It counts references: one for the thread that builds it (the
 * guard), and one for each push_job. A job's finish() puts its result
 * in the op's slot, then drops its reference; whichever drop is the
 * last, on whichever thread, puts the ops and results back in the reply
 * and hands it to send. A push touches only its own op's slot, and the
 * builder touches no slot once a job holds a reference.
 */
class oob_reply {
public:
  /// sends the finished reply; consumes the message reference
  using send_fn = std::function<void(MOSDOpReply*)>;

  oob_reply(MOSDOpReply* reply, std::vector<OSDOp>&& rops,
	    std::vector<ceph::rdma::oob_result_t>&& oob, send_fn send);
  ~oob_reply();

  oob_reply(const oob_reply&) = delete;
  oob_reply& operator=(const oob_reply&) = delete;

  /// one more reference, for a push that will finish(); only while
  /// another reference is held, since the last put() sends the reply
  void get();
  /// the push p is over: finish_op() on its op, then put()
  void finish(const read_push& p, const push_result& r);
  /// drop a reference; the last one sends the reply
  void put();

private:
  void send_reply();

  MOSDOpReply* reply;
  std::vector<OSDOp> rops;
  std::vector<ceph::rdma::oob_result_t> oob;
  send_fn send;
  /// the guard, and one for each push not yet over
  std::atomic<unsigned> pending{1};
};

/**
 * The push of one op of a read: execute_plan() into the op's window,
 * then finish() it in the reply. A job that never runs (refused,
 * expired, shed, or dropped) starts nothing, and its op is declined.
 */
class push_job final : public xfer_job {
public:
  /// here: run on the thread that submits it (see inline_only())
  push_job(CephContext* cct, std::shared_ptr<oob_reply> join,
	   std::string key, read_push push, bool here);
  ~push_job() override;

  uint64_t pinned_bytes() const override { return push.payload.length(); }
  ceph::mono_clock::time_point post_by() const override {
    return push.bounds.post_by;
  }
  bool inline_only() const override { return here; }
  void run() override;
  void expire(int r) override;
  void dump(ceph::Formatter* f) const override;

private:
  void finish(const push_result& r);

  CephContext* const cct;
  std::shared_ptr<oob_reply> join;
  const std::string key;  ///< the object's name, for the executor
  const read_push push;
  const bool here;
  bool finished = false;
};

} // namespace ceph::osd::oob

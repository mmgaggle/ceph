// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cerrno>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/ceph_mutex.h"
#include "common/rdma_token.h"
#include "include/buffer.h"
#include "include/common_fwd.h"
#include "osd/oob_xfer.h"
#include "osd/osd_types.h"

class OSDOobExecutor;
class Throttle;
struct OpRequest;
namespace ceph { class Formatter; }

/**
 * The pulled write: the primary takes the payload of each write that
 * carries a pull descriptor (delivery_t::FLAG_PULL) out of the client's
 * window, and the write then runs as if the client had sent the bytes.
 *
 * Under the PG lock, with the object's lock held, pull_payloads()
 * checks every pull of the request before any of them starts, charges
 * their bytes to the client message throttle once, and creates the
 * request's pull_set (OpRequest::oob_pull). A pull_job runs the pulls
 * one after another, with the checksums their descriptors ask for, and
 * settles the set; the op thread then takes the payloads into the ops.
 * The set stays with the request: a run of the request after the pulls
 * started finds it, and neither pulls nor charges the throttle again.
 *
 * The job goes through the transfer service, but runs on the thread
 * that submits it, whatever threads the service has: a pull holds its
 * op thread, the PG and the object's lock until it settles, as it
 * always has.
 */
namespace ceph::osd::oob {

/// the pull of one write, checked and fixed under the PG lock
struct pull_op {
  size_t idx = 0;  ///< the op's index in the request
  uint64_t len = 0;  ///< the payload's bytes
  uint64_t base_offset = 0;  ///< where the payload starts in the window
  bool want64 = false;   ///< delivery_t::FLAG_CRC64NVME
  bool want32c = false;  ///< delivery_t::FLAG_CRC32C
  OSDOobExecutor* exec = nullptr;
  std::string token;
};

/// what the pull of one write did
struct pull_result {
  size_t idx = 0;  ///< the op's index in the request
  /// 0 when the payload is here, else the errno that fails the request
  int r = -ECANCELED;
  /// a read of it was handed to the transport
  bool started = false;
  /// the payload, when r is 0
  ceph::buffer::list payload;
  /// its byte count and checksums, for the reply (OpRequest::oob_pulled)
  ceph::rdma::oob_result_t oob;
};

/**
 * The pulls of one request, and what they did. Created when a run of
 * the request starts them, kept with the request (OpRequest::oob_pull)
 * and never replaced. The job that runs the pulls settles it; the op
 * thread then takes the payloads (consume()), under the PG lock.
 */
class pull_set {
public:
  /// pulled: the index of each op that the request's pulls are for, in
  /// op order
  explicit pull_set(std::vector<size_t> pulled);
  pull_set(const pull_set&) = delete;
  pull_set& operator=(const pull_set&) = delete;

  /// the job is over: every pull completed, failed, or never started
  bool settled() const;
  /// the ops took the payloads: a later run of the request takes the
  /// ops as they are, which the request may have changed since
  bool consumed() const;
  /**
   * A reply that fails the request says nothing of a pull that reached
   * the transport, as it would not of a push cut off: clear the
   * declined flag of each such op in oob, the request's oob results.
   * Nothing of the others read the window, and they stay declined.
   * Until the set settles, any of its pulls may be reading a window:
   * none of its ops stays declined.
   */
  void clear_declined(std::vector<ceph::rdma::oob_result_t>& oob) const;
  /// the job's end, exactly once: the result of each pull, in op order
  void settle(std::vector<pull_result>&& r);
  /**
   * Under the PG lock, once settled; all or nothing. When every pull
   * completed, move each payload into its op's indata and its result
   * into pulled, which is aligned with ops, and return 0. Else return
   * the errno of the first pull that failed, and leave ops and pulled
   * as they are.
   */
  int consume(std::vector<OSDOp>& ops,
	      std::vector<ceph::rdma::oob_result_t>& pulled);

private:
  enum class state_t {
    in_flight,
    settled,
    consumed,
  };

  mutable ceph::mutex lock = ceph::make_mutex("oob::pull_set::lock");
  const std::vector<size_t> pulled;
  state_t state = state_t::in_flight;
  std::vector<pull_result> results;
};

/**
 * The pulls of one request: execute_pull() of each in op order, out of
 * its window, then the checksums its descriptor asks for, and at last
 * the set settles. All or nothing: after a pull that failed, or one
 * that could not land by post_by any more, no later pull starts. A job
 * that never runs (refused, expired, shed, or dropped) starts nothing,
 * and fails the request: -ETIMEDOUT when it expired, else -EOPNOTSUPP,
 * on which the client sends the payloads inline.
 */
class pull_job final : public xfer_job {
public:
  /// pulls: the request's, in op order; bounds: its post_by is its
  /// land_by, since a pull may start as long as it can land; here: run
  /// on the thread that submits it (see inline_only())
  pull_job(CephContext* cct, std::shared_ptr<pull_set> set, std::string key,
	   std::vector<pull_op> pulls, bounds_t bounds, bool here);
  ~pull_job() override;

  pull_job(const pull_job&) = delete;
  pull_job& operator=(const pull_job&) = delete;

  uint64_t pinned_bytes() const override { return bytes; }
  ceph::mono_clock::time_point post_by() const override {
    return bounds.post_by;
  }
  bool pulls() const override { return true; }
  bool inline_only() const override { return here; }
  void run() override;
  void expire(int r) override;
  void dump(ceph::Formatter* f) const override;

private:
  /// the pull of p, into res
  void pull(const pull_op& p, pull_result& res);
  /// nothing started: every pull fails with r
  void fail(int r);
  void settle(std::vector<pull_result>&& r);

  CephContext* const cct;
  const std::shared_ptr<pull_set> set;
  const std::string key;  ///< the object's name, for the executor
  const std::vector<pull_op> ops;
  const bounds_t bounds;
  const bool here;
  const uint64_t bytes;  ///< the payloads, together
  bool settled = false;
};

/// what the pull step of a request takes from the PG and the OSD
struct pull_env {
  CephContext* cct = nullptr;
  /// the transfer service, or null, which refuses the pulls
  Transfers* xfer = nullptr;
  /// the executor that can pull from a token's window, or null
  std::function<OSDOobExecutor*(const std::string&)> puller_for;
  /// the throttle of the client's messages, which the payloads are
  /// charged to, or null; asked only of a request that pulls
  std::function<Throttle*()> client_throttle;
  /// osd_max_write_size, in bytes: no one payload may exceed it
  uint64_t max_write = 0;
  /// the pool's delivery lease and drain
  double lease = 0;
  double drain = 0;
};

/**
 * The pull step of a write request, under the PG lock, with the
 * object's lock held, before the request runs. For each op in ops, the
 * ops the request runs, that carries a pull descriptor: take its
 * payload out of the client's window into its indata, and its byte
 * count and checksums into OpRequest::oob_pulled, for the reply. The
 * first run of the request checks every pull, then charges the
 * payloads to the client throttle, then starts them; a later run finds
 * what they did in OpRequest::oob_pull, and pulls nothing. Returns 0,
 * or a negative errno that fails the whole request, with nothing
 * written.
 */
int pull_payloads(const pull_env& env, OpRequest& op,
		  std::vector<OSDOp>& ops);

} // namespace ceph::osd::oob

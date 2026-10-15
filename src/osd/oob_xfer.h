// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "common/perf_counters_collection.h"
#include "include/common_fwd.h"
#include "include/utime.h"

namespace ceph { class Formatter; }
namespace ceph::rdma {
struct delivery_t;
struct oob_result_t;
}

/**
 * The OSD's out-of-band transfer service: threads of its own that run
 * the transfers of read pushes, EC shard pushes and pulled writes, so
 * that no op worker thread, and no PG lock, waits on the fabric.
 *
 * The op thread does the checks and the plan under the PG lock, fixes
 * the transfer's bounds, and submits a job. A transfer thread calls the
 * executor, which blocks as it always has, and the job then answers:
 * it sends the reply, or queues the op back to its PG. With no threads
 * (osd_oob_transfer_threads = 0) a job runs on the thread that submits
 * it, before submit() returns, which is how every transfer ran before.
 */
namespace ceph::osd::oob {

/// when a transfer may start, and when it must be over, on the
/// monotonic clock, so that no step of the wall clock moves them
struct bounds_t {
  ceph::mono_clock::time_point post_by;  ///< nothing is handed to the transport after this
  ceph::mono_clock::time_point land_by;  ///< every operation lands or is cut off by this
};

/**
 * The bounds of a transfer of a request received at received, under
 * the PG lock: post_by is receipt plus the pool's delivery lease, and
 * not past the PG's readable_until when given (a read push); land_by
 * is receipt plus the lease plus the drain. received is on the
 * monotonic clock (OpRequest::get_mono_recv_stamp()), so that no step
 * of the wall clock since receipt moves them; readable_until and mnow
 * are OSDService::get_mnow() spans. A pulled write counts from when it
 * plans its pulls, and may start as long as it can land: its caller
 * passes that time, and sets post_by to land_by.
 */
bounds_t make_bounds(ceph::mono_clock::time_point received, double lease,
		     double drain,
		     std::optional<ceph::signedspan> readable_until,
		     ceph::signedspan mnow);
/// as above, for a request received at recv_stamp on the wall clock:
/// its age is taken now, so a step of the clock since receipt moves the
/// bounds by as much
bounds_t make_bounds(utime_t recv_stamp, double lease, double drain,
		     std::optional<ceph::signedspan> readable_until,
		     ceph::signedspan mnow);

/// how long after receipt the writes of a transfer may still land: the
/// pool's delivery lease plus its drain (make_bounds()'s land_by). A
/// peer that holds the token of a window lent to it may write into the
/// window that long. Inline, so that ECCommon.cc, which crimson-osd
/// builds too, needs nothing of oob_xfer.cc, which crimson does not
inline ceph::timespan delivery_window(double lease, double drain)
{
  return ceph::make_timespan(lease + drain);
}

/// true once b's post_by passed: the pool's delivery lease ran out, or
/// the PG's read lease that make_bounds() took, and no transfer within
/// b may start any more
bool lease_expired(const bounds_t& b);

/// the remaining budget at the moment of the call, for
/// execute_plan()/execute_pull(); never negative
std::chrono::milliseconds budget_left(const bounds_t& b);

/// mark declined the result of each op that carries a descriptor in
/// deliveries: no transfer of it started, so nothing of it reached its
/// window, and its client need not fence the window. oob holds one
/// result for each op of the request
void decline_all(const std::vector<ceph::rdma::delivery_t>& deliveries,
		 std::vector<ceph::rdma::oob_result_t>& oob);

/// osd_debug_oob_delay_done_ms: a job sleeps this long after the
/// executor call, before it answers
void debug_delay_done(CephContext* cct);

/**
 * A transfer the service runs: exactly one of run() or expire() is
 * called for each job that submit() accepted, and the service then
 * destroys it.
 *
 * pinned_bytes(), post_by(), pulls() and dump() read only what was
 * fixed before submit(): dump_oob_transfers calls them while the job
 * runs.
 */
struct xfer_job {
  virtual ~xfer_job() = default;
  /// payload and staging the job holds until it is destroyed
  virtual uint64_t pinned_bytes() const = 0;
  /// the job is not started after this
  virtual ceph::mono_clock::time_point post_by() const = 0;
  /// a pull (oob_xfer.pull_lat) rather than a push (push_lat)
  virtual bool pulls() const { return false; }
  /// run on the thread that submits it even when the service has
  /// threads: its executor keeps state that only op threads have
  /// (cuObject without osd_oob_cuobj_transfer_threads)
  virtual bool inline_only() const { return false; }
  /// transfer thread, or inline; may block until land_by
  virtual void run() = 0;
  /// instead of run(), posting nothing: -ETIMEDOUT when the job was
  /// taken past its post_by, -ESHUTDOWN when stop() shed it, which the
  /// job answers as a refused submit
  virtual void expire(int r) = 0;
  /// what the job moves, for dump_oob_transfers
  virtual void dump(ceph::Formatter* f) const {}
};

enum {
  l_oob_xfer_first = 21000,
  l_oob_xfer_queued,
  l_oob_xfer_running,
  l_oob_xfer_refused,
  l_oob_xfer_expired,
  l_oob_xfer_shed,
  l_oob_xfer_queue_lat,
  l_oob_xfer_push_lat,
  l_oob_xfer_pull_lat,
  l_oob_xfer_last,
};

class Transfers {
public:
  /**
   * threads transfer threads, none to run every job inline. A job is
   * refused while max_jobs are queued or running, or when the payload
   * they pin would pass max_bytes. thread_grace is the heartbeat grace
   * of a transfer thread, past the longest transfer
   * (osd_oob_transfer_thread_timeout); a thread past it makes the OSD
   * unhealthy, and is never killed. idle runs on a transfer thread that
   * found nothing to run for 100 ms.
   */
  Transfers(CephContext* cct, unsigned threads, unsigned max_jobs,
	    uint64_t max_bytes, ceph::timespan thread_grace,
	    std::function<void()> idle);
  ~Transfers();

  Transfers(const Transfers&) = delete;
  Transfers& operator=(const Transfers&) = delete;

  void start();
  /// 0: accepted, run() or expire() happens exactly once (inline before
  /// return when threads == 0 or the job is inline_only()). -EBUSY over a
  /// cap; -ESHUTDOWN once stopping for a job that would wait for a
  /// thread, and once stopped for any: job untouched, nothing started.
  int submit(std::unique_ptr<xfer_job>& job);
  /// refuse later jobs that would wait for a thread; a job that runs on
  /// the thread that submits it still runs, as it did before there were
  /// transfer threads
  void start_stopping();
  /// refuse later jobs, shed the queued ones, wait for the running
  /// ones, each until its land_by, and join the threads; a job that ran
  /// on the thread that submitted it is destroyed before this returns
  void stop();
  /// threads > 0
  bool async() const { return nthreads > 0; }
  void dump(ceph::Formatter* f) const;
  PerfCounters* get_logger() const { return logger.get(); }

private:
  using mono_time = ceph::mono_clock::time_point;

  struct queued_t {
    std::unique_ptr<xfer_job> job;
    mono_time submitted;
    uint64_t bytes;
  };
  struct running_t {
    const xfer_job* job;
    mono_time submitted;
    mono_time started;
    uint64_t bytes;
    bool here;  ///< on the thread that submitted it
  };

  void worker(unsigned i);
  /// run() the job, or expire() it when it is past its post_by
  void dispatch(xfer_job& job);

  CephContext* cct;
  const unsigned nthreads;
  const unsigned max_jobs;
  const uint64_t max_bytes;
  const ceph::timespan thread_grace;
  const std::function<void()> idle;
  PerfCountersRef logger;

  mutable ceph::mutex lock = ceph::make_mutex("oob::Transfers::lock");
  ceph::condition_variable cond;
  /// stop() waits here for the jobs that run on the threads that
  /// submitted them
  ceph::condition_variable done_cond;
  std::deque<queued_t> queue;
  std::list<running_t> running;
  /// jobs the threads took, which count against max_jobs
  unsigned threads_running = 0;
  /// jobs that run on the threads that submitted them, until they are
  /// destroyed
  unsigned inline_running = 0;
  /// bytes pinned by the queued jobs and those the threads took
  uint64_t pinned = 0;
  bool stopping = false;
  bool stop_threads = false;
  std::vector<std::thread> threads;
};

} // namespace ceph::osd::oob

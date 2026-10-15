// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "acconfig.h"
#include "include/common_fwd.h"

#include "common/oob_arena.h"
#include "rgw_rdma_rc_transport.h"
#include "rgw_rdma_rc_wire.h"

namespace rgw::rdma::dc { class Target; }
#include "common/ofi_rma_fwd.h"

/**
 * hipobj-rc-v2 session service.
 *
 * A session is one transfer: created by PREPARE, driven by READY,
 * ended by the FINAL reply, CANCEL, or the expiry reaper. Each
 * session owns an RC Connection and one buffer of the arena, registered
 * for that session alone. For a GET the buffer is where the object
 * lands (pushed by the OSDs straight into it when the gateway's DC
 * target or a libfabric window is available, copied in otherwise)
 * before the RC push to the client; for a PUT it is the staging region
 * the client writes into.
 *
 * The REST handlers call in on radosgw request threads; the reaper
 * runs on its own thread, and tears down the queue pair and the buffer
 * of every session that ended, so that the request that ended a
 * session does not wait for that. The service mutex covers the session
 * table and state changes; the data phase runs outside it with an io
 * reference pinning the session.
 */
namespace rgw::rdma::rc {

using clock = std::chrono::steady_clock;

/// a session's buffer: a slice of the arena, which goes back to the
/// arena when the session ends, once nothing can write into it
struct Buffer {
  ceph::oob::Arena::slice_t slice;
  void* ptr = nullptr;
  /// the session's size, which every registration covers exactly
  size_t size = 0;
  /// a PUT's own protection domain, which only mr and the session's
  /// queue pair are on; null for a GET, whose mr is on the device's
  ibv_pd* pd = nullptr;
  ibv_mr* mr = nullptr;
  /// delivery descriptor OSDs write this window through: a cuObject DC
  /// descriptor or a libfabric one; empty when OSD-direct is off
  std::string osd_token;
  /// the buffer's libfabric window, when osd_token is a libfabric one
  uint64_t ofi_window = 0;
  bool ofi_registered = false;
  /// an OSD may still push into a window after its relay ended (see
  /// Service::finish()); the buffer stays out of the arena until this
  /// passes
  clock::time_point held_until = clock::time_point::min();
};

enum class Op : uint8_t { GET = 0, PUT = 1 };

enum class State : uint8_t {
  PREPARED,      ///< waiting for READY
  TRANSFERRING,  ///< a READY handler owns the data phase
};

/// what a READY data phase ended with
enum class Outcome : uint8_t {
  OK,
  BUSY,         ///< peer not ready; session returns to PREPARED (409)
                ///< unless an OSD write of the data phase may still
                ///< land in its buffer
  TIMEOUT,
  VERIFY_FAIL,  ///< completion did not match the session
  WIRE_FAIL,
  BACKEND_FAIL, ///< the object store side failed
};

struct Session {
  std::string id;
  std::string principal;
  Op op = Op::GET;
  std::string target;    ///< canonical x-amz-rdma-target value
  uint64_t size = 0;
  uint64_t offset = 0;
  uint32_t cookie = 0;
  uint32_t client_psn = 0;
  uint32_t server_psn = 0;

  PeerEndpoint peer;           ///< gid/lid from PREPARE, qpn/psn at READY
  uint64_t client_mr_addr = 0; ///< client window for a GET push
  uint32_t client_mr_rkey = 0;

  Connection conn;
  std::unique_ptr<Buffer> buf;

  State state = State::PREPARED;
  clock::time_point deadline;  ///< PREPARE or exec deadline by state
  int io_refs = 0;             ///< handlers pinning the session
  bool reap_pending = false;
  Outcome last_outcome = Outcome::OK;
  /// how long an OSD write of the data phase may still land in buf, as
  /// finish() was told; the teardown releases the buffer with it
  std::chrono::milliseconds quarantine{};
  /// its place in the order sessions ended in, once it left the table
  uint64_t ended_seq = 0;
};

/// service-level errors; the REST layer maps them to HTTP statuses
enum class Error {
  OK,
  ARG,          ///< malformed request (400)
  STATE,        ///< wrong state for the call; session preserved (409)
  SESSION,      ///< owner or cookie mismatch; session preserved (404)
  STALE,        ///< session gone or already terminal (404)
  NO_SESSION,   ///< unknown id (404)
  LIMIT,        ///< session or per-user limit reached (503)
  NO_BUFFER,    ///< no room in the arena (503)
  TOO_LARGE,    ///< transfer exceeds the largest buffer (413)
  WIRE,         ///< verbs failure (500)
  UNSUPPORTED,  ///< no RDMA device (501)
  INTERNAL,
};

struct PrepareRequest {
  std::string principal;
  Op op = Op::GET;
  std::string target;
  uint64_t size = 0;
  uint64_t offset = 0;
  uint32_t cookie = 0;
  uint32_t client_psn = 0;
  std::string client_token;  ///< 88-hex, or all zeroes for loopback
};

struct PrepareReply {
  std::string session_id;
  std::string server_token;  ///< 88-hex
  uint32_t server_psn = 0;
  uint32_t server_qpn = 0;
  uint64_t staging_addr = 0; ///< PUT: where the client writes
  uint32_t staging_rkey = 0;
};

struct ReadyRequest {
  std::string principal;
  std::string session_id;
  uint32_t cookie = 0;
  uint32_t client_qpn = 0;
  uint64_t client_mr_addr = 0;
  uint32_t client_mr_rkey = 0;
};

struct SessionInfo {
  Op op = Op::GET;
  std::string target;
  uint64_t size = 0;
  uint64_t offset = 0;
};

/// the session limits. A session counts from the PREPARE that takes it
/// until its reap, also while prepare() builds it outside the service
/// lock, so that concurrent PREPAREs cannot all pass the limit. Takes no
/// lock; the service calls in under its own
class Limits {
 public:
  Limits() = default;
  Limits(uint32_t max, uint32_t max_per_user)
    : max_sessions(max), max_user_sessions(max_per_user) {}

  /// count a session of principal; false when the gateway or the
  /// principal is at its limit
  bool take(const std::string& principal) {
    if (total >= max_sessions) {
      return false;
    }
    auto& n = per_user[principal];
    if (n >= max_user_sessions) {
      if (n == 0) per_user.erase(principal);
      return false;
    }
    ++n;
    ++total;
    return true;
  }
  /// a session that take() counted ended
  void release(const std::string& principal) {
    auto it = per_user.find(principal);
    if (it == per_user.end()) {
      return;
    }
    --total;
    if (--it->second == 0) {
      per_user.erase(it);
    }
  }
  uint32_t sessions() const { return total; }

 private:
  uint32_t max_sessions = 0;
  uint32_t max_user_sessions = 0;
  uint32_t total = 0;
  std::map<std::string, uint32_t> per_user;
};

/// the order sessions end in, and which of their teardowns run outside
/// the service lock, by the reaper or by a PREPARE short of a buffer. A
/// PREPARE waits only for the sessions that ended before it found the
/// arena full, not for those that end meanwhile: under a steady stream
/// of transfers, those never run out. Takes no lock; the service calls
/// in under its own
class Teardowns {
 public:
  /// a session ended; its place in the order
  uint64_t ended() { return ++last_ended; }
  /// the place of the last session that ended
  uint64_t last() const { return last_ended; }
  /// the teardown of the session that ended at seq begins, or is done
  void begin(uint64_t seq) { running.insert(seq); }
  void end(uint64_t seq) { running.erase(seq); }
  /// whether the teardown of a session that ended by seq still runs
  bool running_upto(uint64_t seq) const {
    return !running.empty() && *running.begin() <= seq;
  }

 private:
  uint64_t last_ended = 0;
  std::set<uint64_t> running;
};

class Service {
 public:
  /// arena: the gateway's out-of-band memory, which the session buffers
  /// come from; it must outlive the service
  static int init(CephContext* cct, ceph::oob::Arena* arena);
  static void shutdown();
  /// null until init() succeeded
  static Service* get();

  ~Service();

  /// true once the device is open
  bool available() const { return dev.is_open(); }
  /// true when the gateway exposes its windows to the OSDs, over a DC
  /// target or libfabric
  bool osd_direct() const;
  uint32_t queue_depth() const { return send_depth; }

  /// routing lookup before authentication: the target and op of a
  /// session, without the owner check (READY re-checks it)
  Error peek(const std::string& id, SessionInfo& out);

  Error prepare(const PrepareRequest& req, PrepareReply& reply);

  /// claim the transfer: PREPARED -> TRANSFERRING, pair the queue pair
  /// against the client, and (PUT) arm the receive. On success *out is
  /// pinned until finish() and may be used from the calling thread.
  Error claim(const ReadyRequest& req, Session** out);

  /// end the data phase. BUSY re-arms the queue pair and returns to
  /// PREPARED for another READY; OK and anything else end the session,
  /// and s must not be used after. A session that ends with writes still
  /// in its send queue loses its queue pair before this returns.
  /// quarantine keeps the buffer out of the arena for that long after
  /// teardown (a relay that may still have OSD writes in flight), and
  /// the libfabric key quarantine at least when it had a libfabric
  /// window. A BUSY with a quarantine reaps too: another READY would
  /// relay through the same buffer while those writes may still land in
  /// it.
  void finish(Session* s, Outcome outcome, uint64_t bytes,
              std::chrono::milliseconds quarantine = {});

  Error cancel(const std::string& id, const std::string& principal);

 private:
  Service() = default;
  int do_init(CephContext* cct);
  void do_shutdown();

  /// a buffer of the arena for one session of op and size bytes,
  /// registered for that session alone: writable from the network only
  /// by whoever writes into it (the client of a PUT, through the queue
  /// pair that shares the buffer's own protection domain; the OSDs of a
  /// relay through the DC target), and, for a GET with OSD-direct relay,
  /// exposed to the OSDs. Null when the arena has no room. Takes no lock
  /// of the service.
  std::unique_ptr<Buffer> make_buffer(Op op, size_t size);
  /// make_buffer() again for a PREPARE that found the arena full, as the
  /// sessions that ended until then give their buffers back: those still
  /// queued are torn down here, one at a time until the buffer fits, and
  /// those torn down elsewhere waited for. Null when it still does not
  /// fit. Takes the lock
  std::unique_ptr<Buffer> make_buffer_from_ended(Op op, size_t size);
  /// close what exposes the buffer of a session whose queue pair is gone,
  /// and give it back to the arena; or, when an OSD write may still land
  /// in it, return it to be held until its held_until. Takes no lock of
  /// the service.
  std::unique_ptr<Buffer> release_buffer(std::unique_ptr<Buffer> b,
                                         std::chrono::milliseconds quarantine);
  /// deregister the buffer and give its slice back to the arena; a slice
  /// whose registration does not go away never goes back
  void free_buffer(Buffer& b);
#ifdef WITH_OOB_OFI
  /// whether closing a libfabric window revokes its key
  bool ofi_close_revokes_key();
#endif
  std::string new_session_id();

  /// erase and hand to the reaper; caller holds the lock
  void reap_locked(const std::string& id);
  /// tear down the sessions that ended until now, one at a time outside
  /// the lock; caller holds it
  void tear_down_ended(std::unique_lock<std::mutex>& lock);
  /// tear down the session that ended first, outside the lock; caller
  /// holds it, and ended is not empty
  void tear_down_one(std::unique_lock<std::mutex>& lock);
  /// destroy the queue pair of a session that ended, then release its
  /// buffer; a buffer to hold, as release_buffer() gives. Takes no lock
  /// of the service.
  std::unique_ptr<Buffer> tear_down(Session& s);
  void reaper_loop();

  CephContext* cct = nullptr;
  Device dev;
#ifdef HAVE_MLX5DV
  std::unique_ptr<dc::Target> dct;
#endif
#ifdef WITH_OOB_OFI
  std::unique_ptr<ceph::ofi::Endpoint> ofi_ep;
  /// false when a closed window's key keeps working, as with a
  /// provider's registration cache; every relay buffer is then held for
  /// the key quarantine
  bool ofi_revokes_keys = false;
#endif
  ceph::oob::Arena* arena = nullptr;
  size_t buf_size = 0;

  uint32_t send_depth = 0;
  std::chrono::milliseconds t_prep{};
  std::chrono::milliseconds t_exec{};

  mutable std::mutex mtx;
  std::map<std::string, std::unique_ptr<Session>> sessions;
  /// sessions out of the table whose queue pair and buffer are still to
  /// be torn down: destroying a queue pair takes the device a while, so
  /// the reaper does it rather than the request that ended the session.
  /// They no longer count against the limits; their buffers, which stay
  /// out of the arena until then, bound them
  std::deque<std::unique_ptr<Session>> ended;
  Teardowns teardowns;
  /// a teardown is done
  std::condition_variable teardown_cv;
  Limits limits;
  /// buffers of ended sessions that an OSD write may still land in; the
  /// reaper gives each back to the arena once its held_until passes
  std::vector<std::unique_ptr<Buffer>> held;
  std::atomic<bool> closing{false};

  std::thread reaper;
  std::condition_variable reaper_cv;
  bool reaper_stop = false;

  /// set once do_init() succeeded: the frontends take requests already
  static std::atomic<Service*> instance;
};

const char* to_string(Error e);
const char* to_string(Outcome o);

} // namespace rgw::rdma::rc

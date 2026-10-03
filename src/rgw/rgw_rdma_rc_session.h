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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "acconfig.h"
#include "include/common_fwd.h"
#include "rgw_rdma_rc_transport.h"
#include "rgw_rdma_rc_wire.h"

namespace rgw::rdma::dc { class Target; }
namespace ceph::ofi { class Endpoint; }

/**
 * hipobj-rc-v2 session service.
 *
 * A session is one transfer: created by PREPARE, driven by READY,
 * destroyed by the FINAL reply, CANCEL, or the expiry reaper. Each
 * session owns an RC Connection and one buffer from the registered
 * pool. For a GET the buffer is where the object lands (pushed by the
 * OSDs straight into it when the gateway's DC target is available,
 * copied in otherwise) before the RC push to the client; for a PUT it
 * is the staging region the client writes into.
 *
 * The REST handlers call in on radosgw request threads; the reaper
 * runs on its own thread. The service mutex covers the session table
 * and state changes; the data phase runs outside it with an io
 * reference pinning the session.
 */
namespace rgw::rdma::rc {

using clock = std::chrono::steady_clock;

struct Buffer {
  void* ptr = nullptr;
  size_t size = 0;
  ibv_mr* mr = nullptr;
  /// delivery descriptor OSDs write this window through: a cuObject DC
  /// descriptor or a libfabric one; empty when OSD-direct is off
  std::string osd_token;
  /// the buffer's libfabric window, when osd_token is a libfabric one
  uint64_t ofi_window = 0;
  bool ofi_registered = false;
  bool in_use = false;
  /// an OSD may still push into a window a failed relay abandoned;
  /// the window stays out of the pool until this passes
  clock::time_point quarantined_until = clock::time_point::min();
};

enum class Op : uint8_t { GET = 0, PUT = 1 };

enum class State : uint8_t {
  PREPARED,      ///< waiting for READY
  TRANSFERRING,  ///< a READY handler owns the data phase
  COMPLETING,    ///< transfer done, FINAL being sent
  REAPING,       ///< marked for teardown
};

/// what a READY data phase ended with
enum class Outcome : uint8_t {
  OK,
  BUSY,         ///< peer not ready; session returns to PREPARED (409)
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
  bool peer_known = false;     ///< false for the all-zero loopback token
  uint64_t client_mr_addr = 0; ///< client window for a GET push
  uint32_t client_mr_rkey = 0;

  Connection conn;
  Buffer* buf = nullptr;

  State state = State::PREPARED;
  clock::time_point created = clock::now();
  clock::time_point deadline;  ///< PREPARE or exec deadline by state
  int io_refs = 0;             ///< handlers pinning the session
  bool reap_pending = false;
  Outcome last_outcome = Outcome::OK;
  uint64_t bytes = 0;          ///< transferred, for the terminal log
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
  NO_BUFFER,    ///< no pool buffer free (503)
  TOO_LARGE,    ///< transfer exceeds the pool buffer size (413)
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

class Service {
 public:
  static int init(CephContext* cct);
  static void shutdown();
  static Service* get();

  ~Service();

  /// true once the device is open and the pool registered
  bool available() const { return dev.is_open(); }
  /// true when the gateway exposes its windows to the OSDs, over a DC
  /// target or libfabric
  bool osd_direct() const;
  size_t buffer_size() const { return buf_size; }
  uint32_t queue_depth() const { return send_depth; }
  std::chrono::milliseconds exec_timeout() const { return t_exec; }

  /// routing lookup before authentication: the target and op of a
  /// session, without the owner check (READY re-checks it)
  Error peek(const std::string& id, SessionInfo& out);

  Error prepare(const PrepareRequest& req, PrepareReply& reply);

  /// look a session up for re-authorization before READY claims it
  Error info(const std::string& id, const std::string& principal,
             SessionInfo& out);

  /// claim the transfer: PREPARED -> TRANSFERRING, pair the queue pair
  /// against the client, and (PUT) arm the receive. On success *out is
  /// pinned until finish() and may be used from the calling thread.
  Error claim(const ReadyRequest& req, Session** out);

  /// end the data phase. OK moves to COMPLETING and tears the session
  /// down once the handler is done with it; BUSY re-arms the queue pair
  /// and returns to PREPARED for another READY; anything else reaps.
  /// quarantine keeps the buffer out of the pool for that long after
  /// teardown (a relay that may still have OSD writes in flight).
  void finish(Session* s, Outcome outcome, uint64_t bytes,
              std::chrono::milliseconds quarantine = {});

  Error cancel(const std::string& id, const std::string& principal);

  /// mark every session for teardown (shutdown)
  void cancel_all();

 private:
  Service() = default;
  int do_init(CephContext* cct);
  void do_shutdown();

  Buffer* acquire_buffer(size_t needed);
  void release_buffer(Buffer* b, std::chrono::milliseconds quarantine);
  bool limits_take(const std::string& principal);
  void limits_release(const std::string& principal);
  std::string new_session_id();

  /// tear down and erase; caller holds the lock
  void reap_locked(std::unique_lock<std::mutex>& lock, const std::string& id);
  void reaper_loop();

  CephContext* cct = nullptr;
  Device dev;
#ifdef HAVE_MLX5DV
  std::unique_ptr<dc::Target> dct;
#endif
#ifdef WITH_OOB_OFI
  std::unique_ptr<ceph::ofi::Endpoint> ofi_ep;
#endif
  std::vector<Buffer> pool;
  size_t buf_size = 0;

  uint32_t max_sessions = 0;
  uint32_t max_user_sessions = 0;
  uint32_t send_depth = 0;
  std::chrono::milliseconds t_prep{};
  std::chrono::milliseconds t_exec{};

  mutable std::mutex mtx;
  std::map<std::string, std::unique_ptr<Session>> sessions;
  std::map<std::string, uint32_t> per_user;
  std::atomic<bool> closing{false};

  std::thread reaper;
  std::condition_variable reaper_cv;
  bool reaper_stop = false;

  static std::unique_ptr<Service> instance;
};

const char* to_string(Error e);
const char* to_string(Outcome o);

} // namespace rgw::rdma::rc

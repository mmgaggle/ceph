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

#include "rgw_rdma_rc_session.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <random>

#include <fmt/format.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/errno.h"

#ifdef HAVE_MLX5DV
#include "rgw_rdma_dc_target.h"
#endif

#define dout_subsys ceph_subsys_rgw

namespace rgw::rdma::rc {

std::unique_ptr<Service> Service::instance;

namespace {

/// completion markers
constexpr uint64_t WR_RECV = 0x5245435632494d4dULL;  // "RECV2IMM"

/// a READY re-arms with the receive posted before pairing, so the
/// queue only ever holds one
constexpr uint32_t RECV_DEPTH = 1;

} // anonymous namespace

const char* to_string(Error e)
{
  switch (e) {
  case Error::OK: return "ok";
  case Error::ARG: return "invalid argument";
  case Error::STATE: return "wrong state";
  case Error::SESSION: return "session mismatch";
  case Error::STALE: return "stale session";
  case Error::NO_SESSION: return "no such session";
  case Error::LIMIT: return "session limit";
  case Error::NO_BUFFER: return "no buffer";
  case Error::TOO_LARGE: return "too large";
  case Error::WIRE: return "wire failure";
  case Error::UNSUPPORTED: return "unsupported";
  case Error::INTERNAL: return "internal";
  }
  return "?";
}

const char* to_string(Outcome o)
{
  switch (o) {
  case Outcome::OK: return "ok";
  case Outcome::BUSY: return "busy";
  case Outcome::TIMEOUT: return "timeout";
  case Outcome::VERIFY_FAIL: return "verify-fail";
  case Outcome::WIRE_FAIL: return "wire-fail";
  case Outcome::BACKEND_FAIL: return "backend-fail";
  }
  return "?";
}

int Service::init(CephContext* cct)
{
  if (instance) {
    return 0;
  }
  instance.reset(new Service());
  int r = instance->do_init(cct);
  if (r < 0) {
    instance.reset();
  }
  return r;
}

void Service::shutdown()
{
  instance.reset();
}

Service* Service::get()
{
  return instance.get();
}

Service::~Service()
{
  do_shutdown();
}

int Service::do_init(CephContext* c)
{
  cct = c;
  const auto& conf = cct->_conf;

  DeviceConfig dcfg;
  dcfg.device_name = conf.get_val<std::string>("rgw_rdma_rc_device");
  dcfg.gid_hint = conf.get_val<std::string>("rgw_rdma_rc_gid_hint");
  dcfg.port = static_cast<uint8_t>(conf.get_val<uint64_t>("rgw_rdma_rc_port"));
  dcfg.gid_index = static_cast<int>(conf.get_val<int64_t>("rgw_rdma_rc_gid_index"));
  if (int r = dev.open(cct, dcfg); r < 0) {
    return r;
  }

  buf_size = conf.get_val<Option::size_t>("rgw_rdma_rc_buffer_size");
  const auto buf_count = conf.get_val<uint64_t>("rgw_rdma_rc_buffer_count");
  max_sessions = conf.get_val<uint64_t>("rgw_rdma_rc_max_sessions");
  max_user_sessions = conf.get_val<uint64_t>("rgw_rdma_rc_max_sessions_per_user");
  send_depth = conf.get_val<uint64_t>("rgw_rdma_rc_send_depth");
  t_prep = std::chrono::milliseconds(conf.get_val<uint64_t>("rgw_rdma_rc_prepare_timeout_ms"));
  t_exec = std::chrono::milliseconds(conf.get_val<uint64_t>("rgw_rdma_rc_exec_timeout_ms"));
  if (buf_size == 0 || buf_count == 0 || send_depth == 0) {
    lderr(cct) << "rgw_rdma_rc: buffer size, count and send depth must be nonzero"
               << dendl;
    return -EINVAL;
  }
  if (buf_size > MAX_TRANSFER_SIZE) {
    lderr(cct) << "rgw_rdma_rc: rgw_rdma_rc_buffer_size exceeds the protocol's "
               << MAX_TRANSFER_SIZE << " byte transfer limit" << dendl;
    return -EINVAL;
  }

#ifdef HAVE_MLX5DV
  if (conf.get_val<bool>("rgw_rdma_rc_osd_direct")) {
    auto target = std::make_unique<dc::Target>();
    const auto dc_key = conf.get_val<uint64_t>("rgw_rdma_rc_dc_key");
    int r = target->open(cct, dev, dc_key);
    if (r == 0) {
      dct = std::move(target);
    } else {
      ldout(cct, 1) << "rgw_rdma_rc: DC target unavailable (" << cpp_strerror(-r)
                    << "); OSD-direct delivery disabled, objects will be "
                    << "staged through the gateway" << dendl;
    }
  }
#endif

  pool.resize(buf_count);
  for (size_t i = 0; i < buf_count; i++) {
    auto& b = pool[i];
    void* ptr = nullptr;
    if (posix_memalign(&ptr, 4096, buf_size) != 0 || !ptr) {
      lderr(cct) << "rgw_rdma_rc: cannot allocate buffer " << i << " of "
                 << buf_size << " bytes" << dendl;
      return -ENOMEM;
    }
    std::memset(ptr, 0, buf_size);
    b.ptr = ptr;
    b.size = buf_size;
    b.mr = dev.register_memory(ptr, buf_size);
    if (!b.mr) {
      lderr(cct) << "rgw_rdma_rc: ibv_reg_mr failed for buffer " << i << ": "
                 << cpp_strerror(errno) << dendl;
      return -EIO;
    }
#ifdef HAVE_MLX5DV
    if (dct) {
      b.dc_token = dct->token(ptr, static_cast<uint32_t>(buf_size), b.mr->rkey);
    }
#endif
  }

  reaper = std::thread([this] { reaper_loop(); });

  ldout(cct, 1) << "rgw_rdma_rc: ready on " << dev.name() << " with "
                << buf_count << " x " << buf_size << " byte buffers, "
                << (osd_direct() ? "OSD-direct delivery on" : "OSD-direct delivery off")
                << dendl;
  return 0;
}

void Service::do_shutdown()
{
  closing = true;
  {
    std::lock_guard l(mtx);
    reaper_stop = true;
  }
  reaper_cv.notify_all();
  if (reaper.joinable()) {
    reaper.join();
  }
  {
    std::unique_lock l(mtx);
    // no handler may still hold a session by now; destroy every queue
    // pair before the memory it references
    for (auto& [id, s] : sessions) {
      s->conn.destroy();
    }
    sessions.clear();
  }
  for (auto& b : pool) {
    Device::deregister_memory(b.mr);
    b.mr = nullptr;
    std::free(b.ptr);
    b.ptr = nullptr;
  }
  pool.clear();
#ifdef HAVE_MLX5DV
  dct.reset();
#endif
  dev.close();
}

bool Service::osd_direct() const
{
#ifdef HAVE_MLX5DV
  return static_cast<bool>(dct);
#else
  return false;
#endif
}

Buffer* Service::acquire_buffer(size_t needed)
{
  // caller holds mtx
  const auto now = clock::now();
  for (auto& b : pool) {
    if (!b.in_use && b.size >= needed && now >= b.quarantined_until) {
      b.in_use = true;
      return &b;
    }
  }
  return nullptr;
}

void Service::release_buffer(Buffer* b, std::chrono::milliseconds quarantine)
{
  // caller holds mtx
  if (!b) {
    return;
  }
  b->in_use = false;
  if (quarantine.count() > 0) {
    b->quarantined_until = clock::now() + quarantine;
  }
}

bool Service::limits_take(const std::string& principal)
{
  // caller holds mtx
  if (sessions.size() >= max_sessions) {
    return false;
  }
  auto& n = per_user[principal];
  if (n >= max_user_sessions) {
    if (n == 0) per_user.erase(principal);
    return false;
  }
  ++n;
  return true;
}

void Service::limits_release(const std::string& principal)
{
  // caller holds mtx
  auto it = per_user.find(principal);
  if (it == per_user.end()) {
    return;
  }
  if (--it->second == 0) {
    per_user.erase(it);
  }
}

std::string Service::new_session_id()
{
  std::random_device rd;
  return fmt::format("{:08x}{:08x}{:08x}{:08x}", rd(), rd(), rd(), rd());
}

Error Service::prepare(const PrepareRequest& req, PrepareReply& reply)
{
  if (closing) {
    return Error::UNSUPPORTED;
  }
  if (req.size == 0 || req.size > MAX_TRANSFER_SIZE ||
      req.offset > MAX_TRANSFER_SIZE - req.size) {
    return Error::ARG;
  }
  if (req.size > buf_size) {
    return Error::TOO_LARGE;
  }

  auto s = std::make_unique<Session>();
  s->principal = req.principal;
  s->op = req.op;
  s->target = req.target;
  s->size = req.size;
  s->offset = req.offset;
  s->cookie = req.cookie;
  s->client_psn = req.client_psn;

  // the client's token names the peer to route the pairing to; the
  // all-zero token is the explicit same-host loopback marker
  if (!token_is_zero(req.client_token)) {
    auto tok = decode_token(req.client_token);
    if (!tok || tok->transport != transport_t::RC || tok->gid_is_zero()) {
      return Error::ARG;
    }
    std::memcpy(s->peer.gid.raw, tok->gid, sizeof(tok->gid));
    s->peer.lid = tok->lid;
    s->peer_known = true;
    // a PUT client may advertise its window here already
    s->client_mr_addr = tok->addr;
    s->client_mr_rkey = tok->rkey;
  } else {
    s->peer.gid = dev.gid();
    s->peer.lid = dev.lid();
  }

  {
    std::unique_lock l(mtx);
    if (!limits_take(req.principal)) {
      return Error::LIMIT;
    }
    s->buf = acquire_buffer(req.size);
    if (!s->buf) {
      limits_release(req.principal);
      return Error::NO_BUFFER;
    }
  }

  auto rollback = [&] {
    std::lock_guard l(mtx);
    release_buffer(s->buf, {});
    limits_release(req.principal);
  };

  // the queue pair is created per session and left in INIT until READY
  // pairs it; the depth bounds how many writes a relay can leave
  // unreaped while it streams
  if (s->conn.create(cct, dev, send_depth, RECV_DEPTH) < 0) {
    rollback();
    return Error::WIRE;
  }
  if (s->op == Op::PUT) {
    // arm the receive before the queue pair can reach RTR so the
    // client's write can never find the queue empty
    if (s->conn.post_recv(s->buf->mr, s->buf->ptr,
                          static_cast<uint32_t>(s->size), WR_RECV) < 0) {
      rollback();
      return Error::WIRE;
    }
  }

  {
    std::random_device rd;
    do {
      s->server_psn = rd() & 0xffffff;
    } while (s->server_psn == 0);
  }

  token_t server_token;
  server_token.transport = transport_t::RC;
  server_token.qpn = s->conn.qpn();
  std::memcpy(server_token.gid, dev.gid().raw, sizeof(server_token.gid));
  server_token.port = dev.port_num();
  server_token.lid = dev.lid();
  if (s->op == Op::PUT) {
    server_token.rkey = s->buf->mr->rkey;
    server_token.addr = reinterpret_cast<uintptr_t>(s->buf->ptr);
    server_token.length = s->size;
  }

  reply = PrepareReply{};
  reply.server_token = encode_token(server_token);
  reply.server_psn = s->server_psn;
  reply.server_qpn = s->conn.qpn();
  if (s->op == Op::PUT) {
    reply.staging_addr = reinterpret_cast<uintptr_t>(s->buf->ptr);
    reply.staging_rkey = s->buf->mr->rkey;
  }

  s->deadline = clock::now() + t_prep;
  {
    std::lock_guard l(mtx);
    for (int attempt = 0; attempt < 4; attempt++) {
      s->id = new_session_id();
      if (!sessions.contains(s->id)) {
        break;
      }
      s->id.clear();
    }
    if (s->id.empty()) {
      release_buffer(s->buf, {});
      limits_release(req.principal);
      return Error::INTERNAL;
    }
    reply.session_id = s->id;
    ldout(cct, 20) << "rgw_rdma_rc: prepared session " << s->id << " op="
                   << (s->op == Op::GET ? "GET" : "PUT") << " target="
                   << s->target << " size=" << s->size << " offset="
                   << s->offset << " qpn=" << reply.server_qpn << dendl;
    sessions.emplace(s->id, std::move(s));
  }
  return Error::OK;
}

Error Service::info(const std::string& id, const std::string& principal,
                    SessionInfo& out)
{
  std::lock_guard l(mtx);
  auto it = sessions.find(id);
  if (it == sessions.end()) {
    return Error::NO_SESSION;
  }
  const Session& s = *it->second;
  if (s.principal != principal) {
    return Error::SESSION;
  }
  out.op = s.op;
  out.target = s.target;
  out.size = s.size;
  out.offset = s.offset;
  return Error::OK;
}

Error Service::peek(const std::string& id, SessionInfo& out)
{
  std::lock_guard l(mtx);
  auto it = sessions.find(id);
  if (it == sessions.end()) {
    return Error::NO_SESSION;
  }
  const Session& s = *it->second;
  out.op = s.op;
  out.target = s.target;
  out.size = s.size;
  out.offset = s.offset;
  return Error::OK;
}

Error Service::claim(const ReadyRequest& req, Session** out)
{
  if (closing) {
    return Error::UNSUPPORTED;
  }
  Session* s = nullptr;
  {
    std::lock_guard l(mtx);
    auto it = sessions.find(req.session_id);
    if (it == sessions.end()) {
      return Error::NO_SESSION;
    }
    s = it->second.get();
    if (s->principal != req.principal) {
      return Error::SESSION;
    }
    if (s->state == State::TRANSFERRING || s->state == State::COMPLETING) {
      return Error::STATE;  // duplicate READY
    }
    if (s->state != State::PREPARED || s->reap_pending) {
      return Error::STALE;
    }
    if (req.cookie != s->cookie) {
      return Error::SESSION;
    }
    // claim under the lock: state and the reference move before the
    // queue pair transitions so a concurrent READY cannot race it
    s->peer.qpn = req.client_qpn;
    s->peer.psn = s->client_psn;
    if (req.client_mr_addr) s->client_mr_addr = req.client_mr_addr;
    if (req.client_mr_rkey) s->client_mr_rkey = req.client_mr_rkey;
    s->state = State::TRANSFERRING;
    s->io_refs = 1;
    s->deadline = clock::now() + t_exec;
  }

  if (s->conn.pair(cct, dev, s->peer, s->server_psn) < 0) {
    std::lock_guard l(mtx);
    s->last_outcome = Outcome::WIRE_FAIL;
    s->reap_pending = true;
    s->io_refs = 0;
    return Error::WIRE;
  }
  ldout(cct, 20) << "rgw_rdma_rc: session " << s->id << " paired with client qpn="
                 << req.client_qpn << dendl;
  *out = s;
  return Error::OK;
}

void Service::finish(Session* s, Outcome outcome, uint64_t bytes,
                     std::chrono::milliseconds quarantine)
{
  std::unique_lock l(mtx);
  s->last_outcome = outcome;
  s->bytes = bytes;
  ldout(cct, 20) << "rgw_rdma_rc: session " << s->id << " finished "
                 << to_string(outcome) << " bytes=" << bytes << dendl;
  if (outcome == Outcome::BUSY && !s->reap_pending) {
    // roll the claim back so the client can retry READY on the same
    // session; the queue pair goes back to INIT through RESET
    l.unlock();
    bool rearmed = s->conn.rearm(cct, dev) == 0;
    if (rearmed && s->op == Op::PUT) {
      // RESET discarded the armed receive
      rearmed = s->conn.post_recv(s->buf->mr, s->buf->ptr,
                                  static_cast<uint32_t>(s->size), WR_RECV) == 0;
    }
    l.lock();
    if (rearmed && !s->reap_pending) {
      s->state = State::PREPARED;
      s->deadline = clock::now() + t_prep;
      s->io_refs = 0;
      return;
    }
    s->last_outcome = Outcome::WIRE_FAIL;
  }
  s->io_refs = 0;
  s->reap_pending = true;
  s->state = State::REAPING;
  if (s->buf && quarantine.count() > 0) {
    s->buf->quarantined_until = clock::now() + quarantine;
  }
  reap_locked(l, s->id);
}

Error Service::cancel(const std::string& id, const std::string& principal)
{
  std::unique_lock l(mtx);
  auto it = sessions.find(id);
  if (it == sessions.end()) {
    return Error::STALE;
  }
  Session& s = *it->second;
  if (s.principal != principal) {
    return Error::SESSION;
  }
  s.reap_pending = true;
  if (s.io_refs == 0) {
    reap_locked(l, id);
  }
  return Error::OK;
}

void Service::cancel_all()
{
  std::unique_lock l(mtx);
  std::vector<std::string> ids;
  for (auto& [id, s] : sessions) {
    s->reap_pending = true;
    if (s->io_refs == 0) {
      ids.push_back(id);
    }
  }
  for (const auto& id : ids) {
    reap_locked(l, id);
  }
}

void Service::reap_locked(std::unique_lock<std::mutex>& lock,
                          const std::string& id)
{
  auto it = sessions.find(id);
  if (it == sessions.end() || it->second->io_refs > 0) {
    return;
  }
  std::unique_ptr<Session> s = std::move(it->second);
  sessions.erase(it);
  Buffer* buf = s->buf;
  s->buf = nullptr;
  limits_release(s->principal);
  ldout(cct, 20) << "rgw_rdma_rc: reaping session " << s->id << " ("
                 << to_string(s->last_outcome) << ")" << dendl;
  // the queue pair must be gone before its buffer can be handed out
  // again: a posted work request may still reference the region
  lock.unlock();
  s->conn.destroy();
  lock.lock();
  // a quarantine set by finish() stays; otherwise the buffer is free
  release_buffer(buf, {});
}

void Service::reaper_loop()
{
  std::unique_lock l(mtx);
  while (!reaper_stop) {
    reaper_cv.wait_for(l, std::chrono::seconds(1));
    if (reaper_stop) {
      break;
    }
    const auto now = clock::now();
    std::vector<std::string> expired;
    for (auto& [id, s] : sessions) {
      if (s->io_refs > 0) {
        // a handler owns it; its finish() reaps
        continue;
      }
      if (s->reap_pending || now > s->deadline) {
        if (!s->reap_pending) {
          s->last_outcome = Outcome::TIMEOUT;
        }
        expired.push_back(id);
      }
    }
    for (const auto& id : expired) {
      reap_locked(l, id);
    }
  }
}

} // namespace rgw::rdma::rc

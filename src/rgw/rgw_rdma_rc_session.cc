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
#include <cstring>
#include <limits>
#include <random>

#include <fmt/format.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/oob_arena.h"
#include "common/oob_config.h"
#include "common/rdma_token.h"

#ifdef HAVE_MLX5DV
#include "rgw_rdma_dc_target.h"
#endif
#ifdef WITH_OOB_OFI
#include "common/ofi_rma.h"
#endif

#define dout_subsys ceph_subsys_rgw

namespace rgw::rdma::rc {

std::atomic<Service*> Service::instance{nullptr};

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

int Service::init(CephContext* cct, ceph::oob::Arena* arena)
{
  if (instance.load()) {
    return 0;
  }
  // the frontends take requests already: request threads see the service
  // only once it is complete, and never one whose setup failed
  std::unique_ptr<Service> svc{new Service()};
  svc->arena = arena;
  if (int r = svc->do_init(cct); r < 0) {
    return r;
  }
  instance.store(svc.release(), std::memory_order_release);
  return 0;
}

void Service::shutdown()
{
  delete instance.exchange(nullptr);
}

Service* Service::get()
{
  return instance.load(std::memory_order_acquire);
}

Service::~Service()
{
  do_shutdown();
}

int Service::do_init(CephContext* c)
{
  ceph_assert(arena);
  cct = c;
  const auto& conf = cct->_conf;

  // a session takes a buffer of the arena at its size, at most this
  buf_size = ceph::oob::Arena::MAX;
  limits = Limits(conf.get_val<uint64_t>("rgw_rdma_rc_max_sessions"),
                  conf.get_val<uint64_t>("rgw_rdma_rc_max_sessions_per_user"));
  send_depth = conf.get_val<uint64_t>("rgw_rdma_rc_send_depth");
  t_prep = std::chrono::milliseconds(conf.get_val<uint64_t>("rgw_rdma_rc_prepare_timeout_ms"));
  t_exec = std::chrono::milliseconds(conf.get_val<uint64_t>("rgw_rdma_rc_exec_timeout_ms"));
  if (send_depth == 0) {
    lderr(cct) << "rgw_rdma_rc: rgw_rdma_rc_send_depth must be nonzero"
               << dendl;
    return -EINVAL;
  }
  static_assert(ceph::oob::Arena::MAX <= MAX_TRANSFER_SIZE);

  // the device, port and GID that carry the gateway's out-of-band
  // address; without one, the first device
  ceph::oob::location_t loc;
  std::string err;
  if (int r = ceph::oob::resolve(cct, ceph::oob::public_ip(cct), &loc, &err);
      r < 0) {
    lderr(cct) << "rgw_rdma_rc: " << err << dendl;
    return r;
  }
  ldout(cct, 1) << "rgw_rdma_rc: out-of-band address: " << loc.describe()
                << dendl;
  if (!loc.addr.empty() && loc.rdma_device.empty()) {
    lderr(cct) << "rgw_rdma_rc: no RDMA device carries " << loc.addr << dendl;
    return -ENODEV;
  }
  DeviceConfig dcfg;
  dcfg.device_name = loc.rdma_device;
  if (loc.rdma_port) {
    dcfg.port = static_cast<uint8_t>(loc.rdma_port);
  }
  dcfg.gid_index = loc.gid_index;
  dcfg.client_mtu = conf.get_val<uint64_t>("rgw_rdma_rc_path_mtu");
  if (int r = dev.open(cct, dcfg); r < 0) {
    return r;
  }

  const auto oob = ceph::oob::read_transports(cct);

  // expose the session buffers to the OSDs over the cluster transport;
  // without one, stripes are staged through the gateway
  if (conf.get_val<bool>("rgw_rdma_osd_direct") && !oob.cluster.empty()) {
#ifdef WITH_OOB_OFI
    if (oob.cluster_ofi()) {
      ceph::ofi::config_t oc;
      oc.provider = oob.cluster;
      ceph::oob::ofi_where(loc, oob.cluster, &oc.domain, &oc.node);
      // OSD writes land only while a manual-progress provider is polled
      oc.progress_thread = true;
      std::string err;
      const auto open = [&] { ofi_ep = ceph::ofi::Endpoint::open(oc, &err); };
      std::string no_raw;
      if (!ceph::oob::needs_net_raw(oob.cluster)) {
        open();
      } else if (ceph::oob::with_net_raw(open, &no_raw) == -EPERM) {
        // this thread keeps it through --setuser when root holds it
        // (drop_privileges() in rgw_asio_frontend.cc), which podman's
        // default capabilities do not include
        no_raw += "; a gateway in a container needs --cap-add=NET_RAW";
      }
      if (!ofi_ep && !no_raw.empty()) {
        err += "; " + no_raw;
      }
      if (ofi_ep) {
        ldout(cct, 1) << "rgw_rdma_rc: OSD-direct relay windows over "
                      << "libfabric: " << ofi_ep->describe() << dendl;
        ofi_revokes_keys = ofi_close_revokes_key();
        if (!ofi_revokes_keys) {
          ldout(cct, 1) << "rgw_rdma_rc: a closed relay window keeps its key "
                        << "(" << ofi_ep->last_error() << "); each relay "
                        << "buffer stays out of use for "
                        << ofi_ep->key_quarantine().count()
                        << "ms after its session" << dendl;
        }
      } else {
        ldout(cct, 1) << "rgw_rdma_rc: libfabric relay windows unavailable ("
                      << err << ")" << dendl;
      }
    }
#endif
#ifdef HAVE_MLX5DV
    if (oob.cluster_cuobj()) {
      auto target = std::make_unique<dc::Target>();
      uint64_t dc_key = 0;
      if (ceph::oob::dc_key(cct, &dc_key, &err) < 0) {
        lderr(cct) << "rgw_rdma_rc: " << err << dendl;
      } else if (int r = target->open(cct, dev, dc_key); r == 0) {
        dct = std::move(target);
      } else {
        ldout(cct, 1) << "rgw_rdma_rc: DC target unavailable ("
                      << cpp_strerror(-r) << ")" << dendl;
      }
    }
#endif
    if (!osd_direct()) {
      ldout(cct, 1) << "rgw_rdma_rc: the cluster transport "
                    << oob.cluster << " exposes no relay windows here; "
                    << "objects will be staged through the gateway" << dendl;
    }
  }

  reaper = std::thread([this] { reaper_loop(); });

  ldout(cct, 1) << "rgw_rdma_rc: ready on " << dev.name() << ", buffers of "
                << "up to " << buf_size << " bytes from " << arena->size()
                << " bytes of out-of-band memory, "
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
    // no handler may still hold a session by now, and the reaper is
    // gone: destroy every queue pair, of the sessions in the table and of
    // those that ended, before the memory it references. The arena, which
    // holds the memory, goes after the service, and nothing takes from it
    // any more
    for (auto& [id, s] : sessions) {
      ended.push_back(std::move(s));
    }
    sessions.clear();
    for (auto& s : ended) {
      s->conn.destroy();
      if (s->buf) {
        free_buffer(*s->buf);
      }
    }
    ended.clear();
    for (auto& b : held) {
      free_buffer(*b);
    }
    held.clear();
  }
#ifdef WITH_OOB_OFI
  // stops polling
  ofi_ep.reset();
#endif
#ifdef HAVE_MLX5DV
  dct.reset();
#endif
  dev.close();
}

bool Service::osd_direct() const
{
#ifdef WITH_OOB_OFI
  if (ofi_ep) {
    return true;
  }
#endif
#ifdef HAVE_MLX5DV
  return static_cast<bool>(dct);
#else
  return false;
#endif
}

std::unique_ptr<Buffer> Service::make_buffer(Op op, size_t size)
{
  // no lock: the arena, the device and the endpoint take their own, and
  // registering up to 64 MiB takes a while
  const auto slice = arena->alloc(size);
  if (!slice) {
    return nullptr;
  }
  auto b = std::make_unique<Buffer>();
  b->slice = slice;
  b->ptr = arena->ptr(slice);
  b->size = size;
  // Nothing reads the buffer from the network. A PUT's buffer and queue
  // pair get a protection domain of their own: no other session's queue
  // pair reaches the buffer, whatever key it holds or a device hands out
  // again, and a write that the session's queue pair takes lands in this
  // buffer or nowhere. The registration covers exactly the session's
  // size, so the write of that size that READY takes fills it. The
  // buffer is zeroed as well, so that nothing an earlier user of the
  // memory left in it can be stored should a write ever fall short of
  // that. A GET buffer is written by the OSDs when they relay through
  // the DC target, and only its landed bytes are pushed.
  int access = 0;
  if (op == Op::PUT) {
    b->pd = dev.alloc_pd();
    if (!b->pd) {
      lderr(cct) << "rgw_rdma_rc: ibv_alloc_pd failed: " << cpp_strerror(errno)
                 << dendl;
      arena->free(slice);
      return nullptr;
    }
    std::memset(b->ptr, 0, size);
    access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
  }
#ifdef HAVE_MLX5DV
  if (op == Op::GET && dct) {
    access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
  }
#endif
  b->mr = dev.register_memory(b->ptr, size, access, b->pd);
  if (!b->mr) {
    lderr(cct) << "rgw_rdma_rc: ibv_reg_mr failed for a buffer of " << size
               << " bytes: " << cpp_strerror(errno) << dendl;
    Device::dealloc_pd(b->pd);
    arena->free(slice);
    return nullptr;
  }
  if (op == Op::GET) {
#ifdef HAVE_MLX5DV
    if (dct) {
      b->osd_token = dct->token(b->ptr, static_cast<uint32_t>(size),
                                b->mr->rkey);
    }
#endif
#ifdef WITH_OOB_OFI
    if (ofi_ep) {
      ceph::ofi::Endpoint::window_t w;
      if (int r = ofi_ep->register_window(static_cast<char*>(b->ptr), size, &w);
          r == 0) {
        b->osd_token = ofi_ep->window_token(w, 0, size);
        b->ofi_window = w.id;
        b->ofi_registered = true;
      } else {
        // this session relays through the gateway
        ldout(cct, 1) << "rgw_rdma_rc: registering a relay window over "
                      << "libfabric failed (" << ofi_ep->last_error() << ")"
                      << dendl;
      }
    }
#endif
  }
  return b;
}

std::unique_ptr<Buffer> Service::make_buffer_from_ended(Op op, size_t size)
{
  // Only the sessions that ended until now: those that end meanwhile
  // are not this request's to wait for. After each teardown, here or
  // elsewhere, the buffer may fit already
  std::unique_lock l(mtx);
  const uint64_t upto = teardowns.last();
  for (;;) {
    if (!ended.empty() && ended.front()->ended_seq <= upto) {
      tear_down_one(l);
    } else if (teardowns.running_upto(upto)) {
      teardown_cv.wait(l);
    } else {
      return nullptr;
    }
    l.unlock();
    if (auto b = make_buffer(op, size); b) {
      return b;
    }
    l.lock();
  }
}

std::unique_ptr<Buffer> Service::release_buffer(
  std::unique_ptr<Buffer> b, std::chrono::milliseconds quarantine)
{
  // caller does not hold mtx; the session's queue pair is gone
  if (!b) {
    return nullptr;
  }
#ifdef WITH_OOB_OFI
  if (b->ofi_registered) {
    // Closing the window retires its key, so that no OSD write meant for
    // the session lands once the memory serves another: not one a failed
    // relay left in flight, and not a provider's late duplicate of one
    // that completed, which the delivery lease does not bound. After a
    // session that left no write in flight the buffer is free at once.
    // After one that did (a quarantine: its reads failed, or one was
    // resent) it keeps its quarantine anyway, and the key quarantine at
    // least: those writes may still be on the way, and the retired key
    // keeps them out only if the provider drops what carries it. So does
    // every buffer when closing does not revoke the key.
    ofi_ep->deregister_window(b->ofi_window);
    b->ofi_registered = false;
    if (quarantine.count() > 0 || !ofi_revokes_keys) {
      quarantine = std::max(quarantine, ofi_ep->key_quarantine());
    }
  }
#endif
  if (quarantine.count() > 0) {
    // the verbs registration stays until then, so that the key a DC
    // write carries names no other memory meanwhile
    b->held_until = clock::now() + quarantine;
    return b;
  }
  free_buffer(*b);
  return nullptr;
}

void Service::free_buffer(Buffer& b)
{
#ifdef WITH_OOB_OFI
  if (b.ofi_registered) {
    ofi_ep->deregister_window(b.ofi_window);
    b.ofi_registered = false;
  }
#endif
  if (int r = Device::deregister_memory(b.mr); r != 0) {
    // the key still reaches the memory: it never serves another session
    lderr(cct) << "rgw_rdma_rc: ibv_dereg_mr failed for a buffer of "
               << b.size << " bytes: " << cpp_strerror(r)
               << "; keeping it out of use" << dendl;
  } else {
    // the queue pair went first
    if (int r = Device::dealloc_pd(b.pd); r != 0) {
      ldout(cct, 1) << "rgw_rdma_rc: ibv_dealloc_pd failed: "
                    << cpp_strerror(r) << dendl;
    }
    // nothing exposes the memory any more
    arena->free(b.slice);
  }
  b.mr = nullptr;
  b.pd = nullptr;
  b.slice = {};
  b.ptr = nullptr;
}

#ifdef WITH_OOB_OFI
bool Service::ofi_close_revokes_key()
{
  // a provider whose registration cache keeps a closed region, and with
  // it the key, hands that key back when the memory is registered again
  const auto slice = arena->alloc(ceph::oob::Arena::MIN);
  if (!slice) {
    return false;
  }
  bool revokes = false;
  ceph::ofi::Endpoint::window_t w;
  if (ofi_ep->register_window(arena->ptr(slice), slice.len, &w) == 0) {
    revokes = ofi_ep->rekey_window(w) == 0;
    ofi_ep->deregister_window(w.id);
  }
  arena->free(slice);
  return revokes;
}
#endif

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
  // the cap is on the size only: the offset places the range in the
  // object, and the range need only fit the signed offsets of a read
  constexpr uint64_t max_end = std::numeric_limits<int64_t>::max();
  if (req.size == 0 || req.size > MAX_TRANSFER_SIZE ||
      req.offset > max_end - req.size) {
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
    // a PUT client may advertise its window here already
    s->client_mr_addr = tok->addr;
    s->client_mr_rkey = tok->rkey;
  } else {
    s->peer.gid = dev.gid();
    s->peer.lid = dev.lid();
  }

  {
    std::lock_guard l(mtx);
    if (!limits.take(req.principal)) {
      return Error::LIMIT;
    }
  }
  s->buf = make_buffer(s->op, req.size);
  if (!s->buf) {
    // the arena may be short only of the buffers of sessions that ended
    // and are not torn down yet
    s->buf = make_buffer_from_ended(s->op, req.size);
  }
  if (!s->buf) {
    std::lock_guard l(mtx);
    limits.release(req.principal);
    // for the 503: a buffer an OSD write may still land in stays out of
    // the arena until its quarantine passes
    ldout(cct, 4) << "rgw_rdma_rc: no buffer of " << req.size << " bytes: "
                  << sessions.size() << " sessions, " << ended.size()
                  << " ended ones to tear down, " << held.size()
                  << " buffers in quarantine" << dendl;
    return Error::NO_BUFFER;
  }

  auto rollback = [&] {
    // the queue pair goes first: a posted receive references the buffer.
    // Nobody got the buffer's address or keys yet
    s->conn.destroy();
    free_buffer(*s->buf);
    std::lock_guard l(mtx);
    limits.release(req.principal);
  };

  // the queue pair is created per session and left in INIT until READY
  // pairs it; the depth bounds how many writes a relay can leave
  // unreaped while it streams. Only a PUT client writes into the
  // gateway, through the buffer's own protection domain, and nobody
  // reads from it
  const int qp_access = s->op == Op::PUT ? IBV_ACCESS_REMOTE_WRITE : 0;
  if (s->conn.create(cct, dev, send_depth, RECV_DEPTH, qp_access,
                     s->buf->pd) < 0) {
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
    if (!s->id.empty()) {
      reply.session_id = s->id;
      ldout(cct, 20) << "rgw_rdma_rc: prepared session " << s->id << " op="
                     << (s->op == Op::GET ? "GET" : "PUT") << " target="
                     << s->target << " size=" << s->size << " offset="
                     << s->offset << " qpn=" << reply.server_qpn << dendl;
      sessions.emplace(s->id, std::move(s));
      return Error::OK;
    }
  }
  rollback();
  return Error::INTERNAL;
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
    if (s->state == State::TRANSFERRING) {
      return Error::STATE;  // duplicate READY
    }
    if (s->reap_pending) {
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
  ldout(cct, 20) << "rgw_rdma_rc: session " << s->id << " finished "
                 << to_string(outcome) << " bytes=" << bytes << dendl;
  if (outcome == Outcome::BUSY && !s->reap_pending &&
      quarantine.count() == 0) {
    // roll the claim back so the client can retry READY on the same
    // session; the queue pair goes back to INIT through RESET. Not with a
    // quarantine: that READY would relay through this buffer while an
    // OSD write of this data phase may still land in it
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
  if (s->conn.outstanding() > 0) {
    // a push that failed or timed out may leave writes in the send
    // queue, which the queue pair would go on sending into the client's
    // memory after the reply. It goes now, before the reply, rather
    // than with the teardown; the io reference still pins the session
    l.unlock();
    s->conn.destroy();
    l.lock();
  }
  s->io_refs = 0;
  s->reap_pending = true;
  // the reaper hands it to release_buffer(), which closes the buffer's
  // windows and must not mistake it for a clean one
  s->quarantine = quarantine;
  reap_locked(s->id);
}

Error Service::cancel(const std::string& id, const std::string& principal)
{
  std::lock_guard l(mtx);
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
    reap_locked(id);
  }
  return Error::OK;
}

void Service::reap_locked(const std::string& id)
{
  auto it = sessions.find(id);
  if (it == sessions.end() || it->second->io_refs > 0) {
    return;
  }
  std::unique_ptr<Session> s = std::move(it->second);
  sessions.erase(it);
  limits.release(s->principal);
  ldout(cct, 20) << "rgw_rdma_rc: reaping session " << s->id << " ("
                 << to_string(s->last_outcome) << ", quarantine "
                 << s->quarantine.count() << "ms)" << dendl;
  s->ended_seq = teardowns.ended();
  ended.push_back(std::move(s));
  reaper_cv.notify_one();
}

void Service::tear_down_ended(std::unique_lock<std::mutex>& lock)
{
  // only those queued now: sessions that end meanwhile must not keep
  // the reaper from its scan
  const uint64_t upto = teardowns.last();
  while (!ended.empty() && ended.front()->ended_seq <= upto) {
    tear_down_one(lock);
  }
}

void Service::tear_down_one(std::unique_lock<std::mutex>& lock)
{
  std::unique_ptr<Session> s = std::move(ended.front());
  ended.pop_front();
  teardowns.begin(s->ended_seq);
  lock.unlock();
  auto hold = tear_down(*s);
  lock.lock();
  teardowns.end(s->ended_seq);
  if (hold) {
    held.push_back(std::move(hold));
  }
  // a PREPARE short of a buffer may wait for this one
  teardown_cv.notify_all();
}

std::unique_ptr<Buffer> Service::tear_down(Session& s)
{
  // the queue pair, unless finish() destroyed it already, must be gone
  // before its buffer is: a posted work request may still reference the
  // region
  s.conn.destroy();
  // with the quarantine finish() was given. A session reaped without
  // finish() has none: it never ran a data phase, or went back to
  // PREPARED after one that left no write in flight.
  return release_buffer(std::move(s.buf), s.quarantine);
}

void Service::reaper_loop()
{
  std::unique_lock l(mtx);
  auto next_scan = clock::now() + std::chrono::seconds(1);
  while (!reaper_stop) {
    // sessions that ended are torn down as they come; the scan for held
    // buffers and expired sessions runs once a second
    reaper_cv.wait_until(l, next_scan, [this] {
      return reaper_stop || !ended.empty();
    });
    if (reaper_stop) {
      break;
    }
    tear_down_ended(l);
    const auto now = clock::now();
    if (now < next_scan) {
      continue;
    }
    next_scan = now + std::chrono::seconds(1);
    // buffers whose quarantine passed go back to the arena
    std::vector<std::unique_ptr<Buffer>> due;
    for (auto it = held.begin(); it != held.end(); ) {
      if (now >= (*it)->held_until) {
        due.push_back(std::move(*it));
        it = held.erase(it);
      } else {
        ++it;
      }
    }
    if (!due.empty()) {
      l.unlock();
      for (auto& b : due) {
        free_buffer(*b);
      }
      l.lock();
    }
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
    // torn down on the next pass
    for (const auto& id : expired) {
      reap_locked(id);
    }
  }
}

} // namespace rgw::rdma::rc

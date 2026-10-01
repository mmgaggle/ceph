// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/osd_uet.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"

#include "osd/uet_shim.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_uet: "

namespace {

constexpr std::string_view UET_TAG = "uet1";
/// largest single RMA write; stripes are at most a few MiB, so this
/// only splits unusually large reads
constexpr uint64_t MAX_WRITE = 8ull << 20;

std::optional<uint64_t> parse_hex(std::string_view s)
{
  if (s.empty() || s.size() > 16) {
    return std::nullopt;
  }
  uint64_t v = 0;
  for (char c : s) {
    int d;
    if (c >= '0' && c <= '9') d = c - '0';
    else if (c >= 'a' && c <= 'f') d = c - 'a' + 10;
    else if (c >= 'A' && c <= 'F') d = c - 'A' + 10;
    else return std::nullopt;
    v = (v << 4) | d;
  }
  return v;
}

} // anonymous namespace

std::optional<uet_token_t> parse_uet_token(std::string_view token)
{
  std::string_view f[5];
  size_t n = 0;
  while (n < 5) {
    auto colon = token.find(':');
    f[n++] = token.substr(0, colon);
    if (colon == std::string_view::npos) {
      token = {};
      break;
    }
    token.remove_prefix(colon + 1);
  }
  if (n != 5 || !token.empty() || f[2] != UET_TAG) {
    return std::nullopt;
  }
  auto base = parse_hex(f[0]);
  auto size = parse_hex(f[1]);
  auto key = parse_hex(f[4]);
  in_addr in;
  const std::string ip{f[3]};
  if (!base || !size || !key || inet_pton(AF_INET, ip.c_str(), &in) != 1) {
    return std::nullopt;
  }
  return uet_token_t{*base, *size, ntohl(in.s_addr), *key};
}

struct OSDUet::Impl {
  std::mutex mtx;  ///< the provider is driven from one thread at a time
  uet_shim* shim = nullptr;
  std::chrono::milliseconds op_timeout{5000};

  /// receive pool, carved into fixed windows peers push into
  struct slot_t {
    bool in_use = false;
    std::chrono::steady_clock::time_point quarantined_until{};
  };
  std::mutex win_mtx;
  std::vector<slot_t> slots;
  size_t slot_size = 0;
  std::string ip;  ///< this OSD's fabric endpoint, dotted

  /// a software provider places incoming data only while polled
  std::thread progress;
  std::atomic<bool> stopping{false};

  int wait_writes(uint64_t count);
};

int OSDUet::Impl::wait_writes(uint64_t count)
{
  const auto deadline = std::chrono::steady_clock::now() + op_timeout;
  while (count > 0) {
    // polling is also what drives the software provider's receive path
    // (RUDI responses, retransmits)
    int r = uet_shim_poll_tx(shim);
    if (r > 0) {
      --count;
      continue;
    }
    if (r < 0) {
      return r;
    }
    if (std::chrono::steady_clock::now() > deadline) {
      return -ETIMEDOUT;
    }
  }
  return 0;
}

OSDUet::OSDUet(CephContext* cct) : impl(std::make_unique<Impl>()), cct(cct) {}

OSDUet::~OSDUet()
{
  impl->stopping = true;
  if (impl->progress.joinable()) {
    impl->progress.join();
  }
  uet_shim_close(impl->shim);
}

int OSDUet::init()
{
  auto& d = *impl;
  const auto ifname = cct->_conf.get_val<std::string>("osd_uet_ifname");
  if (ifname.empty()) {
    derr << "osd_uet_ifname is not set" << dendl;
    return -EINVAL;
  }
  const auto tx_timeout = cct->_conf.get_val<uint64_t>("osd_uet_tx_timeout_ms");
  // the provider reads its configuration from the environment; this
  // runs once, from OSD::init
  setenv("UET_IFNAME", ifname.c_str(), 1);
  setenv("UET_PDS", "pds", 1);           // the full PDS, which has RUDI
  setenv("UET_FORCE_RUDI", "1", 1);      // idempotent writes go over RUDI
  setenv("UET_PDS_TX_TIMEOUT", std::to_string(tx_timeout).c_str(), 1);
  d.op_timeout = std::chrono::milliseconds(
    cct->_conf.get_val<uint64_t>("osd_uet_op_timeout_ms"));

  char err[256] = "";
  const size_t stage = cct->_conf.get_val<Option::size_t>("osd_uet_buffer_size");
  d.slot_size = cct->_conf.get_val<Option::size_t>("osd_oob_window_size");
  const size_t nslots = cct->_conf.get_val<uint64_t>("osd_oob_window_count");
  d.shim = uet_shim_open(stage, d.slot_size * nslots, err, sizeof(err));
  if (!d.shim) {
    derr << "UET endpoint on " << ifname << ": " << err << dendl;
    return -EIO;
  }
  d.slots.resize(d.slot_size ? nslots : 0);
  in_addr a;
  a.s_addr = htonl(uet_shim_ipv4(d.shim));
  char ip[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &a, ip, sizeof(ip));
  d.ip = ip;
  if (!d.slots.empty()) {
    // peers push into our windows while no op of ours is polling
    d.progress = std::thread([&d] {
      while (!d.stopping) {
	{
	  std::lock_guard l(d.mtx);
	  for (int i = 0; i < 16; i++) {
	    uet_shim_poll_rx(d.shim);
	  }
	}
	std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
    });
  }
  dout(1) << "UET delivery up on " << ifname << " (" << d.ip << "), staging "
	  << stage << " bytes, " << d.slots.size() << " windows of "
	  << d.slot_size << " bytes" << dendl;
  return 0;
}

std::optional<OSDOobExecutor::window_t> OSDUet::acquire_window(size_t size)
{
  auto& d = *impl;
  if (!d.shim || size > d.slot_size) {
    return std::nullopt;
  }
  const auto now = std::chrono::steady_clock::now();
  std::lock_guard l(d.win_mtx);
  for (size_t i = 0; i < d.slots.size(); i++) {
    auto& slot = d.slots[i];
    if (slot.in_use || now < slot.quarantined_until) {
      continue;
    }
    slot.in_use = true;
    windows_acquired++;
    window_t w;
    w.id = i;
    w.ptr = uet_shim_window(d.shim) + i * d.slot_size;
    w.size = d.slot_size;
    // the window pool is zero-based, so a window's base is its offset
    char tok[160];
    snprintf(tok, sizeof(tok), "%zx:%zx:uet1:%s:%llx", i * d.slot_size,
	     d.slot_size, d.ip.c_str(),
	     static_cast<unsigned long long>(uet_shim_key(d.shim)));
    w.token = tok;
    return w;
  }
  windows_exhausted++;
  return std::nullopt;
}

void OSDUet::window_sync()
{
  // the provider places incoming data with this mutex held (progress
  // thread or a polling op); taking it orders our reads after those
  // writes
  std::lock_guard l(impl->mtx);
}

void OSDUet::release_window(uint64_t id, uint64_t quarantine_ms)
{
  auto& d = *impl;
  std::lock_guard l(d.win_mtx);
  if (id >= d.slots.size()) {
    return;
  }
  d.slots[id].in_use = false;
  if (quarantine_ms) {
    d.slots[id].quarantined_until = std::chrono::steady_clock::now() +
      std::chrono::milliseconds(quarantine_ms);
  }
}

bool OSDUet::is_available() const
{
  return impl->shim != nullptr;
}

bool OSDUet::handles(const std::string& token) const
{
  return parse_uet_token(token).has_value();
}

ssize_t OSDUet::execute_plan(const std::string& key,
			     const std::string& token,
			     const ceph::buffer::list& data,
			     const ceph::osd::oob::placement_plan& plan)
{
  auto& d = *impl;
  auto tok = parse_uet_token(token);
  if (!tok) {
    return -EINVAL;
  }
  const uint64_t total = data.length();
  for (const auto& t : plan) {
    if (t.local_ofs + t.len > total || t.client_ofs + t.len > tok->size) {
      dout(5) << "plan for " << key << " exceeds the payload or window"
	      << dendl;
      return -ERANGE;
    }
  }
  plans_started++;
  std::lock_guard l(d.mtx);
  if (!d.shim) {
    plans_failed++;
    return -EOPNOTSUPP;
  }
  const auto stage = cct->_conf.get_val<Option::size_t>("osd_uet_buffer_size");
  if (total > stage) {
    plans_failed++;
    return -E2BIG;
  }
  // stage into the registered region; the software provider copies
  // again into packets
  data.begin().copy(total, uet_shim_buffer(d.shim));

  const int peer = uet_shim_peer(d.shim, tok->ipv4);
  if (peer < 0) {
    dout(5) << "adding the client endpoint: " << uet_shim_strerror(peer)
	    << dendl;
    plans_failed++;
    return peer;
  }
  uint64_t posted = 0;
  for (const auto& t : plan) {
    for (uint64_t o = 0; o < t.len; ) {
      const uint64_t n = std::min(t.len - o, MAX_WRITE);
      int r = uet_shim_write(d.shim, peer, t.local_ofs + o, n,
			     tok->base + t.client_ofs + o, tok->key);
      if (r == -EAGAIN) {
	// transmit queue full: complete what is out, then retry
	if (int w = d.wait_writes(posted); w < 0) {
	  plans_failed++;
	  return w;
	}
	posted = 0;
	continue;
      }
      if (r < 0) {
	dout(5) << "uet write: " << uet_shim_strerror(r) << dendl;
	d.wait_writes(posted);
	plans_failed++;
	return r;
      }
      posted++;
      writes_posted++;
      o += n;
    }
  }
  if (int w = d.wait_writes(posted); w < 0) {
    dout(5) << "plan for " << key << " did not complete: "
	    << cpp_strerror(w) << dendl;
    plans_failed++;
    return w;
  }
  plans_completed++;
  bytes_pushed += total;
  dout(20) << "placed " << total << " bytes of " << key << " in "
	   << plan.size() << " range(s)" << dendl;
  return static_cast<ssize_t>(total);
}

void OSDUet::dump_stats(ceph::Formatter* f) const
{
  f->dump_bool("available", impl->shim != nullptr);
  f->dump_unsigned("plans_started", plans_started);
  f->dump_unsigned("plans_completed", plans_completed);
  f->dump_unsigned("plans_failed", plans_failed);
  f->dump_unsigned("bytes_pushed", bytes_pushed);
  f->dump_unsigned("writes_posted", writes_posted);
  f->dump_unsigned("windows_acquired", windows_acquired);
  f->dump_unsigned("windows_exhausted", windows_exhausted);
}

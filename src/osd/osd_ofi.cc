// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/osd_ofi.h"

#include <cstdlib>
#include <cstring>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ofi_rma.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_ofi: "

OSDOfi::OSDOfi(CephContext* cct) : cct(cct) {}

OSDOfi::~OSDOfi()
{
  // the endpoint goes first: it stops the progress thread and closes
  // the regions over the pool
  ep.reset();
  std::free(pool);
}

int OSDOfi::init()
{
  const auto& conf = cct->_conf;
  ceph::ofi::config_t cfg;
  cfg.provider = conf.get_val<std::string>("osd_ofi_provider");
  cfg.domain = conf.get_val<std::string>("osd_ofi_domain");
  cfg.node = conf.get_val<std::string>("osd_ofi_node");
  cfg.stage_size = conf.get_val<Option::size_t>("osd_oob_buffer_size");
  cfg.stage_count = conf.get_val<uint64_t>("osd_oob_buffer_count");
  const bool gather = conf.get_val<bool>("osd_oob_gather");
  // windows only fill while someone polls a manual-progress provider
  cfg.progress_thread = gather;
  if (cfg.stage_size == 0 || cfg.stage_count == 0) {
    derr << "osd_oob_buffer_size and osd_oob_buffer_count must be nonzero"
	 << dendl;
    return -EINVAL;
  }
  std::string err;
  ep = ceph::ofi::Endpoint::open(cfg, &err);
  if (!ep) {
    derr << "libfabric endpoint: " << err << dendl;
    return -EIO;
  }

  if (gather) {
    slot_size = conf.get_val<Option::size_t>("osd_oob_window_size");
    const size_t nslots = conf.get_val<uint64_t>("osd_oob_window_count");
    const size_t len = slot_size * nslots;
    void* p = nullptr;
    if (len && posix_memalign(&p, 4096, len) == 0 && p) {
      std::memset(p, 0, len);
      pool = static_cast<char*>(p);
      ceph::ofi::Endpoint::window_t w;
      if (int r = ep->register_window(pool, len, &w); r < 0) {
	derr << "registering the gather windows: " << ep->last_error()
	     << "; gathers stay inline" << dendl;
      } else {
	pool_window = w.id;
	slots.resize(nslots);
      }
    }
  }
  dout(1) << "libfabric delivery up: " << ep->describe() << ", staging "
	  << cfg.stage_count << " x " << cfg.stage_size << " bytes, "
	  << slots.size() << " gather windows of " << slot_size << " bytes"
	  << dendl;
  if (ep->provider() == "tcp" || ep->provider() == "sockets") {
    // closing a socket does not discard what the kernel already queued
    // on it, so a cut-off write can still land after the pool's drain
    dout(0) << "WARNING: " << ep->provider() << " cannot cut off writes "
	    << "that miss their deadline: the kernel still delivers what it "
	    << "queued. Use it for tests only." << dendl;
  }
  if (!ep->delivery_complete()) {
    dout(0) << "WARNING: " << ep->provider() << " does not promise "
	    << "delivery-complete writes; a reply may race the bytes it "
	    << "reports placed" << dendl;
  }
  return 0;
}

bool OSDOfi::is_available() const
{
  return static_cast<bool>(ep);
}

bool OSDOfi::handles(const std::string& token) const
{
  if (!ep) {
    return false;
  }
  auto t = ceph::ofi::parse_token(token);
  return t && t->provider == ep->provider();
}

ssize_t OSDOfi::execute_plan(const std::string& key,
			     const std::string& token,
			     const ceph::buffer::list& data,
			     const ceph::osd::oob::placement_plan& plan,
			     std::chrono::milliseconds budget)
{
  auto t = ceph::ofi::parse_token(token);
  if (!ep || !t) {
    return -EINVAL;
  }
  std::vector<struct iovec> iov;
  iov.reserve(data.get_num_buffers());
  for (const auto& b : data.buffers()) {
    if (b.length()) {
      iov.push_back({const_cast<char*>(b.c_str()), b.length()});
    }
  }
  std::vector<ceph::ofi::Endpoint::write_t> writes;
  writes.reserve(plan.size());
  for (const auto& tr : plan) {
    writes.push_back({tr.local_ofs, tr.len, tr.client_ofs});
  }
  plans_started++;
  const int r = ep->write(*t, iov.data(), iov.size(), writes, budget);
  if (r < 0) {
    plans_failed++;
    dout(5) << "plan for " << key << " failed: " << cpp_strerror(r)
	    << (r == -EIO || r == -ETIMEDOUT || r == -ECANCELED ?
		" (" + ep->last_error() + ")" :
		std::string{}) << dendl;
    return r;
  }
  plans_completed++;
  bytes_pushed += data.length();
  dout(20) << "placed " << data.length() << " bytes of " << key << " in "
	   << plan.size() << " range(s)" << dendl;
  return static_cast<ssize_t>(data.length());
}

std::optional<OSDOobExecutor::window_t> OSDOfi::acquire_window(size_t size)
{
  if (!ep || size > slot_size) {
    return std::nullopt;
  }
  const auto now = std::chrono::steady_clock::now();
  std::lock_guard l(win_mtx);
  for (size_t i = 0; i < slots.size(); i++) {
    auto& slot = slots[i];
    if (slot.in_use || now < slot.quarantined_until) {
      continue;
    }
    const ceph::ofi::Endpoint::window_t pw{pool_window, pool,
					   slot_size * slots.size()};
    std::string token = ep->window_token(pw, i * slot_size, slot_size);
    if (token.empty()) {
      return std::nullopt;
    }
    slot.in_use = true;
    windows_acquired++;
    window_t w;
    w.id = i;
    w.ptr = pool + i * slot_size;
    w.size = slot_size;
    w.token = std::move(token);
    return w;
  }
  windows_exhausted++;
  return std::nullopt;
}

void OSDOfi::release_window(uint64_t id, uint64_t quarantine_ms)
{
  std::lock_guard l(win_mtx);
  if (id >= slots.size()) {
    return;
  }
  slots[id].in_use = false;
  if (quarantine_ms) {
    slots[id].quarantined_until = std::chrono::steady_clock::now() +
      std::chrono::milliseconds(quarantine_ms);
  }
}

void OSDOfi::window_sync()
{
  if (ep) {
    ep->sync();
  }
}

void OSDOfi::dump_stats(ceph::Formatter* f) const
{
  f->dump_bool("available", static_cast<bool>(ep));
  if (!ep) {
    return;
  }
  f->dump_string("transport", ep->describe());
  f->dump_unsigned("plans_started", plans_started);
  f->dump_unsigned("plans_completed", plans_completed);
  f->dump_unsigned("plans_failed", plans_failed);
  f->dump_unsigned("bytes_pushed", bytes_pushed);
  const auto s = ep->stats();
  f->dump_unsigned("writes_posted", s.writes_posted);
  f->dump_unsigned("writes_failed", s.writes_failed);
  f->dump_unsigned("peers_inserted", s.peers_inserted);
  f->dump_unsigned("staging_busy", s.staging_busy);
  f->dump_unsigned("timeouts", s.timeouts);
  f->dump_unsigned("cutoffs", s.resets);
  f->dump_unsigned("windows_acquired", windows_acquired);
  f->dump_unsigned("windows_exhausted", windows_exhausted);
  f->dump_string("last_error", ep->last_error());
}

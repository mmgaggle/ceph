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

OSDOfi::OSDOfi(CephContext* cct, LogChannelRef clog)
  : cct(cct), clog(std::move(clog)) {}

OSDOfi::~OSDOfi()
{
  // the windows, then the endpoint, which stops the progress thread,
  // before the memory goes
  windows.reset();
  ep.reset();
  std::free(pool_mem);
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
  cfg.late_tolerance =
    conf.get_val<std::chrono::milliseconds>("osd_oob_cutoff_late_tolerance");
  cfg.late_fail_closed = conf.get_val<bool>("osd_oob_cutoff_late_fail_closed");
  cfg.cutoff_close_hook = [cct = cct] {
    return cct->_conf.get_val<bool>("osd_ofi_inject_cutoff_failure") ?
      -EIO : 0;
  };
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

  size_t slot_size = 0;
  if (gather) {
    slot_size = conf.get_val<Option::size_t>("osd_oob_window_size");
    const size_t nslots = conf.get_val<uint64_t>("osd_oob_window_count");
    const size_t len = slot_size * nslots;
    void* p = nullptr;
    if (len && posix_memalign(&p, 4096, len) == 0 && p) {
      std::memset(p, 0, len);
      pool_mem = static_cast<char*>(p);
      // a window per slot, so that each gets a key of its own, and a new
      // one when it is released
      windows = ceph::ofi::WindowPool::create(*ep, pool_mem, slot_size, nslots,
					      &err);
      if (!windows) {
	derr << "registering the gather windows: " << err
	     << "; gathers stay inline" << dendl;
      }
    }
  }
  dout(1) << "libfabric delivery up: " << ep->describe() << ", staging "
	  << cfg.stage_count << " x " << cfg.stage_size << " bytes, "
	  << (windows ? windows->count() : 0) << " gather windows of "
	  << slot_size << " bytes"
	  << dendl;
  if (ep->provider() == "tcp" || ep->provider() == "sockets") {
    // closing a socket does not discard what the kernel already queued
    // on it, so a cut-off write can still land after the pool's drain
    dout(0) << "WARNING: " << ep->provider() << " cannot cut off writes "
	    << "that miss their deadline: the kernel still delivers what it "
	    << "queued. Use it for tests only." << dendl;
  }
  if (const auto st = ep->stats(); st.close_discards == 0) {
    // a late write is then only cut off once the provider has drained it,
    // which the endpoint measures, and treats as unsafe when the drain
    // takes longer than the tolerance
    dout(0) << "WARNING: " << ep->provider() << " says closing an endpoint "
	    << "does not discard its writes; a write that misses its deadline "
	    << "can land late, and a cut-off that takes longer than "
	    << "osd_oob_cutoff_late_tolerance stops libfabric delivery"
	    << dendl;
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
  return ep && !stopped && !ep->unsafe();
}

bool OSDOfi::check_unsafe()
{
  if (!ep || !ep->unsafe()) {
    return false;
  }
  if (stopped.exchange(true)) {
    return true;
  }
  const std::string why = ep->last_error();
  derr << "libfabric delivery is unsafe: " << why
       << "; libfabric delivery stopped on this osd" << dendl;
  if (clog) {
    clog->error() << "libfabric delivery is unsafe: " << why
		  << "; libfabric delivery stopped";
  }
  if (cct->_conf.get_val<std::string>("osd_oob_cutoff_failure") == "abort") {
    ceph_abort_msg("libfabric delivery could not cut off its writes (" + why +
		   "); osd_oob_cutoff_failure=abort");
  }
  return true;
}

void OSDOfi::check_past_tolerance()
{
  const auto s = ep->stats();
  uint64_t seen = past_reported;
  if (s.cutoffs_past_tolerance <= seen ||
      !past_reported.compare_exchange_strong(seen, s.cutoffs_past_tolerance)) {
    return;
  }
  // The close or cancel returned, so nothing of the writes it cut off is
  // sent any more, and delivery goes on. Bytes may have landed in a
  // client's window after the fence a client that gave its request up
  // counts on: say so where an operator looks.
  const std::string why = ep->last_error();
  derr << "libfabric delivery cut writes off late: " << why << dendl;
  if (clog) {
    clog->error() << "libfabric delivery cut writes off late: " << why;
  }
}

bool OSDOfi::handles(const std::string& token) const
{
  if (!is_available()) {
    return false;
  }
  auto t = ceph::ofi::parse_token(token);
  return t && t->provider == ep->provider();
}

ssize_t OSDOfi::execute_plan(const std::string& key,
			     const std::string& token,
			     const ceph::buffer::list& data,
			     const ceph::osd::oob::placement_plan& plan,
			     std::chrono::milliseconds budget,
			     bool* started)
{
  if (started) {
    *started = false;
  }
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
  const int r = ep->write(*t, iov.data(), iov.size(), writes, budget, started);
  if (r == -ENOTRECOVERABLE || ep->unsafe()) {
    check_unsafe();
  } else if (r == -ETIMEDOUT) {
    check_past_tolerance();
  }
  if (r < 0) {
    plans_failed++;
    dout(5) << "plan for " << key << " failed: " << cpp_strerror(r)
	    << (r == -EIO || r == -ETIMEDOUT || r == -ECANCELED ||
		r == -ENOTRECOVERABLE ?
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
  if (!windows || !is_available()) {
    return std::nullopt;
  }
  auto lent = windows->acquire(size);
  if (!lent) {
    return std::nullopt;
  }
  window_t w;
  w.id = lent->id;
  w.ptr = lent->ptr;
  w.size = lent->size;
  w.token = std::move(lent->token);
  return w;
}

void OSDOfi::release_window(uint64_t id, uint64_t quarantine_ms)
{
  if (!windows) {
    return;
  }
  // A new key, so that no write meant for this gather lands once the
  // window is lent again: not a peer's that missed it, and not a
  // provider's late duplicate of one that completed, which the delivery
  // lease does not bound. A gather that consumed the push gives no
  // quarantine, and the window is free at once. One that did not - the
  // shard replied inline, or the read was cancelled - keeps it out for
  // the quarantine anyway: the shard's push may still be on the way, and
  // the new key keeps it out only if the provider drops what carries the
  // old one.
  windows->release(id, std::chrono::milliseconds(quarantine_ms),
		   cct->_conf.get_val<bool>("osd_oob_rekey_windows"));
}

bool OSDOfi::delivery_complete() const
{
  return ep && ep->delivery_complete();
}

void OSDOfi::window_sync()
{
  if (ep) {
    ep->sync();
  }
}

void OSDOfi::dump_stats(ceph::Formatter* f) const
{
  f->dump_bool("available", is_available());
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
  f->dump_unsigned("plans_cut_off", s.plans_cut_off);
  f->dump_unsigned("cancels_failed", s.cancels_failed);
  f->dump_bool("cancel_discards", s.cancel_discards);
  f->dump_int("close_discards", s.close_discards);
  f->dump_unsigned("cutoffs_failed", s.cutoffs_failed);
  f->dump_unsigned("cutoffs_late", s.cutoffs_late);
  f->dump_unsigned("max_cutoff_lateness_ms", s.max_cutoff_lateness_ms);
  f->dump_unsigned("late_tolerance_ms", s.late_tolerance_ms);
  f->dump_unsigned("cutoffs_past_tolerance", s.cutoffs_past_tolerance);
  f->dump_unsigned("max_poll_gap_ms", s.max_poll_gap_ms);
  f->dump_unsigned("cutoffs_on_behalf", s.cutoffs_on_behalf);
  f->dump_unsigned("cutoff_slack_ms", s.cutoff_slack_ms);
  f->dump_bool("unsafe", s.unsafe);
  f->dump_bool("broken", s.broken);
  f->dump_unsigned("cutoff_cost_ms", s.cutoff_cost_ms);
  f->dump_unsigned("budget_refused", s.budget_refused);
  f->dump_unsigned("late_starts", s.late_starts);
  f->dump_unsigned("peer_timeouts", s.peer_timeouts);
  f->dump_unsigned("peers", s.peers);
  f->dump_unsigned("pending_inserts", s.pending_inserts);
  f->dump_unsigned("inserts_refused", s.inserts_refused);
  const auto ws = windows ? windows->stats() : ceph::ofi::WindowPool::stats_t{};
  f->dump_unsigned("windows_acquired", ws.acquired);
  f->dump_unsigned("windows_exhausted", ws.exhausted);
  f->dump_unsigned("gather_crc_mismatch", get_gather_crc_mismatch());
  f->dump_unsigned("windows_rekeyed", ws.rekeyed);
  f->dump_unsigned("windows_rekeyed_quarantined", ws.rekeyed_quarantined);
  f->dump_unsigned("windows_rekey_failed", ws.rekey_failed);
  f->dump_unsigned("rekeys_in_place", s.rekeys_in_place);
  f->dump_unsigned("rekeys_reregistered", s.rekeys_reregistered);
  f->dump_int("rekey_in_place", s.rekey_in_place);
  f->dump_unsigned("key_collisions", s.key_collisions);
  f->dump_string("last_error", ep->last_error());
}

void OSDOfi::get_alerts(std::map<std::string, std::string>& alerts) const
{
  if (!ep) {
    return;
  }
  const auto s = ep->stats();
  if (s.unsafe) {
    alerts.emplace("OOB_DELIVERY_UNSAFE",
		   "libfabric delivery could not cut off writes in time (" +
		   ep->last_error() + "); libfabric delivery stopped");
    return;
  }
  if (s.broken) {
    alerts.emplace("OOB_DELIVERY_DOWN",
		   "libfabric delivery could not reopen its endpoint (" +
		   ep->last_error() + "); libfabric delivery stopped");
    return;
  }
  const auto now = std::chrono::steady_clock::now();
  const auto period =
    cct->_conf.get_val<std::chrono::seconds>("osd_oob_cutoff_late_alert_period");
  const auto minutes = std::to_string(
    std::chrono::duration_cast<std::chrono::minutes>(period).count());
  if (s.cutoffs_past_tolerance && now - s.last_past_cutoff < period) {
    std::string pause;
    if (s.max_poll_gap_ms) {
      pause = "; its threads once went " + std::to_string(s.max_poll_gap_ms) +
	" ms without running, as when the process is stopped or paused";
    }
    alerts.emplace("OOB_CUTOFF_PAST_TOLERANCE",
		   std::to_string(s.cutoffs_past_tolerance) + " write cut-off(s) "
		   "ended later than the tolerance of " +
		   std::to_string(s.late_tolerance_ms) + " ms, the latest "
		   "within the last " + minutes + " minutes, by up to " +
		   std::to_string(s.max_cutoff_lateness_ms) + " ms" + pause +
		   "; delivery goes on. A client that gave a request up and "
		   "reused its window after the pool's lease and drain may "
		   "have had it written");
  } else if (s.cutoffs_late && now - s.last_late_cutoff < period) {
    alerts.emplace("OOB_CUTOFF_LATE",
		   std::to_string(s.cutoffs_late) + " write cut-off(s) ended "
		   "after their budget, the latest within the last " + minutes +
		   " minutes, by up to " +
		   std::to_string(s.max_cutoff_lateness_ms) + " ms, within "
		   "the tolerance of " + std::to_string(s.late_tolerance_ms) +
		   " ms; a client that gives a request up must allow that "
		   "beyond the pool's delivery lease and drain");
  }
}

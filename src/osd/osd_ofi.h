// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/LogClient.h"
#include "include/common_fwd.h"
#include "osd/oob_executor.h"

namespace ceph::ofi { class Endpoint; class WindowPool; }

/**
 * Out-of-band delivery over libfabric.
 *
 * Serves delivery tokens of the form ceph::ofi::token_t describes
 * ("<base>:<size>:ofi1:<provider>:<endpoint>:<key>") whose provider is
 * the one this OSD runs (osd_ofi_provider). The provider picks the
 * wire: tcp or shm need no special hardware, verbs runs RC under the
 * rxm utility provider, efa runs SRD, and a UET provider runs Ultra
 * Ethernet. The window owner never inserts this OSD into its address
 * vector, so any OSD holding the token can write, as with cuObject's DC
 * transport.
 *
 * With osd_oob_gather, the executor also lends windows: a pool of
 * osd_oob_window_count windows of osd_oob_window_size bytes registered
 * for remote writes, which shards of an erasure-coded read push into.
 * With osd_oob_rekey_windows, a window gets a new memory key when it is
 * released, so that no write meant for an earlier gather, a provider's
 * late duplicate included, lands in it once it is lent again.
 *
 * When a cut-off fails, writes may land in a client's window after the
 * client was told they would not (see ceph::ofi::Endpoint::write()). The
 * executor then stops: it serves no token and lends no window, logs to
 * the cluster log once, and raises the OOB_DELIVERY_UNSAFE health alert.
 * With osd_oob_cutoff_failure set to abort, the OSD exits instead, which
 * ends the writes of a software provider and makes a device drop the
 * queue pair's. A quick cut-off that ends later than
 * osd_oob_cutoff_late_tolerance fails only with
 * osd_oob_cutoff_late_fail_closed; otherwise the executor logs it to the
 * cluster log, raises OOB_CUTOFF_PAST_TOLERANCE, and goes on.
 */
class OSDOfi : public OSDOobExecutor {
public:
  OSDOfi(CephContext* cct, LogChannelRef clog);
  ~OSDOfi() override;

  OSDOfi(const OSDOfi&) = delete;
  OSDOfi& operator=(const OSDOfi&) = delete;

  /// open the endpoint, the staging buffers and the window pool
  int init();

  bool is_available() const override;
  bool handles(const std::string& token) const override;
  ssize_t execute_plan(const std::string& key,
		       const std::string& token,
		       const ceph::buffer::list& data,
		       const ceph::osd::oob::placement_plan& plan,
		       std::chrono::milliseconds budget,
		       bool* started) override;
  void dump_stats(ceph::Formatter* f) const override;
  std::optional<window_t> acquire_window(size_t size) override;
  void release_window(uint64_t id, uint64_t quarantine_ms) override;
  void window_sync() override;
  void get_alerts(std::map<std::string, std::string>& alerts) const override;
  bool delivery_complete() const override;

private:
  CephContext* cct;
  LogChannelRef clog;
  std::unique_ptr<ceph::ofi::Endpoint> ep;
  /// the endpoint went unsafe and this executor stopped; reported once
  std::atomic<bool> stopped{false};
  /// stop for good once the endpoint is unsafe; true when stopped
  bool check_unsafe();
  /// report, once each, the cut-offs that ended beyond the tolerance
  /// though the endpoint stayed clean
  void check_past_tolerance();
  std::atomic<uint64_t> past_reported{0};

  /// the gather windows, over pool_mem; null when gathers stay inline
  std::unique_ptr<ceph::ofi::WindowPool> windows;
  char* pool_mem = nullptr;

  std::atomic<uint64_t> plans_started{0};
  std::atomic<uint64_t> plans_completed{0};
  std::atomic<uint64_t> plans_failed{0};
  std::atomic<uint64_t> bytes_pushed{0};

};

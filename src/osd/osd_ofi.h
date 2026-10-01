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

#include "include/common_fwd.h"
#include "osd/oob_executor.h"

namespace ceph::ofi { class Endpoint; }

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
 */
class OSDOfi : public OSDOobExecutor {
public:
  explicit OSDOfi(CephContext* cct);
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
		       std::chrono::milliseconds budget) override;
  void dump_stats(ceph::Formatter* f) const override;
  std::optional<window_t> acquire_window(size_t size) override;
  void release_window(uint64_t id, uint64_t quarantine_ms) override;
  void window_sync() override;

private:
  CephContext* cct;
  std::unique_ptr<ceph::ofi::Endpoint> ep;

  struct slot_t {
    bool in_use = false;
    std::chrono::steady_clock::time_point quarantined_until{};
  };
  std::mutex win_mtx;
  std::vector<slot_t> slots;
  size_t slot_size = 0;
  char* pool = nullptr;
  uint64_t pool_window = 0;  ///< the pool's id in ep

  std::atomic<uint64_t> plans_started{0};
  std::atomic<uint64_t> plans_completed{0};
  std::atomic<uint64_t> plans_failed{0};
  std::atomic<uint64_t> bytes_pushed{0};
  std::atomic<uint64_t> windows_acquired{0};
  std::atomic<uint64_t> windows_exhausted{0};
};

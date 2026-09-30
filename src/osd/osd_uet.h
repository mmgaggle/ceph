// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "include/common_fwd.h"
#include "osd/oob_executor.h"

/**
 * A UET delivery descriptor:
 *
 *   <base hex>:<window size hex>:uet1:<client IPv4>:<memory key hex>
 *
 * The addr:size prefix is the one every delivery token starts with, so
 * the gateway's passthrough eligibility checks apply unchanged. The
 * rest names the client's fabric endpoint and the key of the memory
 * region backing the window. With the UEC reference provider the region
 * is zero-based, so the base is 0 and triples address offsets into it.
 */
struct uet_token_t {
  uint64_t base = 0;
  uint64_t size = 0;
  uint32_t ipv4 = 0;  ///< host byte order
  uint64_t key = 0;
};

std::optional<uet_token_t> parse_uet_token(std::string_view token);

/**
 * Out-of-band delivery over Ultra Ethernet Transport (mock-up).
 *
 * Writes each placement triple into the client's window with RMA writes
 * in UET's RUDI mode: reliable, unordered, connectionless and meant for
 * idempotent operations. The client holds no state for this OSD - no
 * address-vector entry, no handshake - so any OSD holding the token can
 * write, as with cuObject's DC transport; unlike DC it works on any
 * Ethernet NIC. The completion the OSD waits for is the initiator-side
 * one, so the reply that follows means the bytes are placed.
 *
 * Built on the UEC reference provider (uet-ref-prov), a software UET
 * stack over raw Ethernet sockets: the OSD needs CAP_NET_RAW and an
 * interface (osd_uet_ifname). The provider keeps process-global state,
 * so there is one executor per OSD, and every call into it is
 * serialized. The provider takes its settings from the environment,
 * which init() sets before the provider starts.
 */
class OSDUet : public OSDOobExecutor {
public:
  explicit OSDUet(CephContext* cct);
  ~OSDUet() override;

  OSDUet(const OSDUet&) = delete;
  OSDUet& operator=(const OSDUet&) = delete;

  /// bring up the provider, the endpoint and the staging region
  int init();

  bool is_available() const override;
  bool handles(const std::string& token) const override;
  ssize_t execute_plan(const std::string& key,
		       const std::string& token,
		       const ceph::buffer::list& data,
		       const ceph::osd::oob::placement_plan& plan) override;
  void dump_stats(ceph::Formatter* f) const override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl;
  CephContext* cct;

  std::atomic<uint64_t> plans_started{0};
  std::atomic<uint64_t> plans_completed{0};
  std::atomic<uint64_t> plans_failed{0};
  std::atomic<uint64_t> bytes_pushed{0};
  std::atomic<uint64_t> writes_posted{0};
};

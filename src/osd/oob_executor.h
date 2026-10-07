// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <sys/types.h>

#include "include/buffer.h"
#include "osd/oob_placement.h"

namespace ceph { class Formatter; }

/**
 * A transport that can execute an out-of-band placement plan: write
 * each triple's bytes of a read reply into the client memory window
 * a delivery descriptor's token names.
 *
 * The delivery descriptor is opaque to the OSD's op path; each
 * executor recognizes the token shapes it can serve. cuObject tokens
 * name a Dynamically Connected target; libfabric tokens name a
 * provider, an endpoint and a memory key. PrimaryLogPG asks OSDService
 * for the executor that handles a token and falls back to inline
 * delivery when there is none.
 */
class OSDOobExecutor {
public:
  virtual ~OSDOobExecutor() = default;

  /// true once the transport is up
  virtual bool is_available() const = 0;

  /// true when this executor can serve the token's transport
  virtual bool handles(const std::string& token) const = 0;

  /**
   * Execute the plan: every triple's bytes of data go to the token's
   * window at the token's base plus triple.client_ofs. All or
   * nothing: returns the total bytes placed only if every triple
   * completed, else a negative errno, and the caller then delivers
   * inline. Blocks until the transfer completes.
   *
   * The budget bounds when the writes may still land: the caller
   * derives it from the pool's delivery lease and drain, counted from
   * receipt of the request. An executor that cannot finish within it
   * cuts its writes off, or does not start them, so that nothing lands
   * after the window's owner may reuse the window.
   *
   * *started, when given, says whether any write was handed to the
   * transport: false means that nothing of the plan reached the window,
   * whatever the result, and the OSD tells the client so.
   */
  virtual ssize_t execute_plan(const std::string& key,
			       const std::string& token,
			       const ceph::buffer::list& data,
			       const ceph::osd::oob::placement_plan& plan,
			       std::chrono::milliseconds budget,
			       bool* started) = 0;

  /// true when a plan that succeeded had every byte placed in the
  /// window's memory before execute_plan() returned
  virtual bool delivery_complete() const { return false; }

  /// true when this executor can pull from the token's window: see
  /// execute_pull()
  virtual bool pulls(const std::string& token) const { return false; }

  /**
   * Execute the plan the other way: read every triple's bytes out of
   * the token's window, at the token's base plus triple.client_ofs,
   * into a buffer of total bytes at triple.local_ofs, and hand that
   * buffer to *out. The plan must cover the buffer exactly. All or
   * nothing: returns total only if every triple completed, and *out is
   * untouched otherwise. Blocks until the transfer completes, or until
   * the budget runs out. A read still in flight then can only land in
   * the executor's own memory, which it does not reuse until the read
   * completes.
   *
   * This is how an OSD takes the payload of a write out of client
   * memory (delivery_t::FLAG_PULL). *started says whether any read was
   * handed to the transport.
   */
  virtual ssize_t execute_pull(const std::string& key,
			       const std::string& token,
			       const ceph::osd::oob::placement_plan& plan,
			       uint64_t total,
			       ceph::buffer::list* out,
			       std::chrono::milliseconds budget,
			       bool* started) {
    if (started) {
      *started = false;
    }
    return -EOPNOTSUPP;
  }

  /// asok/debug counters
  virtual void dump_stats(ceph::Formatter* f) const = 0;

  /**
   * Add this executor's health alerts, by alert name, to an OSD's alerts
   * (osd_alert_list_t). The OSD reports them with its stats, and the
   * manager raises each name as a health warning.
   */
  virtual void get_alerts(std::map<std::string, std::string>& alerts) const {}

  /**
   * A registered region of this OSD's memory that peers can push into,
   * named by a token of this executor's transport. A primary gathering
   * shard reads hands the token to each shard in its sub-read, and the
   * shards place their data here instead of sending it in the reply.
   */
  struct window_t {
    uint64_t id = 0;
    char* ptr = nullptr;
    size_t size = 0;
    std::string token;
  };

  /// a window of at least size bytes, or nullopt when the transport
  /// cannot receive or none is free
  virtual std::optional<window_t> acquire_window(size_t size) {
    return std::nullopt;
  }

  /**
   * Return a window. quarantine_ms keeps it out of use that long: after
   * a gather that did not finish cleanly, a peer that received the
   * token may still write into it until the pool's delivery lease runs
   * out.
   */
  virtual void release_window(uint64_t id, uint64_t quarantine_ms) {}

  /**
   * Order the caller's reads of window memory after the transport's
   * writes to it. A software transport places data from its own
   * thread; a gather reads the window from the op thread that handled
   * the shard's reply, and nothing else orders the two.
   */
  virtual void window_sync() {}

  /// a shard's push into one of our windows did not match the shard's
  /// checksum of it: the read went to the other shards instead. Counted
  /// here, whichever transport lent the window.
  void note_gather_crc_mismatch() { gather_crc_mismatch++; }
  uint64_t get_gather_crc_mismatch() const { return gather_crc_mismatch; }

private:
  std::atomic<uint64_t> gather_crc_mismatch{0};
};

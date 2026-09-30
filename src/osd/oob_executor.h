// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

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
 * name a Dynamically Connected target; UET tokens name a fabric
 * endpoint and a memory key. PrimaryLogPG asks OSDService for the
 * executor that handles a token and falls back to inline delivery
 * when there is none.
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
   */
  virtual ssize_t execute_plan(const std::string& key,
			       const std::string& token,
			       const ceph::buffer::list& data,
			       const ceph::osd::oob::placement_plan& plan) = 0;

  /// asok/debug counters
  virtual void dump_stats(ceph::Formatter* f) const = 0;
};

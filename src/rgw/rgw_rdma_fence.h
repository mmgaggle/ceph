// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <ostream>

#include "include/rados/librados.hpp"

namespace rgw::rdma {

/**
 * Whether a fallback after OSD-direct reads must wait out the pools'
 * delivery lease and drain before anything else writes the window.
 *
 * Not when every descriptor-bearing read came back, without having been
 * resent, saying either that its OSD started no transfer for it
 * (declined) or that every byte was in the window before the OSD replied
 * (landed): then no write of the request can land any more. A read that
 * was resent may have started a transfer in an earlier attempt, on
 * another OSD, that its result knows nothing of. A read without a result,
 * as one that timed out, or one from an OSD that does not report either,
 * counts as having started a transfer.
 */
template <typename Results>
bool fence_needed(const Results& results)
{
  using R = librados::ObjectReadOperation;
  for (const auto& r : results) {
    if (r.flags & R::RDMA_DELIVERY_RESENT) {
      return true;
    }
    if (!(r.flags & (R::RDMA_DELIVERY_DECLINED | R::RDMA_DELIVERY_LANDED))) {
      return true;
    }
  }
  return false;
}

/**
 * Whether a write of a GET's descriptor-bearing reads, every one of them
 * completed, may still land in the window: the fence a response waits
 * out, or a relay window's quarantine in its place. needed is what
 * fence_needed() says of the results, resent whether one of them is
 * RDMA_DELIVERY_RESENT. After a GET that failed, when the fence is
 * needed. After one that delivered everything, each stripe's OSD placed
 * its bytes before replying, so only when a read was resent: its earlier
 * attempt may write after every reply.
 */
inline bool write_may_land(bool success, bool needed, bool resent)
{
  return success ? resent : needed;
}

/// what a passthrough GET makes of one stripe read's reply
enum class stripe_reply {
  failed,  ///< the read failed: the GET fails with its error
  inline_data,  ///< the OSD returned the data: the GET falls back to HTTP
  placed,  ///< the OSD placed the data in the window
};

/**
 * A stripe read that failed fails the GET. One whose OSD returned its data
 * inline sends the GET back to HTTP, whatever its delivery result says:
 * the OSD declined to push, or its push was cut off - per plan or with the
 * endpoint - and it answered with the data instead, so the window may
 * hold part of the stripe, and only the fallback rewrites it, behind the
 * fence when one is needed. Anything else was placed.
 */
inline stripe_reply classify_stripe(int rval, uint64_t inline_bytes)
{
  if (rval < 0) {
    return stripe_reply::failed;
  }
  return inline_bytes > 0 ? stripe_reply::inline_data : stripe_reply::placed;
}

/// the delivery results of a GET's stripe reads, counted for a log line
struct results_summary {
  size_t reads = 0;
  size_t declined = 0;
  size_t landed = 0;
  size_t resent = 0;
  /// neither declined nor landed: a transfer may have started
  size_t open = 0;
  uint64_t bytes = 0;
};

template <typename Results>
results_summary summarize(const Results& results)
{
  using R = librados::ObjectReadOperation;
  results_summary s;
  for (const auto& r : results) {
    s.reads++;
    s.bytes += r.bytes;
    if (r.flags & R::RDMA_DELIVERY_RESENT) {
      s.resent++;
    }
    if (r.flags & R::RDMA_DELIVERY_DECLINED) {
      s.declined++;
    } else if (r.flags & R::RDMA_DELIVERY_LANDED) {
      s.landed++;
    } else {
      s.open++;
    }
  }
  return s;
}

inline std::ostream& operator<<(std::ostream& o, const results_summary& s)
{
  return o << s.reads << " reads (" << s.declined << " declined, "
           << s.landed << " landed, " << s.open << " open, " << s.resent
           << " resent), " << s.bytes << " bytes placed";
}

} // namespace rgw::rdma

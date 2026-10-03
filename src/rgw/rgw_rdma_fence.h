// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

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

} // namespace rgw::rdma

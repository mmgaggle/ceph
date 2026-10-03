// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <optional>
#include <utility>

#include "include/buffer.h"

namespace ceph::osd::oob {

/// what take_pushed() found in a gather window
struct pushed_take_t {
  uint64_t bytes = 0;     ///< bytes copied out of the window
  uint32_t crc = 0;       ///< their crc32c, seeded with -1
  bool overrun = false;   ///< the extents run past the end of the window
  bool mismatch = false;  ///< the copy does not match the shard's crc32c
  bool ok() const { return !overrun && !mismatch; }
};

/**
 * Rebuild the buffers a shard pushed into a gather window instead of
 * returning them in its reply: its extents sit there back to back, in
 * reply order. They are copied out first and checked after, so a write
 * that lands in the window meanwhile cannot change what was checked.
 *
 * When the shard sent the crc32c of what it pushed, the copy has to match
 * it. Without the check, the window's memory would pass for the shard's
 * data whatever put it there: a push that completed short, a stray or
 * late write that landed after it, or a window lent to two gathers.
 *
 * On success the extents are appended to out; on failure out is left
 * as it was.
 */
template <typename Key>
pushed_take_t take_pushed(
  const char* win, uint64_t size,
  const std::map<Key, std::list<std::pair<uint64_t, uint64_t>>>& pushed,
  const std::optional<uint32_t>& want_crc,
  std::map<Key, std::list<std::pair<uint64_t, ceph::buffer::list>>>& out)
{
  pushed_take_t t;
  t.crc = static_cast<uint32_t>(-1);
  std::map<Key, std::list<std::pair<uint64_t, ceph::buffer::list>>> got;
  for (const auto& [key, extents] : pushed) {
    for (const auto& [offset, len] : extents) {
      if (len > size - t.bytes) {
	t.overrun = true;
	return t;
      }
      ceph::buffer::list bl;
      bl.append(win + t.bytes, len);
      t.crc = bl.crc32c(t.crc);
      got[key].emplace_back(offset, std::move(bl));
      t.bytes += len;
    }
  }
  if (want_crc && *want_crc != t.crc) {
    t.mismatch = true;
    return t;
  }
  for (auto& [key, extents] : got) {
    auto& o = out[key];
    o.splice(o.end(), extents);
  }
  return t;
}

} // namespace ceph::osd::oob

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/oob_arena.h"
#include "common/Formatter.h"

#include <algorithm>
#include <bit>
#include <cstdlib>

namespace ceph::oob {

std::unique_ptr<Arena> Arena::create(uint64_t size, std::string* err)
{
  size = std::max<uint64_t>(MAX, size / MAX * MAX);
  void* p = nullptr;
  // aligned to a huge page, so that a provider can map it with few
  // entries
  if (posix_memalign(&p, 2 << 20, size) != 0 || !p) {
    *err = "cannot allocate " + std::to_string(size) +
      " bytes for out-of-band transfers";
    return nullptr;
  }
  return std::unique_ptr<Arena>(new Arena(static_cast<char*>(p), size));
}

Arena::Arena(char* base, uint64_t size)
  : base_(base), size_(size)
{
  for (uint64_t ofs = 0; ofs < size; ofs += MAX) {
    free_[ORDERS - 1].insert(ofs);
  }
  st.size = size;
}

Arena::~Arena()
{
  std::free(base_);
}

uint64_t Arena::round(uint64_t len)
{
  if (len > MAX) {
    return 0;
  }
  return std::max(MIN, std::bit_ceil(len));
}

unsigned Arena::order_of(uint64_t len)
{
  return static_cast<unsigned>(std::countr_zero(round(len) / MIN));
}

Arena::slice_t Arena::alloc(uint64_t len)
{
  const uint64_t need = round(len);
  std::lock_guard l(mtx);
  if (need == 0) {
    st.failed++;
    return {};
  }
  const unsigned k = order_of(need);
  unsigned j = k;
  while (j < ORDERS && free_[j].empty()) {
    j++;
  }
  if (j == ORDERS) {
    st.failed++;
    return {};
  }
  const uint64_t ofs = *free_[j].begin();
  free_[j].erase(free_[j].begin());
  // split down to the order asked for, keeping the lower halves
  while (j > k) {
    j--;
    free_[j].insert(ofs + (MIN << j));
  }
  st.in_use += need;
  st.allocs++;
  return {ofs, need};
}

void Arena::free(uint64_t offset, uint64_t len)
{
  const uint64_t size = round(len);
  if (size == 0) {
    return;
  }
  std::lock_guard l(mtx);
  st.in_use -= size;
  unsigned k = order_of(size);
  uint64_t ofs = offset;
  // join with the buddy while it is free too
  while (k + 1 < ORDERS) {
    const uint64_t buddy = ofs ^ (MIN << k);
    if (free_[k].erase(buddy) == 0) {
      break;
    }
    ofs = std::min(ofs, buddy);
    k++;
  }
  free_[k].insert(ofs);
}

char* Arena::reserve(uint64_t len)
{
  const uint64_t blocks = (len + MAX - 1) / MAX;
  if (blocks == 0) {
    return nullptr;
  }
  std::lock_guard l(mtx);
  auto& top = free_[ORDERS - 1];
  // the highest run, so that buffers stay at the start
  for (auto it = top.rbegin(); it != top.rend(); ++it) {
    const uint64_t last = *it;
    if (last + MAX < blocks * MAX) {
      break;
    }
    const uint64_t first = last + MAX - blocks * MAX;
    uint64_t n = 0;
    while (n < blocks && top.count(first + n * MAX)) {
      n++;
    }
    if (n == blocks) {
      for (uint64_t i = 0; i < blocks; i++) {
        top.erase(first + i * MAX);
      }
      st.reserved += blocks * MAX;
      return base_ + first;
    }
  }
  return nullptr;
}

Arena::stats_t Arena::stats() const
{
  std::lock_guard l(mtx);
  return st;
}

void Arena::dump(ceph::Formatter* f) const
{
  const auto s = stats();
  f->dump_unsigned("arena_size", s.size);
  f->dump_unsigned("arena_in_use", s.in_use);
  f->dump_unsigned("arena_reserved", s.reserved);
  f->dump_unsigned("arena_allocs", s.allocs);
  f->dump_unsigned("arena_failed", s.failed);
}

} // namespace ceph::oob

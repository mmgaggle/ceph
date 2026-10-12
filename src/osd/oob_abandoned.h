// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

/**
 * Plans that stopped waiting for their writes, for an executor whose
 * transport cannot cancel a posted write (cuObject).
 *
 * Every write carries a handle that is unique in the process: a plan
 * reserves count handles from first, and its write i carries
 * first + i. A plan that stops waiting while writes are still posted
 * keeps the staging buffer that they read, and records it here. Their
 * completions come back to whoever polls the channel next: credit()
 * counts each one against the plan that posted it, and hands that
 * plan's buffer back with the last one. drop() hands every buffer back,
 * for when the transport flushed what was posted.
 *
 * Free of the transport, so that the bookkeeping can be tested without
 * one.
 */
namespace ceph::osd::oob {

/// true when h is one of the count handles from first
inline bool owns_handle(uint64_t first, uint64_t count, uint64_t h)
{
  return h - first < count;  // unsigned: an h below first wraps high
}

template <typename Buf>
class abandoned_plans {
public:
  bool empty() const { return plans.empty(); }
  size_t size() const { return plans.size(); }

  /// a plan with count handles from first stopped waiting while
  /// outstanding (more than 0) of its writes were still posted
  void abandon(uint64_t first, uint64_t count, uint64_t outstanding, Buf buf)
  {
    plans.push_back({first, count, outstanding, std::move(buf)});
  }

  enum class credit_t {
    unknown,  ///< no plan here posted it, or drop() gave its plan up
    counted,  ///< its plan still has writes posted
    last,     ///< its plan's last write: *buf is that plan's buffer
  };

  /// the completion of the write that carried handle h
  credit_t credit(uint64_t h, Buf* buf)
  {
    auto p = std::find_if(plans.begin(), plans.end(),
			  [h](const plan_t& a) {
			    return owns_handle(a.first, a.count, h);
			  });
    if (p == plans.end()) {
      return credit_t::unknown;
    }
    if (--p->outstanding > 0) {
      return credit_t::counted;
    }
    *buf = std::move(p->buf);
    plans.erase(p);
    return credit_t::last;
  }

  /// the transport flushed every posted write: all buffers come back
  std::vector<Buf> drop()
  {
    std::vector<Buf> bufs;
    bufs.reserve(plans.size());
    for (auto& a : plans) {
      bufs.push_back(std::move(a.buf));
    }
    plans.clear();
    return bufs;
  }

private:
  struct plan_t {
    uint64_t first;        // the plan's first handle
    uint64_t count;        // the handles it reserved
    uint64_t outstanding;  // its writes still posted
    Buf buf;
  };
  std::vector<plan_t> plans;
};

} // namespace ceph::osd::oob

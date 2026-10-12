// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/osd_ofi.h"

#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "global/global_context.h"
#include "common/ofi_rma.h"
#include "common/oob_arena.h"
#include "common/oob_config.h"
#include "osd/oob_placement.h"

namespace {

/// a gather window, as the executor sizes them
constexpr size_t WINDOW = 8 << 20;

/// a libfabric executor over tcp on the loopback, which needs no special
/// hardware, staging in an arena of blocks of the arena's largest buffer
std::unique_ptr<ceph::oob::Arena> arena(size_t blocks)
{
  std::string err;
  auto a = ceph::oob::Arena::create(blocks * ceph::oob::Arena::MAX, &err);
  EXPECT_TRUE(a) << err;
  return a;
}

ceph::oob::location_t loopback()
{
  ceph::oob::location_t loc;
  loc.addr = "127.0.0.1";
  loc.source = "oob_addr";
  return loc;
}

} // anonymous namespace

// Only the executor of the cluster transport lends the gather windows,
// and registers them. Another one pins no memory for windows, and lends
// none.
TEST(OSDOfi, RegistersWindowsOnlyWhenLending)
{
  {
    auto a = arena(1);
    OSDOfi ofi(g_ceph_context, nullptr);
    if (ofi.init("tcp", loopback(), false, a.get()) != 0) {
      GTEST_SKIP() << "tcp is not available";
    }
    EXPECT_TRUE(ofi.is_available());
    EXPECT_FALSE(ofi.acquire_window(4096));
    EXPECT_EQ(0u, a->stats().reserved);
  }
  {
    // the windows take a run of the arena: 16 of 8 MiB, two blocks
    auto a = arena(3);
    OSDOfi ofi(g_ceph_context, nullptr);
    ASSERT_EQ(0, ofi.init("tcp", loopback(), true, a.get()));
    EXPECT_EQ(2 * ceph::oob::Arena::MAX, a->stats().reserved);
    std::vector<OSDOobExecutor::window_t> lent;
    while (auto w = ofi.acquire_window(4096)) {
      EXPECT_EQ(WINDOW, w->size);
      EXPECT_FALSE(w->token.empty());
      lent.push_back(*w);
    }
    EXPECT_EQ(16u, lent.size());
    EXPECT_FALSE(ofi.acquire_window(WINDOW + 1));
    for (const auto& w : lent) {
      ofi.release_window(w.id, 0);
    }
  }
  {
    // no room for the windows: the executor still delivers
    auto a = arena(1);
    OSDOfi ofi(g_ceph_context, nullptr);
    ASSERT_EQ(0, ofi.init("tcp", loopback(), true, a.get()));
    EXPECT_FALSE(ofi.acquire_window(4096));
  }
}

namespace {

/// a client that lends a window of data that peers may read, as an
/// OSD-direct PUT's client does
struct client_t {
  std::unique_ptr<ceph::ofi::Endpoint> ep;
  std::vector<char> mem;
  ceph::ofi::Endpoint::window_t w;
  std::string token;
};

client_t lend(size_t n, unsigned access)
{
  client_t c;
  ceph::ofi::config_t cfg;
  cfg.provider = "tcp";
  cfg.node = "127.0.0.1";
  cfg.progress_thread = true;  // tcp serves remote reads only when polled
  cfg.reads = true;
  std::string err;
  c.ep = ceph::ofi::Endpoint::open(cfg, &err);
  if (!c.ep) {
    return c;
  }
  c.mem.resize(n);
  for (size_t i = 0; i < n; i++) {
    c.mem[i] = static_cast<char>(i * 31 + 7);
  }
  if (c.ep->register_window(c.mem.data(), n, ceph::ofi::memory_t{}, access,
			    &c.w) == 0) {
    c.token = c.ep->window_token(c.w, 0, n);
  }
  return c;
}

} // anonymous namespace

// An OSD-direct PUT: the primary pulls a write's payload out of the
// client's window, through its staging, into a buffer of its own.
TEST(OSDOfi, PullsAWritePayload)
{
  auto a = arena(1);
  OSDOfi ofi(g_ceph_context, nullptr);
  if (ofi.init("tcp", loopback(), false, a.get()) != 0) {
    GTEST_SKIP() << "tcp is not available";
  }
  auto c = lend(WINDOW, ceph::ofi::Endpoint::remote_read);
  ASSERT_TRUE(c.ep);
  ASSERT_FALSE(c.token.empty()) << c.ep->last_error();
  ASSERT_TRUE(ofi.pulls(c.token));

  // the second half of the window, as a stripe at offset WINDOW / 2
  const uint64_t n = WINDOW / 2;
  const auto plan = ceph::osd::oob::linear_plan(WINDOW / 2, n);
  ceph::buffer::list out;
  bool started = false;
  ASSERT_EQ(ssize_t(n), ofi.execute_pull("obj", c.token, plan, n, &out,
					 std::chrono::seconds(5), &started));
  EXPECT_TRUE(started);
  ASSERT_EQ(n, out.length());
  EXPECT_EQ(0, memcmp(out.c_str(), c.mem.data() + WINDOW / 2, n));

  // a window lent for writes only cannot be read, and a failed pull
  // hands nothing out
  auto wo = lend(WINDOW, ceph::ofi::Endpoint::remote_write);
  ASSERT_FALSE(wo.token.empty());
  ceph::buffer::list none;
  none.append("untouched");
  EXPECT_LT(ofi.execute_pull("obj", wo.token, ceph::osd::oob::linear_plan(0, n),
			     n, &none, std::chrono::seconds(2), nullptr), 0);
  EXPECT_EQ("untouched", none.to_str());

  // the staging buffer went back to the arena
  EXPECT_EQ(0u, a->stats().in_use);

  // a payload larger than the arena's largest buffer is not one it can
  // pull
  const size_t over = ceph::oob::Arena::MAX + 4096;
  auto large = lend(over, ceph::ofi::Endpoint::remote_read);
  ASSERT_FALSE(large.token.empty());
  ceph::buffer::list big;
  EXPECT_EQ(-E2BIG, ofi.execute_pull(
	      "obj", large.token, ceph::osd::oob::linear_plan(0, over),
	      over, &big, std::chrono::seconds(2), nullptr));
}

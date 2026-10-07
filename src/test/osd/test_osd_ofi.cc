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
#include "osd/oob_placement.h"

namespace {

constexpr size_t WINDOW = 64 << 10;

/// a libfabric executor over tcp on the loopback, which needs no special
/// hardware, with small staging and two gather windows
void configure()
{
  auto& conf = g_ceph_context->_conf;
  // startup options, set after the test's global_init
  conf._clear_safe_to_start_threads();
  conf.set_val_or_die("osd_ofi_provider", "tcp");
  conf.set_val_or_die("osd_ofi_node", "127.0.0.1");
  conf.set_val_or_die("osd_oob_buffer_size", std::to_string(WINDOW));
  conf.set_val_or_die("osd_oob_buffer_count", "2");
  conf.set_val_or_die("osd_oob_window_size", std::to_string(WINDOW));
  conf.set_val_or_die("osd_oob_window_count", "2");
  // on, so that what the executor is told decides, not this option
  conf.set_val_or_die("osd_oob_gather", "true");
  conf.apply_changes(nullptr);
}

} // anonymous namespace

// Only the executor that lends the gather windows registers them (see
// OSDService::oob_next_lends()). Another one pins no memory for windows,
// and lends none, though osd_oob_gather is on.
TEST(OSDOfi, RegistersWindowsOnlyWhenLending)
{
  configure();
  {
    OSDOfi ofi(g_ceph_context, nullptr);
    if (ofi.init(false) != 0) {
      GTEST_SKIP() << "tcp is not available";
    }
    EXPECT_TRUE(ofi.is_available());
    EXPECT_FALSE(ofi.acquire_window(4096));
  }
  {
    OSDOfi ofi(g_ceph_context, nullptr);
    ASSERT_EQ(0, ofi.init(true));
    auto a = ofi.acquire_window(4096);
    ASSERT_TRUE(a);
    EXPECT_EQ(WINDOW, a->size);
    EXPECT_FALSE(a->token.empty());
    auto b = ofi.acquire_window(WINDOW);
    ASSERT_TRUE(b);
    EXPECT_NE(a->id, b->id);
    // both windows are lent
    EXPECT_FALSE(ofi.acquire_window(1));
    ofi.release_window(a->id, 0);
    ofi.release_window(b->id, 0);
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
  configure();
  g_ceph_context->_conf.set_val_or_die("osd_oob_pull", "true");
  g_ceph_context->_conf.apply_changes(nullptr);
  OSDOfi ofi(g_ceph_context, nullptr);
  if (ofi.init(false) != 0) {
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

  // a payload larger than a staging buffer is not one it can pull
  auto large = lend(2 * WINDOW, ceph::ofi::Endpoint::remote_read);
  ASSERT_FALSE(large.token.empty());
  ceph::buffer::list big;
  EXPECT_EQ(-E2BIG, ofi.execute_pull(
	      "obj", large.token, ceph::osd::oob::linear_plan(0, 2 * WINDOW),
	      2 * WINDOW, &big, std::chrono::seconds(2), nullptr));
}

// With osd_oob_pull off, the endpoint does not ask for reads, and the
// executor pulls nothing.
TEST(OSDOfi, PullsOnlyWhenAllowed)
{
  configure();
  g_ceph_context->_conf.set_val_or_die("osd_oob_pull", "false");
  g_ceph_context->_conf.apply_changes(nullptr);
  OSDOfi ofi(g_ceph_context, nullptr);
  if (ofi.init(false) != 0) {
    GTEST_SKIP() << "tcp is not available";
  }
  auto c = lend(WINDOW, ceph::ofi::Endpoint::remote_read);
  ASSERT_FALSE(c.token.empty());
  EXPECT_TRUE(ofi.handles(c.token));
  EXPECT_FALSE(ofi.pulls(c.token));
  ceph::buffer::list out;
  EXPECT_EQ(-EOPNOTSUPP, ofi.execute_pull(
	      "obj", c.token, ceph::osd::oob::linear_plan(0, 4096), 4096, &out,
	      std::chrono::seconds(2), nullptr));
}

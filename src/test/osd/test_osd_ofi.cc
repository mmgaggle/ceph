// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/osd_ofi.h"

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "global/global_context.h"

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

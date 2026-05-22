// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
//
// Unit tests for the PSP netlink mock backend.
//
// These tests exercise the NetlinkBackend interface contract using
// only the in-memory mock - no kernel, no hardware, no real netlink.
// The two-peer rendezvous test models the shape of the in-band PSP
// handshake without requiring the protocol code to exist yet.
// See doc/dev/msgr2-psp.rst.

#include "msg/async/PSPNetlink.h"

#include <gtest/gtest.h>

using namespace ceph::msgr::psp;

TEST(PSPNetlinkMock, GetDevCapsReportsSupported) {
  auto backend = make_mock_backend(MockConfig{});
  auto caps = backend->get_dev_caps(/*sock_fd*/ 42);
  ASSERT_TRUE(caps.has_value());
  EXPECT_TRUE(caps->psp_supported);
  EXPECT_EQ(caps->ifname, "mock0");
  EXPECT_EQ(caps->free_tx_assoc_slots, 64u);
  EXPECT_EQ(caps->free_rx_assoc_slots, 64u);
}

TEST(PSPNetlinkMock, GetDevCapsHonorsUnsupported) {
  MockConfig cfg;
  cfg.psp_supported = false;
  auto backend = make_mock_backend(cfg);
  auto caps = backend->get_dev_caps(42);
  ASSERT_TRUE(caps.has_value());
  EXPECT_FALSE(caps->psp_supported);
}

TEST(PSPNetlinkMock, GetDevCapsHardError) {
  MockConfig cfg;
  cfg.fail_get_dev_caps = true;
  auto backend = make_mock_backend(cfg);
  EXPECT_FALSE(backend->get_dev_caps(42).has_value());
}

// Models the in-band PSP handshake shape: each peer allocs its
// own rx-key locally, the wrapped blob round-trips over the
// (mocked) wire, and each peer installs the other's blob as its
// tx-key. After this, both peers consider the connection PSP-up.
TEST(PSPNetlinkMock, TwoPeerRendezvous) {
  auto peer_a = make_mock_backend(MockConfig{});
  auto peer_b = make_mock_backend(MockConfig{});

  auto blob_a = peer_a->alloc_rx_assoc(/*fd*/ 100);
  ASSERT_TRUE(blob_a.has_value());
  auto blob_b = peer_b->alloc_rx_assoc(/*fd*/ 200);
  ASSERT_TRUE(blob_b.has_value());

  EXPECT_EQ(0, peer_a->install_tx_assoc(100, *blob_b));
  EXPECT_EQ(0, peer_b->install_tx_assoc(200, *blob_a));

  auto caps_a = peer_a->get_dev_caps(0);
  ASSERT_TRUE(caps_a.has_value());
  EXPECT_EQ(caps_a->free_tx_assoc_slots, 64u - 1);
  EXPECT_EQ(caps_a->free_rx_assoc_slots, 64u - 1);
}

// Exercises the tear-down-on-attach-failure path: handshake completes
// (alloc succeeds) but the kernel-side install rejects.
TEST(PSPNetlinkMock, InjectedInstallFailure) {
  MockConfig cfg;
  cfg.fail_next_install_tx_assoc = 1;
  auto backend = make_mock_backend(cfg);

  auto blob = backend->alloc_rx_assoc(/*fd*/ 50);
  ASSERT_TRUE(blob.has_value());
  EXPECT_EQ(-ENOSPC, backend->install_tx_assoc(50, *blob));

  // Budget consumed; next attempt succeeds.
  auto blob2 = backend->alloc_rx_assoc(51);
  ASSERT_TRUE(blob2.has_value());
  EXPECT_EQ(0, backend->install_tx_assoc(51, *blob2));
}

TEST(PSPNetlinkMock, TxCapacityExhaustion) {
  MockConfig cfg;
  cfg.tx_capacity = 2;
  auto backend = make_mock_backend(cfg);

  for (int i = 0; i < 2; ++i) {
    auto blob = backend->alloc_rx_assoc(100 + i);
    ASSERT_TRUE(blob.has_value());
    EXPECT_EQ(0, backend->install_tx_assoc(100 + i, *blob));
  }
  auto blob3 = backend->alloc_rx_assoc(102);
  ASSERT_TRUE(blob3.has_value());
  EXPECT_EQ(-ENOSPC, backend->install_tx_assoc(102, *blob3));
}

TEST(PSPNetlinkMock, RejectsMalformedBlob) {
  auto backend = make_mock_backend(MockConfig{});
  std::vector<uint8_t> garbage(16, 0xff);
  EXPECT_EQ(-EINVAL, backend->install_tx_assoc(50, garbage));

  std::vector<uint8_t> wrong_size{'P', 'S', 'P', 'M'};
  EXPECT_EQ(-EINVAL, backend->install_tx_assoc(50, wrong_size));
}

TEST(PSPNetlinkMock, TeardownReleasesSlots) {
  auto backend = make_mock_backend(MockConfig{});
  auto blob = backend->alloc_rx_assoc(/*fd*/ 75);
  ASSERT_TRUE(blob.has_value());
  ASSERT_EQ(0, backend->install_tx_assoc(75, *blob));

  EXPECT_EQ(0, backend->teardown(75));

  auto caps = backend->get_dev_caps(0);
  ASSERT_TRUE(caps.has_value());
  EXPECT_EQ(caps->free_tx_assoc_slots, 64u);
  EXPECT_EQ(caps->free_rx_assoc_slots, 64u);
}

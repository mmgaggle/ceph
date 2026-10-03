// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rdma_fence.h"

#include <deque>

#include <gtest/gtest.h>

using R = librados::ObjectReadOperation;
using result = R::rdma_delivery_result;

namespace {
result with(uint32_t flags, uint64_t bytes = 0)
{
  result r;
  r.flags = flags;
  r.bytes = bytes;
  return r;
}
} // anonymous namespace

TEST(RgwRdmaFence, AllDeclined)
{
  // one OSD of the pool delivers nothing out of band: every stripe it
  // serves comes back declined, and the fallback need not wait
  std::deque<result> rs = {with(R::RDMA_DELIVERY_DECLINED),
                           with(R::RDMA_DELIVERY_DECLINED)};
  EXPECT_FALSE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, DeclinedAndLanded)
{
  // stripes delivered with every byte placed before the reply, next to
  // one declined: no write can land any more
  std::deque<result> rs = {
    with(R::RDMA_DELIVERY_LANDED | R::RDMA_DELIVERY_CRC64_VALID, 4 << 20),
    with(R::RDMA_DELIVERY_DECLINED),
    with(R::RDMA_DELIVERY_LANDED, 4 << 20)};
  EXPECT_FALSE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, StartedTransferNeedsTheFence)
{
  // a stripe an OSD started a transfer for, and then returned inline
  std::deque<result> rs = {with(R::RDMA_DELIVERY_DECLINED), with(0)};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, DeliveredWithoutThePromise)
{
  // delivered by an OSD whose transport does not promise that the bytes
  // landed before the reply, or by an older OSD
  std::deque<result> rs = {with(R::RDMA_DELIVERY_CRC64_VALID, 4 << 20),
                           with(R::RDMA_DELIVERY_DECLINED)};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, ResentNeedsTheFence)
{
  // the original attempt may have started a transfer on another OSD
  std::deque<result> rs = {
    with(R::RDMA_DELIVERY_DECLINED | R::RDMA_DELIVERY_RESENT)};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
  std::deque<result> landed = {
    with(R::RDMA_DELIVERY_LANDED | R::RDMA_DELIVERY_RESENT, 4096)};
  EXPECT_TRUE(rgw::rdma::fence_needed(landed));
}

TEST(RgwRdmaFence, NoResultNeedsTheFence)
{
  // a read that never came back, or one from an OSD that reports neither
  std::deque<result> rs = {with(R::RDMA_DELIVERY_DECLINED), result{}};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
}

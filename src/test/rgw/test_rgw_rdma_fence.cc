// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rdma_fence.h"

#include <deque>
#include <sstream>

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

TEST(RgwRdmaFence, WriteMayLand)
{
  using rgw::rdma::write_may_land;
  // a GET that failed: unless every read came back declined or landed,
  // and none was resent
  EXPECT_FALSE(write_may_land(false, false, false));
  EXPECT_TRUE(write_may_land(false, true, false));
  EXPECT_TRUE(write_may_land(false, true, true));
  // one that delivered everything: each OSD placed its stripe before it
  // replied, also one that does not say so, and only the earlier attempt
  // of a resent read can still write
  EXPECT_FALSE(write_may_land(true, false, false));
  EXPECT_FALSE(write_may_land(true, true, false));
  EXPECT_TRUE(write_may_land(true, true, true));
}

TEST(RgwRdmaFence, ResentRelayThatSucceeded)
{
  // the results of a relay that delivered every stripe, one of them on a
  // resend, read as RGWRados::Object::Read::iterate() reads them: its
  // window is held like a failed relay's. An OSD that pushes a resend
  // whose earlier attempts did not settle gives these; ours return it
  // inline, and the fallback waits out the fence instead
  std::deque<result> rs = {
    with(R::RDMA_DELIVERY_LANDED, 4 << 20),
    with(R::RDMA_DELIVERY_LANDED | R::RDMA_DELIVERY_RESENT, 4 << 20)};
  auto may_land = [&rs](bool success) {
    return rgw::rdma::write_may_land(success, rgw::rdma::fence_needed(rs),
                                     rgw::rdma::summarize(rs).resent > 0);
  };
  EXPECT_TRUE(may_land(true));
  EXPECT_TRUE(may_land(false));
  // without the resend every write settled: the window is free at once,
  // after a relay that failed past the reads as after one that did not
  rs.back() = with(R::RDMA_DELIVERY_LANDED, 4 << 20);
  EXPECT_FALSE(may_land(true));
  EXPECT_FALSE(may_land(false));
}

TEST(RgwRdmaFence, RelayOverPlacedStripes)
{
  // a relay over a transport that does not promise delivery-complete
  // writes (cuObject): every stripe placed, none of them landed
  std::deque<result> rs = {with(0, 4 << 20), with(0, 4 << 20)};
  const bool needed = rgw::rdma::fence_needed(rs);
  const bool resent = rgw::rdma::summarize(rs).resent > 0;
  EXPECT_TRUE(needed);
  // reads that failed: the window is held
  EXPECT_TRUE(rgw::rdma::write_may_land(false, needed, resent));
  // reads that delivered the range: not held, also when the writes to
  // the client failed after them (the client was busy, say), which no
  // OSD write depends on
  EXPECT_FALSE(rgw::rdma::write_may_land(true, needed, resent));
}

TEST(RgwRdmaFence, StripeReplies)
{
  using rgw::rdma::classify_stripe;
  using rgw::rdma::stripe_reply;
  EXPECT_EQ(stripe_reply::placed, classify_stripe(0, 0));
  // returned inline - declined, or a push cut off: the GET falls back
  EXPECT_EQ(stripe_reply::inline_data, classify_stripe(0, 4 << 20));
  // a failed read fails the GET even if data came with it
  EXPECT_EQ(stripe_reply::failed, classify_stripe(-ENOENT, 0));
  EXPECT_EQ(stripe_reply::failed, classify_stripe(-EIO, 4096));
}

TEST(RgwRdmaFence, Summary)
{
  std::deque<result> rs = {
    with(R::RDMA_DELIVERY_LANDED, 4 << 20),
    with(R::RDMA_DELIVERY_DECLINED),
    with(R::RDMA_DELIVERY_DECLINED | R::RDMA_DELIVERY_RESENT),
    with(0, 1 << 20),
    with(0)};
  const auto s = rgw::rdma::summarize(rs);
  EXPECT_EQ(5u, s.reads);
  EXPECT_EQ(2u, s.declined);
  EXPECT_EQ(1u, s.landed);
  EXPECT_EQ(2u, s.open);
  EXPECT_EQ(1u, s.resent);
  EXPECT_EQ(uint64_t(5 << 20), s.bytes);
  std::ostringstream o;
  o << s;
  EXPECT_EQ("5 reads (2 declined, 1 landed, 2 open, 1 resent), 5242880 "
            "bytes placed", o.str());
}

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * The client message throttle and the writes whose payload the primary
 * pulls out of client memory (OpRequest::reserve_pulled()): a request
 * reserves the bytes before the pull, only while the throttle has room
 * for them, and gives them back when it is done, with the ops' data
 * that holds them, whatever limit the throttle has by then.
 */

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "common/ceph_context.h"
#include "common/Throttle.h"
#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"

#include "gtest/gtest.h"

namespace {

struct OobPull : public ::testing::Test {
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_OSD), false};
  OpTracker tracker{cct.get(), false, 1};

  /// a client write, as the OSD's messenger hands it over
  OpRequestRef new_op() {
    spg_t pgid;
    return tracker.create_request<OpRequest, Message*>(
      new MOSDOp(0, 1, hobject_t(), pgid, 1, CEPH_OSD_FLAG_WRITE, 0));
  }
};

} // anonymous namespace

TEST_F(OobPull, HeldUntilTheRequestIsDone)
{
  Throttle t(cct.get(), "osd_client_bytes", 1000, false);
  auto op = new_op();
  ASSERT_TRUE(op->reserve_pulled(&t, 300));
  ASSERT_TRUE(op->reserve_pulled(&t, 200));
  EXPECT_EQ(500, t.get_current());
  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, NoRoomReservesNothing)
{
  // the requests the messenger has read hold most of the throttle: a
  // pull that does not fit is declined before it takes anything
  Throttle t(cct.get(), "osd_client_bytes", 1000, false);
  auto a = new_op();
  ASSERT_TRUE(a->reserve_pulled(&t, 900));
  auto b = new_op();
  EXPECT_FALSE(b->reserve_pulled(&t, 200));
  EXPECT_EQ(900, t.get_current());
  b.reset();
  EXPECT_EQ(900, t.get_current());

  // the room comes back with the request that held it
  a.reset();
  EXPECT_EQ(0, t.get_current());
  auto c = new_op();
  EXPECT_TRUE(c->reserve_pulled(&t, 200));
  EXPECT_EQ(200, t.get_current());
}

TEST_F(OobPull, NothingToCharge)
{
  // a peer whose policy has no byte throttle, and an empty payload
  Throttle t(cct.get(), "osd_client_bytes", 1000, false);
  auto op = new_op();
  EXPECT_TRUE(op->reserve_pulled(nullptr, 100));
  EXPECT_TRUE(op->reserve_pulled(&t, 0));
  EXPECT_EQ(0, t.get_current());
  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, LimitChangedWhileHeld)
{
  // osd_client_message_size_cap 0 means no limit; a limit set at run
  // time (OSD::handle_conf_change()) must get back what was taken
  // without one, or the throttle's count goes negative
  Throttle t(cct.get(), "osd_client_bytes", 0, false);
  auto op = new_op();
  ASSERT_TRUE(op->reserve_pulled(&t, 300));
  EXPECT_EQ(300, t.get_current());
  t.reset_max(1000);
  op.reset();
  EXPECT_EQ(0, t.get_current());

  // and the limit taken away
  op = new_op();
  ASSERT_TRUE(op->reserve_pulled(&t, 300));
  t.reset_max(0);
  op.reset();
  EXPECT_EQ(0, t.get_current());
}

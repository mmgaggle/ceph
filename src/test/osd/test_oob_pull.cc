// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * The writes whose payload the primary pulls out of client memory.
 *
 * The client message throttle (OpRequest::reserve_pulled()): a request
 * reserves the bytes before the pull, only while the throttle has room
 * for them, and gives them back when it is done, with the ops' data
 * that holds them, whatever limit the throttle has by then.
 *
 * The pull step (src/osd/oob_pull.h), through a fake executor: every
 * pull of a request is checked before any starts; the pulls are all or
 * nothing; a run of the request after they started pulls nothing and
 * charges the throttle no more; the checksums are what the code before
 * the pull set computed; and a reply that fails the request declines
 * only the ops whose pull started nothing, and none while the pulls
 * run. With transfer threads the pulls still run on the thread that
 * starts them.
 */

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/Clock.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/crc64nvme.h"
#include "common/perf_counters.h"
#include "common/rdma_token.h"
#include "common/Throttle.h"
#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"
#include "osd/oob_executor.h"
#include "osd/oob_placement.h"
#include "osd/oob_pull.h"
#include "osd/oob_xfer.h"

#include "gtest/gtest.h"

using namespace ceph::osd::oob;
using namespace std::chrono_literals;
using ceph::rdma::delivery_t;
using ceph::rdma::oob_result_t;

namespace {

/**
 * Pulls out of a client window of its own, or fails as the token says:
 * "ok" pulls, "started" fails after a read went out, "unstarted" fails
 * before, "big" is too large to stage, and "none" is a token it does
 * not pull from.
 */
struct fake_puller : OSDOobExecutor {
  std::string window = std::string(1 << 16, '\0');
  /// how long each pull takes
  std::chrono::milliseconds delay{0};

  /// runs in each pull, while it reads the window
  std::function<void()> during;

  std::mutex m;
  std::vector<std::string> calls;  ///< the tokens pulled from, in order
  std::vector<std::thread::id> ran_on;

  fake_puller() {
    for (size_t i = 0; i < window.size(); i++) {
      window[i] = static_cast<char>(i * 7 + i / 251);
    }
  }

  bool is_available() const override { return true; }
  bool handles(const std::string&) const override { return false; }
  bool pulls(const std::string& token) const override {
    return token != "none";
  }
  void dump_stats(ceph::Formatter*) const override {}

  ssize_t execute_plan(const std::string&, const std::string&,
		       const ceph::buffer::list&, const placement_plan&,
		       std::chrono::milliseconds, bool* started) override {
    ADD_FAILURE() << "a pull pushed";
    *started = false;
    return -EOPNOTSUPP;
  }

  ssize_t execute_pull(const std::string& key, const std::string& token,
		       const placement_plan& plan, uint64_t total,
		       ceph::buffer::list* out,
		       std::chrono::milliseconds budget,
		       bool* started) override {
    {
      std::lock_guard l(m);
      calls.push_back(token);
      ran_on.push_back(std::this_thread::get_id());
    }
    EXPECT_EQ("obj", key);
    EXPECT_GT(budget.count(), 0);
    if (delay.count() > 0) {
      std::this_thread::sleep_for(delay);
    }
    if (during) {
      during();
    }
    if (token == "unstarted") {
      *started = false;
      return -ETIMEDOUT;
    }
    if (token == "big") {
      *started = false;
      return -E2BIG;
    }
    *started = true;
    if (token == "started") {
      return -EIO;
    }
    std::string buf(total, '\0');
    for (const auto& t : plan) {
      ceph_assert(t.client_ofs + t.len <= window.size());
      ceph_assert(t.local_ofs + t.len <= total);
      memcpy(buf.data() + t.local_ofs, window.data() + t.client_ofs, t.len);
    }
    out->append(buf);
    return total;
  }
};

/// one op of a request
struct op_spec {
  uint16_t type = CEPH_OSD_OP_WRITEFULL;
  uint64_t len = 0;
  std::string token;  ///< empty: the op carries no descriptor
  uint32_t flags = delivery_t::FLAG_PULL;
  uint64_t base = 0;
  std::string data;   ///< inline data
};

/**
 * What the pull of one op reported before the pull set, from
 * PrimaryLogPG::pull_oob_payloads() as of 5a486930422: what the job must
 * still report.
 */
oob_result_t legacy_result(const ceph::buffer::list& bl,
			   const delivery_t& d, uint64_t len)
{
  oob_result_t res;
  res.bytes = len;
  const bool want64 = d.flags & ceph::rdma::delivery_t::FLAG_CRC64NVME;
  const bool want32c = d.flags & ceph::rdma::delivery_t::FLAG_CRC32C;
  if (want64 || want32c) {
    if (want64) {
      res.crc64 = ceph::crc64nvme(bl);
    }
    if (want32c) {
      res.crc32c = ceph::rdma::crc32c_canonical(bl);
    }
    res.ranges.push_back({d.base_offset, len, res.crc64, res.crc32c});
    res.flags |= ceph::rdma::oob_result_t::FLAG_COMBINABLE |
      ceph::rdma::oob_result_t::FLAG_RANGES |
      (want64 ? ceph::rdma::oob_result_t::FLAG_CRC64NVME : 0) |
      (want32c ? ceph::rdma::oob_result_t::FLAG_CRC32C : 0);
  }
  return res;
}

void expect_same(const oob_result_t& want, const oob_result_t& got)
{
  EXPECT_EQ(want.bytes, got.bytes);
  EXPECT_EQ(want.flags, got.flags);
  EXPECT_EQ(want.crc64, got.crc64);
  EXPECT_EQ(want.crc32c, got.crc32c);
  EXPECT_EQ(want.ranges, got.ranges);
}

struct OobPull : public ::testing::Test {
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_OSD), false};
  OpTracker tracker{cct.get(), false, 1};

  ~OobPull() override {
    // join the tracker's history thread, which would outlive the fixture
    tracker.on_shutdown();
  }

  /// a client write, as the OSD's messenger hands it over
  OpRequestRef new_op() {
    spg_t pgid;
    return tracker.create_request<OpRequest, Message*>(
      new MOSDOp(0, 1, hobject_t(), pgid, 1, CEPH_OSD_FLAG_WRITE, 0));
  }

  /// a client write of the ops specs says to object "obj", received
  /// at recv
  OpRequestRef new_op(const std::vector<op_spec>& specs,
		      utime_t recv = ceph_clock_now()) {
    spg_t pgid;
    const hobject_t oid(object_t("obj"), "", CEPH_NOSNAP, 0, 1, "");
    auto m = new MOSDOp(0, 1, oid, pgid, 1, CEPH_OSD_FLAG_WRITE, 0);
    std::vector<delivery_t> deliveries;
    for (const auto& s : specs) {
      OSDOp op;
      op.op.op = s.type;
      op.op.extent.offset = 0;
      op.op.extent.length = s.len;
      op.indata.append(s.data);
      m->ops.push_back(op);
      delivery_t d;
      d.token = s.token;
      if (!s.token.empty()) {
	d.base_offset = s.base;
	d.flags = s.flags;
      }
      deliveries.push_back(d);
    }
    m->set_rdma_deliveries(std::move(deliveries));
    m->set_recv_stamp(recv);
    return tracker.create_request<OpRequest, Message*>(m);
  }

  /// the request's ops, which the pull step fills and the request runs
  static std::vector<OSDOp>& ops_of(const OpRequestRef& op) {
    return static_cast<MOSDOp*>(op->get_nonconst_req())->ops;
  }

  /// what the PG hands the pull step
  pull_env env(Transfers* x, fake_puller* e, Throttle* t) {
    pull_env env;
    env.cct = cct.get();
    env.xfer = x;
    env.puller_for = [e](const std::string& token) -> OSDOobExecutor* {
      return e->pulls(token) ? e : nullptr;
    };
    env.client_throttle = [t] { return t; };
    env.max_write = 90 << 20;
    env.lease = 6;
    env.drain = 4;
    return env;
  }

  /// a transfer service with threads transfer threads
  std::unique_ptr<Transfers> service(unsigned threads) {
    auto x = std::make_unique<Transfers>(cct.get(), threads, 16, 1 << 30,
					 30s, [] {});
    x->start();
    return x;
  }

  /// the oob results of a reply that fails the request, as
  /// OSDService::reply_op_error() and PrimaryLogPG::complete_read_ctx()
  /// build them
  static std::vector<oob_result_t> error_results(const OpRequestRef& op) {
    const auto& deliveries = op->get_req<MOSDOp>()->get_rdma_deliveries();
    std::vector<oob_result_t> oob(deliveries.size());
    for (size_t i = 0; i < deliveries.size(); i++) {
      if (!deliveries[i].empty()) {
	oob[i].flags |= oob_result_t::FLAG_DECLINED;
      }
    }
    if (op->oob_pull) {
      op->oob_pull->clear_declined(oob);
    }
    return oob;
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

TEST_F(OobPull, PullsEveryPayload)
{
  // with transfer threads too, the pulls run on the thread that starts
  // them, and the request goes on with its payloads
  for (unsigned threads : {0u, 2u}) {
    SCOPED_TRACE(threads);
    auto x = service(threads);
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
    const std::vector<op_spec> specs = {
      {CEPH_OSD_OP_WRITEFULL, 4096, "ok",
       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0},
      {CEPH_OSD_OP_CREATE, 0, ""},
      {CEPH_OSD_OP_WRITE, 5000, "ok",
       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME |
       delivery_t::FLAG_CRC32C, 8192},
      {CEPH_OSD_OP_WRITE, 100, "ok", delivery_t::FLAG_PULL, 20000},
      {CEPH_OSD_OP_WRITE, 3000, "ok",
       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC32C, 30001},
      // nothing to pull
      {CEPH_OSD_OP_WRITE, 0, "ok", delivery_t::FLAG_PULL, 0},
    };
    auto op = new_op(specs);
    auto& ops = ops_of(op);
    ASSERT_EQ(0, pull_payloads(env(x.get(), &e, &t), *op, ops));

    EXPECT_EQ(std::vector<std::string>(4, "ok"), e.calls);
    for (const auto& id : e.ran_on) {
      EXPECT_EQ(std::this_thread::get_id(), id);
    }
    EXPECT_EQ(4096 + 5000 + 100 + 3000, t.get_current());
    ASSERT_TRUE(op->oob_pull);
    EXPECT_TRUE(op->oob_pull->consumed());
    ASSERT_EQ(specs.size(), op->oob_pulled.size());
    const auto& deliveries = op->get_req<MOSDOp>()->get_rdma_deliveries();
    for (size_t i = 0; i < specs.size(); i++) {
      SCOPED_TRACE(i);
      const auto& s = specs[i];
      if (s.token.empty() || s.len == 0) {
	EXPECT_EQ(0u, ops[i].indata.length());
	expect_same(oob_result_t{}, op->oob_pulled[i]);
	continue;
      }
      EXPECT_EQ(e.window.substr(s.base, s.len), ops[i].indata.to_str());
      expect_same(legacy_result(ops[i].indata, deliveries[i], s.len),
		  op->oob_pulled[i]);
    }
    // one job, for all the pulls of the request
    EXPECT_EQ(1u, x->get_logger()->get_tavg_ns(l_oob_xfer_pull_lat).second);
    EXPECT_EQ(0u, x->get_logger()->get_tavg_ns(l_oob_xfer_push_lat).second);
    EXPECT_EQ(0u, x->get_logger()->get(l_oob_xfer_running));

    // the bytes go back when the request is done
    op.reset();
    EXPECT_EQ(0, t.get_current());
  }
}

TEST_F(OobPull, AllOrNothing)
{
  auto x = service(0);
  struct case_t {
    std::vector<std::string> tokens;
    int r;
    std::vector<std::string> calls;  ///< the pulls that ran, in order
  };
  const std::vector<case_t> cases = {
    // the second fails after its read went out: the third never starts
    {{"ok", "started", "ok"}, -EIO, {"ok", "started"}},
    // the first fails before anything went out
    {{"unstarted", "ok"}, -ETIMEDOUT, {"unstarted"}},
    // a payload the transport cannot stage is one it cannot pull
    {{"ok", "big", "ok"}, -EOPNOTSUPP, {"ok", "big"}},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.calls.back());
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
    std::vector<op_spec> specs;
    for (size_t i = 0; i < c.tokens.size(); i++) {
      specs.push_back({CEPH_OSD_OP_WRITE, 4096, c.tokens[i],
		       delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME,
		       i * 4096});
    }
    auto op = new_op(specs);
    auto& ops = ops_of(op);
    EXPECT_EQ(c.r, pull_payloads(env(x.get(), &e, &t), *op, ops));
    EXPECT_EQ(c.calls, e.calls);
    // no op has its payload, and no result is reported: the request
    // fails, and nothing of it is written
    for (size_t i = 0; i < ops.size(); i++) {
      EXPECT_EQ(0u, ops[i].indata.length());
      expect_same(oob_result_t{}, op->oob_pulled[i]);
    }
    ASSERT_TRUE(op->oob_pull);
    EXPECT_TRUE(op->oob_pull->settled());
    EXPECT_FALSE(op->oob_pull->consumed());
    // charged all the same, until the request is done
    EXPECT_EQ(int64_t(4096 * c.tokens.size()), t.get_current());
    op.reset();
    EXPECT_EQ(0, t.get_current());
  }
}

TEST_F(OobPull, ChargedOnceOverTwoRuns)
{
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0}});
  auto& ops = ops_of(op);
  const auto env1 = env(x.get(), &e, &t);
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(4096, t.get_current());
  const auto pulled = op->oob_pulled[0];
  EXPECT_EQ(4096u, pulled.bytes);

  // The request waits and runs again, after its first run changed the
  // op: a write that a truncation overtook is cut short, its data too
  // (PrimaryLogPG::do_osd_ops(), CEPH_OSD_OP_WRITE). The op's data can
  // then no longer tell that the payload was pulled; the set can.
  ops[0].op.extent.length = 1000;
  {
    ceph::buffer::list cut;
    cut.substr_of(ops[0].indata, 0, 1000);
    ops[0].indata = std::move(cut);
  }
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(4096, t.get_current());
  EXPECT_EQ(e.window.substr(0, 1000), ops[0].indata.to_str());
  expect_same(pulled, op->oob_pulled[0]);

  // and once more, unchanged
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(4096, t.get_current());

  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, InvalidSecondPullStartsNothing)
{
  auto x = service(0);
  const op_spec good = {CEPH_OSD_OP_WRITE, 4096, "ok",
			delivery_t::FLAG_PULL, 0};
  struct case_t {
    const char* what;
    op_spec second;
    int r;
  };
  const std::vector<case_t> cases = {
    {"a pull descriptor on a read",
     {CEPH_OSD_OP_READ, 4096, "ok", delivery_t::FLAG_PULL, 4096},
     -EINVAL},
    {"a flag a pull does not know",
     {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL | (1u << 30),
      4096},
     -EOPNOTSUPP},
    {"a write that carries its data",
     {CEPH_OSD_OP_WRITE, 4, "ok", delivery_t::FLAG_PULL, 4096, "data"},
     -EINVAL},
    {"a payload over osd_max_write_size",
     {CEPH_OSD_OP_WRITE, (90ull << 20) + 1, "ok", delivery_t::FLAG_PULL,
      4096},
     -EFBIG},
    {"a token no transport pulls from",
     {CEPH_OSD_OP_WRITE, 4096, "none", delivery_t::FLAG_PULL, 4096},
     -EOPNOTSUPP},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.what);
    fake_puller e;
    Throttle t(cct.get(), "osd_client_bytes", 1 << 30, false);
    auto op = new_op({good, c.second});
    auto& ops = ops_of(op);
    EXPECT_EQ(c.r, pull_payloads(env(x.get(), &e, &t), *op, ops));
    // the first pull, which was fine, did not start either
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(0, t.get_current());
    EXPECT_FALSE(op->oob_pull);
    EXPECT_EQ(0u, ops[0].indata.length());
    // and nothing of the request read the window
    for (const auto& r : error_results(op)) {
      EXPECT_EQ(oob_result_t::FLAG_DECLINED, r.flags);
    }
  }

  // a request whose client gave it up starts nothing either
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 30, false);
  auto op = new_op({good, good}, ceph_clock_now() - utime_t(10, 1000));
  EXPECT_EQ(-ETIMEDOUT, pull_payloads(env(x.get(), &e, &t), *op,
				      ops_of(op)));
  EXPECT_TRUE(e.calls.empty());
  EXPECT_EQ(0, t.get_current());
  EXPECT_FALSE(op->oob_pull);
}

TEST_F(OobPull, NoRoomStartsNothing)
{
  // what the messenger has read holds some of the throttle: each payload
  // would fit in the rest, but not both. The request takes the room for
  // all of them, or none
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 10000, false);
  auto held = new_op();
  ASSERT_TRUE(held->reserve_pulled(&t, 3000));
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0},
		    {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL,
		     4096}});
  EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(x.get(), &e, &t), *op,
				       ops_of(op)));
  EXPECT_TRUE(e.calls.empty());
  EXPECT_EQ(3000, t.get_current());
  EXPECT_FALSE(op->oob_pull);
  for (const auto& r : error_results(op)) {
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, r.flags);
  }

  // the payloads of one request are charged at once, as an inline
  // request's data is: a throttle at or under its limit takes more than
  // the limit then, as the messenger takes such a message
  Throttle small(cct.get(), "osd_client_bytes", 6000, false);
  auto small_held = new_op();
  ASSERT_TRUE(small_held->reserve_pulled(&small, 1000));
  auto big = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0},
		     {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL,
		      4096}});
  ASSERT_EQ(0, pull_payloads(env(x.get(), &e, &small), *big, ops_of(big)));
  EXPECT_EQ(2u, e.calls.size());
  EXPECT_EQ(1000 + 8192, small.get_current());
  big.reset();
  EXPECT_EQ(1000, small.get_current());
  small_held.reset();
  EXPECT_EQ(0, small.get_current());
}

TEST_F(OobPull, DeclinedOnlyWhenNothingStarted)
{
  auto x = service(0);
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  const uint32_t declined = oob_result_t::FLAG_DECLINED;

  {
    SCOPED_TRACE("a pull failed after its read went out");
    fake_puller e;
    // the first pulled, the second failed, the third never started;
    // the read's descriptor started no push
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0},
       {CEPH_OSD_OP_WRITE, 4096, "started", delivery_t::FLAG_PULL, 4096},
       {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 8192},
       {CEPH_OSD_OP_READ, 4096, "ok", 0, 0}});
    EXPECT_EQ(-EIO, pull_payloads(env(x.get(), &e, &t), *op, ops_of(op)));
    const auto oob = error_results(op);
    EXPECT_EQ(0u, oob[0].flags);
    EXPECT_EQ(0u, oob[1].flags);
    EXPECT_EQ(declined, oob[2].flags);
    EXPECT_EQ(declined, oob[3].flags);
  }
  {
    SCOPED_TRACE("a pull failed before its read went out");
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "unstarted", delivery_t::FLAG_PULL, 0},
       {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 4096}});
    EXPECT_EQ(-ETIMEDOUT, pull_payloads(env(x.get(), &e, &t), *op,
					ops_of(op)));
    for (const auto& r : error_results(op)) {
      EXPECT_EQ(declined, r.flags);
    }
  }
  {
    SCOPED_TRACE("the second pull could no longer land in time");
    // the first pull takes longer than the request has left: received
    // now, it must land within 1.5 s
    fake_puller e;
    e.delay = 1600ms;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0},
       {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 4096}});
    auto env1 = env(x.get(), &e, &t);
    env1.lease = 1;
    env1.drain = 0.5;
    EXPECT_EQ(-ETIMEDOUT, pull_payloads(env1, *op, ops_of(op)));
    EXPECT_EQ(1u, e.calls.size());
    const auto oob = error_results(op);
    EXPECT_EQ(0u, oob[0].flags);
    EXPECT_EQ(declined, oob[1].flags);
  }
  {
    SCOPED_TRACE("the job could no longer start in time");
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    auto env1 = env(x.get(), &e, &t);
    env1.lease = 1;
    env1.drain = 0.5;
    // the service waits this long before it runs the job
    cct->_conf.set_val_or_die("osd_debug_oob_delay_post_ms", "1600");
    EXPECT_EQ(-ETIMEDOUT, pull_payloads(env1, *op, ops_of(op)));
    cct->_conf.set_val_or_die("osd_debug_oob_delay_post_ms", "0");
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(declined, error_results(op)[0].flags);
    EXPECT_EQ(1u, x->get_logger()->get(l_oob_xfer_expired));
  }
  {
    SCOPED_TRACE("no transfer service");
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(nullptr, &e, &t), *op,
					 ops_of(op)));
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(declined, error_results(op)[0].flags);
  }
  {
    SCOPED_TRACE("a stopped transfer service");
    auto stopped = service(0);
    stopped->stop();
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(stopped.get(), &e, &t), *op,
					 ops_of(op)));
    EXPECT_TRUE(e.calls.empty());
    EXPECT_EQ(declined, error_results(op)[0].flags);
  }
  {
    SCOPED_TRACE("the write failed after its pull");
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *op, ops_of(op)));
    EXPECT_EQ(0u, error_results(op)[0].flags);
  }
  {
    SCOPED_TRACE("a reply while the pull reads the window");
    // the set is in flight: the job has not told what started yet
    fake_puller e;
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "unstarted", delivery_t::FLAG_PULL, 0},
       {CEPH_OSD_OP_READ, 4096, "ok", 0, 0}});
    std::vector<oob_result_t> oob;
    e.during = [&] { oob = error_results(op); };
    EXPECT_EQ(-ETIMEDOUT, pull_payloads(env(x.get(), &e, &t), *op,
					ops_of(op)));
    ASSERT_EQ(2u, oob.size());
    EXPECT_EQ(0u, oob[0].flags);
    EXPECT_EQ(declined, oob[1].flags);
    // once it settled, it has: nothing went out
    for (const auto& r : error_results(op)) {
      EXPECT_EQ(declined, r.flags);
    }
  }
}

TEST_F(OobPull, AJobThatNeverRunsStartsNothing)
{
  fake_puller e;
  const auto now = ceph::mono_clock::now();
  const bounds_t b{now + 10s, now + 10s};
  auto pulls = [&] {
    pull_op p;
    p.idx = 1;
    p.len = 4096;
    p.exec = &e;
    p.token = "ok";
    return std::vector<pull_op>{p};
  };
  for (int r : {-ETIMEDOUT, -ESHUTDOWN, -EBUSY, 0}) {
    SCOPED_TRACE(r);
    auto set = std::make_shared<pull_set>(std::vector<size_t>{1});
    {
      pull_job j(cct.get(), set, "obj", pulls(), b, true);
      EXPECT_TRUE(j.pulls());
      EXPECT_TRUE(j.inline_only());
      EXPECT_EQ(4096u, j.pinned_bytes());
      EXPECT_FALSE(set->settled());
      // until the job ends, it may yet start
      std::vector<oob_result_t> oob(2);
      oob[1].flags = oob_result_t::FLAG_DECLINED;
      set->clear_declined(oob);
      EXPECT_EQ(0u, oob[1].flags);
      if (r) {
	// expired in the queue, shed at stop, or refused
	j.expire(r);
      }
      // else dropped without running
    }
    ASSERT_TRUE(set->settled());
    std::vector<OSDOp> ops(2);
    std::vector<oob_result_t> pulled(2);
    EXPECT_EQ(r == -ETIMEDOUT ? -ETIMEDOUT : -EOPNOTSUPP,
	      set->consume(ops, pulled));
    EXPECT_EQ(0u, ops[1].indata.length());
    std::vector<oob_result_t> oob(2);
    oob[1].flags = oob_result_t::FLAG_DECLINED;
    set->clear_declined(oob);
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, oob[1].flags);
  }
  EXPECT_TRUE(e.calls.empty());
}

TEST_F(OobPull, NothingToPull)
{
  auto x = service(0);
  fake_puller e;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  {
    // a descriptor vector that does not mirror the ops: the write fails
    // later, for its missing data
    auto op = new_op(
      {{CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}});
    auto& ops = ops_of(op);
    ops.push_back(OSDOp());
    EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *op, ops));
    EXPECT_FALSE(op->oob_pull);
  }
  {
    // descriptors, but none of them a pull
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4, "", 0, 0, "data"},
		      {CEPH_OSD_OP_READ, 4096, "ok", 0, 0}});
    EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *op, ops_of(op)));
    EXPECT_FALSE(op->oob_pull);
    EXPECT_EQ(2u, op->oob_pulled.size());
  }
  EXPECT_TRUE(e.calls.empty());
  EXPECT_EQ(0, t.get_current());
}

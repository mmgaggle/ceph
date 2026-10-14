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
 * run.
 *
 * With transfer threads the pulls run there, and the request parks: the
 * set's settle kicks the PG where it waits exactly once, and a set that
 * settles before the request parks is consumed by the thread that
 * would have parked it, which then kicks nothing. A coroutine's request,
 * and one whose executor's transfers stay on the op threads, still pull
 * on the thread that starts them. A pull of a set the PG abandoned does
 * not start. Pulls on a transfer thread are out for every PG of the OSD
 * until they settled, and a later attempt of their request waits for
 * them; pulls that run where they start never are.
 */

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
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

/// the kicks of the PGs where requests wait for their pulls
struct kicks_t {
  std::mutex m;
  std::condition_variable cond;
  std::vector<kick_target> kicked;
  std::vector<std::thread::id> from;

  kick_fn fn() {
    return [this](const kick_target& t) {
      std::lock_guard l(m);
      kicked.push_back(t);
      from.push_back(std::this_thread::get_id());
      cond.notify_all();
    };
  }
  size_t count() {
    std::lock_guard l(m);
    return kicked.size();
  }
  /// wait until there are n kicks, for at most 10 s
  bool wait_for(size_t n) {
    std::unique_lock l(m);
    return cond.wait_for(l, 10s, [&] { return kicked.size() >= n; });
  }
};

/// the PG and object a test request waits at
kick_target test_target(unsigned seed = 1)
{
  kick_target t;
  t.pgid = spg_t(pg_t(seed, 3));
  t.head = hobject_t(object_t("obj"), "", CEPH_NOSNAP, seed, 3, "");
  t.epoch = 10 + seed;
  return t;
}

bool same_target(const kick_target& a, const kick_target& b)
{
  return a.pgid == b.pgid && a.head == b.head && a.epoch == b.epoch;
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
  /// at recv; its attempts share tid
  OpRequestRef new_op(const std::vector<op_spec>& specs,
		      utime_t recv = ceph_clock_now(), ceph_tid_t tid = 1) {
    spg_t pgid;
    const hobject_t oid(object_t("obj"), "", CEPH_NOSNAP, 0, 1, "");
    auto m = new MOSDOp(0, tid, oid, pgid, 1, CEPH_OSD_FLAG_WRITE, 0);
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
    env.target = test_target();
    env.kick = kicks.fn();
    return env;
  }

  kicks_t kicks;

  /**
   * The pull step of a request, as the PG runs it: when it parks, the
   * PG runs it again once the settle kicked it. Returns what the run
   * that went on returned; *parked says whether one waited.
   */
  int run_request(const pull_env& env, const OpRequestRef& op,
		  bool* parked = nullptr) {
    auto& ops = ops_of(op);
    const size_t before = kicks.count();
    int r = pull_payloads(env, *op, ops);
    if (parked) {
      *parked = r == PULLS_PARKED;
    }
    if (r != PULLS_PARKED) {
      return r;
    }
    EXPECT_TRUE(kicks.wait_for(before + 1));
    EXPECT_TRUE(op->oob_pull->settled());
    r = pull_payloads(env, *op, ops);
    EXPECT_NE(PULLS_PARKED, r);
    return r;
  }

  /// wait until x runs no job, for at most 10 s
  static void wait_until_idle(Transfers& x) {
    for (int i = 0; i < 1000 && (x.get_logger()->get(l_oob_xfer_running) ||
				 x.get_logger()->get(l_oob_xfer_queued)); i++) {
      std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(0u, x.get_logger()->get(l_oob_xfer_running));
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
  // with transfer threads the pulls run there, the request waits for
  // them, and its next run goes on with its payloads
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
    bool parked = false;
    ASSERT_EQ(0, run_request(env(x.get(), &e, &t), op, &parked));
    EXPECT_EQ(threads > 0, parked);

    EXPECT_EQ(std::vector<std::string>(4, "ok"), e.calls);
    for (const auto& id : e.ran_on) {
      EXPECT_EQ(threads == 0, std::this_thread::get_id() == id);
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
    // one job, for all the pulls of the request; a transfer thread
    // counts it once the job returned, after the kick
    wait_until_idle(*x);
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

TEST_F(OobPull, ParksWithTransferThreads)
{
  // the job runs on a transfer thread, the request waits for it holding
  // nothing, and the settle kicks the PG where it waits, once
  auto x = service(2);
  fake_puller e;
  e.delay = 200ms;
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  auto op = new_op(
    {{CEPH_OSD_OP_WRITE, 4096, "ok",
      delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME, 0},
     {CEPH_OSD_OP_READ, 4096, "ok", 0, 0}});
  auto& ops = ops_of(op);
  const auto env1 = env(x.get(), &e, &t);
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env1, *op, ops));
  ASSERT_TRUE(op->oob_pull);
  // charged at once, and nothing in the op until a run consumes the set
  EXPECT_EQ(4096, t.get_current());
  EXPECT_EQ(0u, ops[0].indata.length());
  // while the pull runs, a reply that fails the request declines none of
  // its pulled ops: any may be reading its window
  {
    const auto oob = error_results(op);
    EXPECT_EQ(0u, oob[0].flags);
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, oob[1].flags);
  }

  ASSERT_TRUE(kicks.wait_for(1));
  ASSERT_TRUE(op->oob_pull->settled());
  {
    std::lock_guard l(kicks.m);
    EXPECT_TRUE(same_target(env1.target, kicks.kicked[0]));
    // from the transfer thread that settled the set
    ASSERT_EQ(1u, e.ran_on.size());
    EXPECT_EQ(e.ran_on[0], kicks.from[0]);
    EXPECT_NE(std::this_thread::get_id(), kicks.from[0]);
  }

  // the run the kick starts consumes the set; no later run pulls, parks
  // or is kicked
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  EXPECT_EQ(e.window.substr(0, 4096), ops[0].indata.to_str());
  EXPECT_EQ(4096u, op->oob_pulled[0].bytes);
  EXPECT_TRUE(op->oob_pulled[0].flags & oob_result_t::FLAG_CRC64NVME);
  ASSERT_EQ(0, pull_payloads(env1, *op, ops));
  wait_until_idle(*x);
  EXPECT_EQ(1u, kicks.count());
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(4096, t.get_current());
  op.reset();
  EXPECT_EQ(0, t.get_current());
}

TEST_F(OobPull, FailedPullParksToo)
{
  // a pull that fails on the transfer thread fails the request on the
  // run the kick starts, after the set settled; it reports what started
  auto x = service(2);
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  for (const std::string token : {"started", "unstarted"}) {
    SCOPED_TRACE(token);
    fake_puller e;
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, token,
		       delivery_t::FLAG_PULL, 0}});
    bool parked = false;
    EXPECT_EQ(token == "started" ? -EIO : -ETIMEDOUT,
	      run_request(env(x.get(), &e, &t), op, &parked));
    EXPECT_TRUE(parked);
    EXPECT_EQ(token == "started" ? 0u : oob_result_t::FLAG_DECLINED,
	      error_results(op)[0].flags);
  }
  wait_until_idle(*x);
  EXPECT_EQ(2u, kicks.count());
}

TEST_F(OobPull, StaysOnThisThread)
{
  // a coroutine's request does not park, nor one whose executor's
  // transfers stay on the op threads: their pulls run here, and nothing
  // kicks
  auto x = service(2);
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  for (int c = 0; c < 2; c++) {
    SCOPED_TRACE(c);
    fake_puller e;
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}});
    auto env1 = env(x.get(), &e, &t);
    if (c == 0) {
      env1.here = true;
    } else {
      env1.runs_inline = [&e](const OSDOobExecutor* ex) { return ex == &e; };
    }
    ASSERT_EQ(0, pull_payloads(env1, *op, ops_of(op)));
    ASSERT_EQ(1u, e.ran_on.size());
    EXPECT_EQ(std::this_thread::get_id(), e.ran_on[0]);
    EXPECT_TRUE(op->oob_pull->consumed());
    EXPECT_EQ(e.window.substr(0, 4096), ops_of(op)[0].indata.to_str());
  }
  EXPECT_EQ(0u, kicks.count());
}

TEST_F(OobPull, KicksOncePerSettledSet)
{
  pull_result ok;
  ok.idx = 0;
  ok.r = 0;
  ok.started = true;
  ok.payload.append("data");
  const auto t1 = test_target(1);
  const auto t2 = test_target(2);
  {
    SCOPED_TRACE("parked, then settled");
    pull_set set({0});
    EXPECT_TRUE(set.park(t1));
    auto k = set.settle({ok});
    ASSERT_TRUE(k);
    EXPECT_TRUE(same_target(t1, *k));
    // a run after the settle goes on
    EXPECT_FALSE(set.park(t2));
  }
  {
    SCOPED_TRACE("parked again after an interval change");
    // the kick goes where the request waits last, once
    pull_set set({0});
    EXPECT_TRUE(set.park(t1));
    EXPECT_TRUE(set.park(t2));
    auto k = set.settle({ok});
    ASSERT_TRUE(k);
    EXPECT_TRUE(same_target(t2, *k));
  }
  {
    SCOPED_TRACE("settled before the park");
    // the thread that would have parked the request consumes the set,
    // and no kick comes
    pull_set set({0});
    EXPECT_FALSE(set.settle({ok}));
    EXPECT_FALSE(set.park(t1));
    std::vector<OSDOp> ops(1);
    std::vector<oob_result_t> pulled(1);
    EXPECT_EQ(0, set.consume(ops, pulled));
    EXPECT_EQ("data", ops[0].indata.to_str());
    EXPECT_TRUE(set.consumed());
  }
  {
    SCOPED_TRACE("abandoned");
    // the PG dropped the request: nothing to kick
    pull_set set({0});
    EXPECT_TRUE(set.park(t1));
    set.abandon();
    EXPECT_TRUE(set.abandoned());
    EXPECT_FALSE(set.settle({ok}));
  }
}

TEST_F(OobPull, SettleRacesPark)
{
  // a set settles on its transfer thread while an op thread parks the
  // request: either the park comes first, and exactly one kick comes, or
  // the settle does, and the op thread goes on with the set
  const auto t1 = test_target();
  unsigned parked = 0, consumed = 0;
  for (int i = 0; i < 20000; i++) {
    pull_set set({0});
    std::atomic<int> ready{0};
    std::optional<kick_target> k;
    std::thread settler([&] {
      pull_result ok;
      ok.idx = 0;
      ok.r = 0;
      ok.payload.append("data");
      ready++;
      while (ready.load() < 2) {}
      k = set.settle({ok});
    });
    ready++;
    while (ready.load() < 2) {}
    const bool waits = set.park(t1);
    settler.join();
    ASSERT_TRUE(set.settled());
    if (waits) {
      ASSERT_TRUE(k);
      EXPECT_TRUE(same_target(t1, *k));
      parked++;
    } else {
      ASSERT_FALSE(k);
      std::vector<OSDOp> ops(1);
      std::vector<oob_result_t> pulled(1);
      ASSERT_EQ(0, set.consume(ops, pulled));
      consumed++;
    }
  }
  std::cout << "parked first " << parked << ", settled first " << consumed
	    << std::endl;
}

TEST_F(OobPull, SettledBeforeParkIsConsumed)
{
  // A job the service refuses settles before the request can park: the
  // thread that pulls consumes the set, the request fails as a refused
  // pull always did, and nothing kicks
  auto x = std::make_unique<Transfers>(cct.get(), 1, 1, 1 << 30, 30s,
				       [] {});
  x->start();
  fake_puller e;
  std::mutex gate_m;
  std::condition_variable gate_cond;
  bool open = false;
  e.during = [&] {
    std::unique_lock l(gate_m);
    gate_cond.wait(l, [&] { return open; });
  };
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  // the one job the service takes, held
  auto held = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}});
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env(x.get(), &e, &t), *held,
					ops_of(held)));
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL, 4096}});
  EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env(x.get(), &e, &t), *op,
				       ops_of(op)));
  EXPECT_TRUE(op->oob_pull->settled());
  EXPECT_EQ(oob_result_t::FLAG_DECLINED, error_results(op)[0].flags);
  EXPECT_EQ(1u, x->get_logger()->get(l_oob_xfer_refused));
  {
    std::lock_guard l(gate_m);
    open = true;
  }
  gate_cond.notify_all();
  ASSERT_TRUE(kicks.wait_for(1));
  wait_until_idle(*x);
  // only the held request's set kicked
  EXPECT_EQ(1u, kicks.count());
  EXPECT_EQ(0, pull_payloads(env(x.get(), &e, &t), *held, ops_of(held)));

  // And a set that a transfer thread settles before the op thread parks
  // the request, which a fast pull can do: each request either waits
  // and is kicked once, or goes on with its payloads at once
  auto y = service(4);
  unsigned waited = 0, went_on = 0;
  for (int i = 0; i < 2000; i++) {
    fake_puller f;
    auto r = new_op({{CEPH_OSD_OP_WRITE, 512, "ok",
		      delivery_t::FLAG_PULL, 0}});
    const size_t before = kicks.count();
    const int ret = pull_payloads(env(y.get(), &f, &t), *r, ops_of(r));
    if (ret == PULLS_PARKED) {
      ASSERT_TRUE(kicks.wait_for(before + 1));
      ASSERT_EQ(0, pull_payloads(env(y.get(), &f, &t), *r, ops_of(r)));
      waited++;
    } else {
      ASSERT_EQ(0, ret);
      ASSERT_TRUE(r->oob_pull->consumed());
      went_on++;
    }
    EXPECT_EQ(f.window.substr(0, 512), ops_of(r)[0].indata.to_str());
  }
  wait_until_idle(*y);
  EXPECT_EQ(1u + waited, kicks.count());
  std::cout << "waited " << waited << ", went on " << went_on << std::endl;
}

TEST_F(OobPull, AbandonedSetStartsNothing)
{
  // the PG dropped the request while its job waited for a thread: the
  // job posts nothing, and kicks nothing
  auto x = std::make_unique<Transfers>(cct.get(), 1, 16, 1 << 30, 30s,
				       [] {});
  x->start();
  fake_puller e;
  std::mutex gate_m;
  std::condition_variable gate_cond;
  bool open = false;
  e.during = [&] {
    std::unique_lock l(gate_m);
    gate_cond.wait(l, [&] { return open; });
  };
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  auto held = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}});
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env(x.get(), &e, &t), *held,
					ops_of(held)));
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL, 4096},
		    {CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL, 8192}});
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env(x.get(), &e, &t), *op,
					ops_of(op)));
  op->oob_pull->abandon();
  {
    std::lock_guard l(gate_m);
    open = true;
  }
  gate_cond.notify_all();
  wait_until_idle(*x);
  ASSERT_TRUE(op->oob_pull->settled());
  // only the held request pulled, and only it was kicked
  EXPECT_EQ(1u, e.calls.size());
  EXPECT_EQ(1u, kicks.count());
  std::vector<OSDOp> ops(2);
  std::vector<oob_result_t> pulled(2);
  EXPECT_EQ(-ESHUTDOWN, op->oob_pull->consume(ops, pulled));
  for (const auto& r : error_results(op)) {
    EXPECT_EQ(oob_result_t::FLAG_DECLINED, r.flags);
  }
}

TEST_F(OobPull, LaterAttemptWaitsForEarlierPulls)
{
  // The pulls of a request that run on a transfer thread are out, for
  // every PG of the OSD, until they settled, whatever the PG did with
  // the request meanwhile. A later attempt of it finds them, waits for
  // them where it is, and pulls the window itself once they settled
  auto x = service(1);
  fake_puller e;
  std::mutex gate_m;
  std::condition_variable gate_cond;
  bool entered = false, open = false;
  e.during = [&] {
    std::unique_lock l(gate_m);
    entered = true;
    gate_cond.notify_all();
    gate_cond.wait(l, [&] { return open; });
  };
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  pulls_out out;
  auto env1 = env(x.get(), &e, &t);
  env1.out = &out;
  const std::vector<op_spec> specs = {
    {CEPH_OSD_OP_WRITE, 4096, "ok", delivery_t::FLAG_PULL, 0}};
  auto first = new_op(specs, ceph_clock_now(), 7);
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env1, *first, ops_of(first)));
  EXPECT_EQ(1u, out.size());
  {
    // the PG drops the first attempt while its pull reads the window
    std::unique_lock l(gate_m);
    ASSERT_TRUE(gate_cond.wait_for(l, 10s, [&] { return entered; }));
  }
  first->oob_pull->abandon();

  auto second = new_op(specs, ceph_clock_now(), 7);
  ASSERT_EQ(first->get_reqid(), second->get_reqid());
  EXPECT_FALSE(out.find(new_op(specs, ceph_clock_now(), 8)->get_reqid()));
  auto earlier = out.find(second->get_reqid());
  ASSERT_EQ(first->oob_pull.get(), earlier.get());
  // the second waits for it at its own PG: the settle kicks there, once
  const auto t2 = test_target(2);
  EXPECT_TRUE(earlier->park(t2));
  {
    std::lock_guard l(gate_m);
    open = true;
  }
  gate_cond.notify_all();
  ASSERT_TRUE(kicks.wait_for(1));
  {
    std::lock_guard l(kicks.m);
    EXPECT_TRUE(same_target(t2, kicks.kicked[0]));
  }
  wait_until_idle(*x);
  EXPECT_FALSE(out.find(second->get_reqid()));
  EXPECT_EQ(0u, out.size());

  // then it pulls the window itself, out while it runs
  e.during = [&] {
    EXPECT_EQ(second->oob_pull.get(), out.find(second->get_reqid()).get());
  };
  bool parked = false;
  EXPECT_EQ(0, run_request(env1, second, &parked));
  EXPECT_TRUE(parked);
  EXPECT_EQ(2u, e.calls.size());
  EXPECT_EQ(e.window.substr(0, 4096), ops_of(second)[0].indata.to_str());
  wait_until_idle(*x);
  EXPECT_EQ(0u, out.size());
  EXPECT_EQ(2u, kicks.count());
}

TEST_F(OobPull, OnlyPullsOnTransferThreadsAreOut)
{
  // Pulls that run on the thread that starts them hold its PG until they
  // settled: no attempt of their request can find them out
  Throttle t(cct.get(), "osd_client_bytes", 1 << 20, false);
  pulls_out out;
  for (unsigned threads : {0u, 2u}) {
    SCOPED_TRACE(threads);
    auto x = service(threads);
    fake_puller e;
    size_t seen = 99;
    e.during = [&] { seen = out.size(); };
    auto env1 = env(x.get(), &e, &t);
    env1.out = &out;
    // with threads, a coroutine's request
    env1.here = threads > 0;
    auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}});
    ASSERT_EQ(0, pull_payloads(env1, *op, ops_of(op)));
    EXPECT_EQ(0u, seen);
    EXPECT_EQ(0u, out.size());
  }

  // a job the service refuses is out until it settled, before the pull
  // step returns
  auto x = std::make_unique<Transfers>(cct.get(), 1, 1, 1 << 30, 30s,
				       [] {});
  x->start();
  fake_puller e;
  std::mutex gate_m;
  std::condition_variable gate_cond;
  bool open = false;
  e.during = [&] {
    std::unique_lock l(gate_m);
    gate_cond.wait(l, [&] { return open; });
  };
  auto env1 = env(x.get(), &e, &t);
  env1.out = &out;
  auto held = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		       delivery_t::FLAG_PULL, 0}}, ceph_clock_now(), 1);
  ASSERT_EQ(PULLS_PARKED, pull_payloads(env1, *held, ops_of(held)));
  auto op = new_op({{CEPH_OSD_OP_WRITE, 4096, "ok",
		     delivery_t::FLAG_PULL, 4096}}, ceph_clock_now(), 2);
  EXPECT_EQ(-EOPNOTSUPP, pull_payloads(env1, *op, ops_of(op)));
  EXPECT_FALSE(out.find(op->get_reqid()));
  EXPECT_EQ(held->oob_pull.get(), out.find(held->get_reqid()).get());
  EXPECT_EQ(1u, out.size());
  {
    std::lock_guard l(gate_m);
    open = true;
  }
  gate_cond.notify_all();
  ASSERT_TRUE(kicks.wait_for(1));
  wait_until_idle(*x);
  EXPECT_EQ(0u, out.size());
}

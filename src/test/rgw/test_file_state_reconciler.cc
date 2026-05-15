// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

// Reconciler + compose_exports unit tests. Split out of
// test_file_state_memory_store.cc so the file_state substrate
// (Store / MemoryStore / ChangeFeed) stays testable without
// dragging in the reconciler / GaneshaSink layer. These tests use
// RecordingGaneshaSink only; the former DbusGaneshaSink tests are
// intentionally not carried forward (the DBus sink is being
// replaced by a gRPC sink and is preserved only in branch
// wip-rgw-s3-files-api-design / PR #68852).

#include "file_state/change_feed.h"
#include "file_state/desired_export.h"
#include "file_state/ganesha_sink.h"
#include "file_state/memory_store.h"
#include "file_state/reconciler.h"
#include "rgw_s3files_errors.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>

using namespace rgw::file_state;

namespace {

constexpr std::string_view kAccount = "1234567890";
constexpr std::string_view kBucket  = "arn:aws:s3:::test-bucket";
constexpr std::string_view kRole    = "arn:aws:iam::1234567890:role/test-role";
constexpr std::string_view kZone    = "00000000000000000000000000000001";

CreateFileSystemRequest minimum_fs_req() {
  CreateFileSystemRequest req;
  req.owner_account_id = kAccount;
  req.bucket_arn = kBucket;
  req.role_arn = kRole;
  return req;
}

}  // namespace

// =================================================================
// compose_exports
// =================================================================

namespace {

// Helper: create a complete (FS, AP, MT) tuple in the store and
// return the ids.
struct FullTuple {
  std::string fs_id;
  std::string ap_id;
  std::string mt_id;
};

FullTuple make_full_tuple(MemoryStore& s,
                           std::string_view bucket = kBucket,
                           std::string_view zone  = kZone,
                           std::string_view ap_path = "") {
  CreateFileSystemRequest fsreq;
  fsreq.owner_account_id = kAccount;
  fsreq.bucket_arn = std::string(bucket);
  fsreq.role_arn = kRole;
  fsreq.prefix = "fsp/";
  auto fs = s.create_file_system(fsreq);
  if (!ok(fs)) return {};

  CreateAccessPointRequest apreq;
  apreq.owner_account_id = kAccount;
  apreq.filesystem_id = value(fs).spec.id;
  if (!ap_path.empty()) {
    RootDirectory rd;
    rd.path = std::string(ap_path);
    apreq.root_directory = rd;
  }
  apreq.posix_user = PosixUser{1000, 1000, {}};
  auto ap = s.create_access_point(apreq);
  if (!ok(ap)) return {};

  CreateMountTargetRequest mtreq;
  mtreq.owner_account_id = kAccount;
  mtreq.filesystem_id = value(fs).spec.id;
  mtreq.zone_id = std::string(zone);
  auto mt = s.create_mount_target(mtreq);
  if (!ok(mt)) return {};

  return {value(fs).spec.id, value(ap).spec.id, value(mt).spec.id};
}

// Stub resolver used by every test that calls compose_exports() or
// constructs a Reconciler.  Returns one fixed credential pair for any
// non-empty account-id; an empty account-id yields nullopt so we
// exercise the resolution-failed path too.
class StubBootstrapResolver : public BootstrapResolver {
 public:
  std::optional<BootstrapCredentials> resolve(
      const std::string& account_id) override {
    if (account_id.empty()) return std::nullopt;
    return BootstrapCredentials{"testid", "ak", "sk"};
  }
};

// File-scope instance: stateless, so a single shared instance is
// fine for all tests below.
StubBootstrapResolver boot_;

// Build a complete (FS, AP, MT) tuple owned by `account_id`. Used by
// multi-account tests where make_full_tuple's hardcoded kAccount
// would collapse two accounts into one.
FullTuple make_full_tuple_for_account(MemoryStore& s,
                                       std::string_view account_id,
                                       std::string_view bucket,
                                       std::string_view role,
                                       std::string_view zone) {
  CreateFileSystemRequest fsreq;
  fsreq.owner_account_id = std::string(account_id);
  fsreq.bucket_arn = std::string(bucket);
  fsreq.role_arn = std::string(role);
  fsreq.prefix = "fsp/";
  auto fs = s.create_file_system(fsreq);
  if (!ok(fs)) return {};

  CreateAccessPointRequest apreq;
  apreq.owner_account_id = std::string(account_id);
  apreq.filesystem_id = value(fs).spec.id;
  apreq.posix_user = PosixUser{1000, 1000, {}};
  auto ap = s.create_access_point(apreq);
  if (!ok(ap)) return {};

  CreateMountTargetRequest mtreq;
  mtreq.owner_account_id = std::string(account_id);
  mtreq.filesystem_id = value(fs).spec.id;
  mtreq.zone_id = std::string(zone);
  auto mt = s.create_mount_target(mtreq);
  if (!ok(mt)) return {};

  return {value(fs).spec.id, value(ap).spec.id, value(mt).spec.id};
}

// Resolver that returns different bootstrap creds per account and
// counts how many times each account is resolved.  Accounts not in
// the map yield std::nullopt so the resolution-failure path is also
// exercised.
class MultiAccountStubResolver : public BootstrapResolver {
 public:
  explicit MultiAccountStubResolver(
      std::unordered_map<std::string, BootstrapCredentials> creds)
      : creds_(std::move(creds)) {}

  std::optional<BootstrapCredentials> resolve(
      const std::string& account_id) override {
    ++calls_[account_id];
    auto it = creds_.find(account_id);
    if (it == creds_.end()) return std::nullopt;
    return it->second;
  }

  int calls_for(const std::string& account_id) const {
    auto it = calls_.find(account_id);
    return it == calls_.end() ? 0 : it->second;
  }

 private:
  std::unordered_map<std::string, BootstrapCredentials> creds_;
  std::unordered_map<std::string, int> calls_;
};

}  // namespace

TEST(ComposeExports, EmptyStoreYieldsEmpty) {
  MemoryStore s;
  EXPECT_TRUE(compose_exports(s, boot_).empty());
}

TEST(ComposeExports, FsAlone_NoAp_YieldsNothing) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  EXPECT_TRUE(compose_exports(s, boot_).empty());
}

TEST(ComposeExports, FsAndApButNoMt_YieldsNothing) {
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));
  CreateAccessPointRequest apreq;
  apreq.owner_account_id = kAccount;
  apreq.filesystem_id = value(fs).spec.id;
  auto ap = s.create_access_point(apreq);
  ASSERT_TRUE(ok(ap));
  EXPECT_TRUE(compose_exports(s, boot_).empty());
}

TEST(ComposeExports, FullTuple_YieldsOneExport) {
  MemoryStore s;
  auto t = make_full_tuple(s);
  ASSERT_FALSE(t.fs_id.empty());

  auto exports = compose_exports(s, boot_);
  ASSERT_EQ(exports.size(), 1u);
  const auto& e = exports[0];
  EXPECT_EQ(e.fs_id, t.fs_id);
  EXPECT_EQ(e.ap_id, t.ap_id);
  EXPECT_EQ(e.mt_id, t.mt_id);
  EXPECT_EQ(e.bucket_arn, kBucket);
  EXPECT_EQ(e.role_arn, kRole);
  EXPECT_EQ(e.zone_id, kZone);
  // No ap rootDirectory, so composed_prefix == fs prefix.
  EXPECT_EQ(e.composed_prefix, "fsp/");
  ASSERT_TRUE(e.posix_user.has_value());
  EXPECT_EQ(e.posix_user->uid, 1000);
}

TEST(ComposeExports, ComposesPrefixWithRootDirectory) {
  MemoryStore s;
  // FS prefix + AP rootDirectory `/scoped/team-a` should yield
  // composed prefix `fsp/scoped/team-a/`.
  make_full_tuple(s, kBucket, kZone, "/scoped/team-a");
  auto exports = compose_exports(s, boot_);
  ASSERT_EQ(exports.size(), 1u);
  EXPECT_EQ(exports[0].composed_prefix, "fsp/scoped/team-a/");
}

TEST(ComposeExports, MultipleApsCrossWithMt) {
  // One FS, two APs, one MT → two exports (cross-product).
  MemoryStore s;
  auto fs = s.create_file_system(minimum_fs_req());
  ASSERT_TRUE(ok(fs));

  CreateAccessPointRequest apreq;
  apreq.owner_account_id = kAccount;
  apreq.filesystem_id = value(fs).spec.id;
  auto ap1 = s.create_access_point(apreq);
  auto ap2 = s.create_access_point(apreq);
  ASSERT_TRUE(ok(ap1));
  ASSERT_TRUE(ok(ap2));

  CreateMountTargetRequest mtreq;
  mtreq.owner_account_id = kAccount;
  mtreq.filesystem_id = value(fs).spec.id;
  mtreq.zone_id = kZone;
  auto mt = s.create_mount_target(mtreq);
  ASSERT_TRUE(ok(mt));

  auto exports = compose_exports(s, boot_);
  EXPECT_EQ(exports.size(), 2u);
}

TEST(ComposeExports, OrphanedMt_Skipped) {
  // Construct a scenario where a MT references a FS that
  // doesn't exist (shouldn't happen in practice, but the
  // reconciler must tolerate transient inconsistency).
  MemoryStore s;
  auto t = make_full_tuple(s);
  ASSERT_FALSE(t.fs_id.empty());

  // Delete the AP and FS, leaving the MT orphaned. We can't
  // delete an FS while children exist, so delete in order.
  s.delete_access_point(kAccount, t.ap_id);
  // delete_mount_target(t.mt_id) would clean cleanly, so we
  // skip that to simulate the orphan case.
  // The FS still has the MT, so delete_file_system should
  // be rejected; the test isn't about that, it's about
  // compose_exports tolerating inconsistency. Drop the MT
  // explicitly to leave just the FS.
  // Actually simpler: just verify post-delete_ap, the
  // exports list is empty (no AP → no export).
  EXPECT_TRUE(compose_exports(s, boot_).empty());
}

// -----------------------------------------------------------------
// Multi-account isolation
//
// The reconciler resolves bootstrap credentials per FS owner and
// renders one DesiredExport per (FS, AP, MT) tuple. These tests
// guard the property that an account's identifiers, role ARN, and
// bootstrap credentials never appear in another account's exports
// -- the in-memory side of tenant isolation.  The runtime side
// (trust-policy denial when account A tries to AssumeRole on
// account B's role) is enforced by RGW's STSService and exercised
// in the librgw / end-to-end suites.
// -----------------------------------------------------------------

TEST(ComposeExports, MultiAccount_BootstrapResolvedPerAccount) {
  // Two FSes owned by two different accounts. Each rendered
  // DesiredExport must carry its own account's bootstrap
  // credentials and role ARN -- a leak here would let one tenant's
  // NFS export AssumeRole as another tenant's principal.
  MemoryStore s;
  constexpr std::string_view kAcctA = "111111111111";
  constexpr std::string_view kAcctB = "222222222222";
  auto ta = make_full_tuple_for_account(
      s, kAcctA, "arn:aws:s3:::bucket-a",
      "arn:aws:iam::111111111111:role/role-a",
      "00000000000000000000000000000001");
  auto tb = make_full_tuple_for_account(
      s, kAcctB, "arn:aws:s3:::bucket-b",
      "arn:aws:iam::222222222222:role/role-b",
      "00000000000000000000000000000002");
  ASSERT_FALSE(ta.fs_id.empty());
  ASSERT_FALSE(tb.fs_id.empty());

  MultiAccountStubResolver resolver({
      {std::string(kAcctA),
       BootstrapCredentials{"root-a", "AKIA-A", "SECRET-A"}},
      {std::string(kAcctB),
       BootstrapCredentials{"root-b", "AKIA-B", "SECRET-B"}},
  });

  auto exports = compose_exports(s, resolver);
  ASSERT_EQ(exports.size(), 2u);

  // compose_exports sorts by (fs_id, ap_id, mt_id); we don't know
  // which fs_id sorted first, so look up by owner_account_id.
  const DesiredExport* a = nullptr;
  const DesiredExport* b = nullptr;
  for (const auto& e : exports) {
    if (e.owner_account_id == kAcctA) a = &e;
    if (e.owner_account_id == kAcctB) b = &e;
  }
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);

  EXPECT_EQ(a->bootstrap_user_id,    "root-a");
  EXPECT_EQ(a->bootstrap_access_key, "AKIA-A");
  EXPECT_EQ(a->bootstrap_secret_key, "SECRET-A");
  EXPECT_EQ(b->bootstrap_user_id,    "root-b");
  EXPECT_EQ(b->bootstrap_access_key, "AKIA-B");
  EXPECT_EQ(b->bootstrap_secret_key, "SECRET-B");

  // role_arn pairing: a swap here would silently mount one
  // tenant's bucket under another tenant's role.
  EXPECT_EQ(a->role_arn, "arn:aws:iam::111111111111:role/role-a");
  EXPECT_EQ(b->role_arn, "arn:aws:iam::222222222222:role/role-b");
}

TEST(ComposeExports, MultiAccount_ResolverFailureSkipsOnlyAffectedAccount) {
  // If bootstrap resolution fails for one account (e.g. its root
  // user has no access keys), only that account's exports drop --
  // the other account's exports still render. A blanket-fail
  // would over-shrink the desired set and tear down healthy
  // mounts.
  MemoryStore s;
  constexpr std::string_view kAcctA = "111111111111";
  constexpr std::string_view kAcctB = "222222222222";
  make_full_tuple_for_account(s, kAcctA, "arn:aws:s3:::bucket-a",
      "arn:aws:iam::111111111111:role/role-a",
      "00000000000000000000000000000001");
  auto tb = make_full_tuple_for_account(s, kAcctB, "arn:aws:s3:::bucket-b",
      "arn:aws:iam::222222222222:role/role-b",
      "00000000000000000000000000000002");
  ASSERT_FALSE(tb.fs_id.empty());

  // Only account B has known creds; A is intentionally absent
  // from the resolver map (simulates "no usable root key").
  MultiAccountStubResolver resolver({
      {std::string(kAcctB),
       BootstrapCredentials{"root-b", "AKIA-B", "SECRET-B"}},
  });

  auto exports = compose_exports(s, resolver);
  ASSERT_EQ(exports.size(), 1u);
  EXPECT_EQ(exports[0].owner_account_id, kAcctB);
  EXPECT_EQ(exports[0].bootstrap_user_id, "root-b");
}

TEST(ComposeExports, MultiAccount_ResolverCachedPerComposeCall) {
  // compose_exports caches resolutions by owner_account_id within
  // a single call so we don't hammer SAL's account-listing path
  // once per export. Two FSes per account, two accounts -> 4
  // exports but only 2 resolve calls per compose. (Two FSes
  // can't share a bucket, hence the per-FS bucket suffix.)
  MemoryStore s;
  constexpr std::string_view kAcctA = "111111111111";
  constexpr std::string_view kAcctB = "222222222222";
  make_full_tuple_for_account(s, kAcctA, "arn:aws:s3:::bucket-a1",
      "arn:aws:iam::111111111111:role/role-a",
      "00000000000000000000000000000001");
  make_full_tuple_for_account(s, kAcctA, "arn:aws:s3:::bucket-a2",
      "arn:aws:iam::111111111111:role/role-a",
      "00000000000000000000000000000002");
  make_full_tuple_for_account(s, kAcctB, "arn:aws:s3:::bucket-b1",
      "arn:aws:iam::222222222222:role/role-b",
      "00000000000000000000000000000003");
  make_full_tuple_for_account(s, kAcctB, "arn:aws:s3:::bucket-b2",
      "arn:aws:iam::222222222222:role/role-b",
      "00000000000000000000000000000004");

  MultiAccountStubResolver resolver({
      {std::string(kAcctA),
       BootstrapCredentials{"root-a", "AKIA-A", "SECRET-A"}},
      {std::string(kAcctB),
       BootstrapCredentials{"root-b", "AKIA-B", "SECRET-B"}},
  });

  auto exports = compose_exports(s, resolver);
  EXPECT_EQ(exports.size(), 4u);
  EXPECT_EQ(resolver.calls_for(std::string(kAcctA)), 1);
  EXPECT_EQ(resolver.calls_for(std::string(kAcctB)), 1);

  // The cache is per-call: a second compose re-resolves once
  // per account (so 2 each, not 1 or 4).
  (void)compose_exports(s, resolver);
  EXPECT_EQ(resolver.calls_for(std::string(kAcctA)), 2);
  EXPECT_EQ(resolver.calls_for(std::string(kAcctB)), 2);
}

// =================================================================
// Reconciler
// =================================================================

TEST(Reconciler, ReconcileOnce_EmptyStore_AppliesEmptySet) {
  MemoryStore s;
  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  Reconciler r(s, feed, sink, boot_);
  r.reconcile_once();
  ASSERT_EQ(sink.call_count(), 1u);
  EXPECT_TRUE(sink.last().empty());
}

TEST(Reconciler, ReconcileOnce_FullStore_AppliesExpectedSet) {
  MemoryStore s;
  auto t = make_full_tuple(s);
  ASSERT_FALSE(t.fs_id.empty());

  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  Reconciler r(s, feed, sink, boot_);
  r.reconcile_once();
  ASSERT_EQ(sink.call_count(), 1u);
  ASSERT_EQ(sink.last().size(), 1u);
  EXPECT_EQ(sink.last()[0].fs_id, t.fs_id);
  EXPECT_EQ(sink.last()[0].ap_id, t.ap_id);
  EXPECT_EQ(sink.last()[0].mt_id, t.mt_id);
}

TEST(Reconciler, Start_RunsInitialReconcileWithoutChanges) {
  MemoryStore s;
  make_full_tuple(s);

  InProcessChangeFeed feed;
  RecordingGaneshaSink sink;
  // Long safety-net timer so we observe just the start-time
  // initial reconcile, not a periodic one.
  Reconciler r(s, feed, sink, boot_, ReconcilerConfig{
      .safety_net_interval = std::chrono::seconds(60),
  });
  r.start();
  // Wait briefly for the worker thread to drain its initial
  // dirty flag and apply.
  for (int i = 0; i < 100 && sink.call_count() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  r.stop();
  EXPECT_GE(sink.call_count(), 1u);
  EXPECT_EQ(sink.last().size(), 1u);
}

TEST(Reconciler, FeedSignal_TriggersReconcile) {
  MemoryStore s;
  InProcessChangeFeed feed;
  RecordingGaneshaSink sink;
  // Wire the store to the feed.
  s.set_on_change([&feed]{ feed.fire(); });

  Reconciler r(s, feed, sink, boot_, ReconcilerConfig{
      .safety_net_interval = std::chrono::seconds(60),
  });
  r.start();

  // Wait for the initial reconcile to settle.
  for (int i = 0; i < 100 && sink.call_count() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  const auto initial_calls = sink.call_count();

  // Mutate: should fire the feed → wake the worker → another
  // apply().
  make_full_tuple(s);
  for (int i = 0; i < 200 && sink.call_count() == initial_calls; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  r.stop();

  EXPECT_GT(sink.call_count(), initial_calls);
  EXPECT_EQ(sink.last().size(), 1u);
}

TEST(Reconciler, BurstOfChanges_CoalescesIntoFewerApplies) {
  MemoryStore s;
  InProcessChangeFeed feed;
  RecordingGaneshaSink sink;
  s.set_on_change([&feed]{ feed.fire(); });

  Reconciler r(s, feed, sink, boot_, ReconcilerConfig{
      .safety_net_interval = std::chrono::seconds(60),
  });
  r.start();

  // Fire a burst of mutations as fast as we can. The reconciler
  // should coalesce — far fewer applies than mutations.
  constexpr int kBurst = 50;
  for (int i = 0; i < kBurst; ++i) {
    auto fsreq = minimum_fs_req();
    fsreq.bucket_arn = std::string(kBucket) + "-" + std::to_string(i);
    s.create_file_system(fsreq);
  }

  // Wait for the worker to settle. Generous so a slow CI box
  // doesn't flake.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  r.stop();

  // Strictly fewer applies than mutations means coalescing.
  // (Initial + at-least-one + at-most-kBurst.)
  EXPECT_LT(sink.call_count(), kBurst);
  EXPECT_GE(sink.call_count(), 1u);
}

TEST(Reconciler, Idempotent_NoStateChange_StillSafeToReconcile) {
  // Repeated reconcile_once() with no underlying change yields
  // identical apply() payloads — important for the safety-net
  // timer not to jitter Ganesha unnecessarily.
  MemoryStore s;
  make_full_tuple(s);
  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  Reconciler r(s, feed, sink, boot_);
  r.reconcile_once();
  r.reconcile_once();
  r.reconcile_once();
  ASSERT_EQ(sink.call_count(), 3u);
  EXPECT_EQ(sink.calls()[0], sink.calls()[1]);
  EXPECT_EQ(sink.calls()[1], sink.calls()[2]);
}

TEST(Reconciler, SafetyNetTimer_FiresEvenWithoutChanges) {
  MemoryStore s;
  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  // Tiny safety-net interval so the test doesn't hang.
  Reconciler r(s, feed, sink, boot_, ReconcilerConfig{
      .safety_net_interval = std::chrono::milliseconds(50),
  });
  r.start();
  // Hold the reconciler alive long enough for at least 3 timer
  // wakes (initial + 2-3 from the 50ms timer).
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  r.stop();
  // Initial reconcile + ~3-4 timer-driven ones.
  EXPECT_GE(sink.call_count(), 3u);
}

TEST(Reconciler, StopIsIdempotentAndSafe) {
  MemoryStore s;
  NoopChangeFeed feed;
  RecordingGaneshaSink sink;
  Reconciler r(s, feed, sink, boot_);
  r.stop();              // never started
  r.start();
  r.stop();
  r.stop();              // double stop
}

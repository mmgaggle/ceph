// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Concurrent uncached writers on one client, with client_write_submit_unlocked
 * on and off, through both the blocking and the non-blocking write paths.
 * Every write is followed by an fsync, and the test reads everything back.
 *
 * Each thread writes its own 4 KiB blocks of a shared file, which encrypted
 * files can send without a read-modify-write, and unaligned records to a file
 * of its own, which makes encrypted files read the partial blocks first.
 */

#include <errno.h>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <fmt/format.h>

#include "test/client/TestClient.h"

namespace {

constexpr int NTHREADS = 8;
constexpr int NWRITES = 32;
constexpr size_t BLOCK = 4096;
constexpr size_t RECORD = 1000;
constexpr int QUEUE_DEPTH = 4;

char fill(int thread, int i)
{
  return 'A' + (thread * NWRITES + i) % 26;
}

class ConfSetting {
  std::string key, old;
public:
  ConfSetting(const std::string& key, const std::string& val) : key(key) {
    auto& conf = g_ceph_context->_conf;
    conf.get_val(key, &old);
    conf.set_val_or_die(key, val);
    conf.apply_changes(nullptr);
  }
  ~ConfSetting() {
    auto& conf = g_ceph_context->_conf;
    conf.set_val_or_die(key, old);
    conf.apply_changes(nullptr);
  }
};

} // anonymous namespace

class TestClientConcurrentWrite
  : public TestClient,
    public ::testing::WithParamInterface<std::tuple<bool, bool>> {
protected:
  // Write len bytes of c at off, then fsync. Non-blocking writes (with the
  // fsync flag) are queued up to QUEUE_DEPTH at a time and waited for by
  // drain(). The data is copied before the call returns.
  struct Writer {
    ClientScaffold *client;
    Fh *fh;
    bool nonblocking;
    std::vector<std::unique_ptr<C_SaferCond>> inflight;

    void write(loff_t off, size_t len, char c) {
      std::string buf(len, c);
      if (!nonblocking) {
        ASSERT_EQ((int)len, client->ll_write(fh, off, len, buf.data()));
        ASSERT_EQ(0, client->ll_fsync(fh, false));
        return;
      }
      struct iovec iov = {buf.data(), len};
      auto done = std::make_unique<C_SaferCond>("concurrent-write");
      ASSERT_EQ(0, client->ll_preadv_pwritev(fh, &iov, 1, off, true,
                                             done.get(), nullptr, true, false));
      inflight.push_back(std::move(done));
      if (inflight.size() >= QUEUE_DEPTH) {
        drain(len);
      }
    }

    void drain(size_t len) {
      for (auto& done : inflight) {
        ASSERT_EQ((int64_t)len, done->wait());
      }
      inflight.clear();
    }
  };

  void check_contents(Fh *fh, Inode *in, const std::string& expect) {
    struct ceph_statx stx;
    ASSERT_EQ(0, client->ll_getattrx(in, &stx, CEPH_STATX_SIZE, 0, myperm));
    ASSERT_EQ(expect.size(), stx.stx_size);

    bufferlist bl;
    ASSERT_EQ((int)expect.size(), client->ll_read(fh, 0, expect.size(), &bl));
    const std::string got = bl.to_str();
    const auto mismatch = std::mismatch(expect.begin(), expect.end(), got.begin());
    ASSERT_TRUE(mismatch.first == expect.end())
      << "first mismatch at offset " << (mismatch.first - expect.begin());
  }
};

TEST_P(TestClientConcurrentWrite, WritesWithFsync) {
  const auto [unlocked, nonblocking] = GetParam();
  ConfSetting oc("client_oc", "false");
  ConfSetting submit("client_write_submit_unlocked", unlocked ? "true" : "false");

  Inode *root = client->get_root();
  ASSERT_NE(nullptr, root);
  const std::string prefix = fmt::format("concurrent_write_{}_{}_{}_{}",
                                         getpid(), rand(), unlocked, nonblocking);
  struct ceph_statx stx;

  const std::string shared_name = prefix + "_shared";
  Inode *shared_in = nullptr;
  Fh *shared_fh = nullptr;
  ASSERT_EQ(0, client->ll_createx(root, shared_name.c_str(), 0666,
                                  O_RDWR | O_CREAT | O_TRUNC,
                                  &shared_in, &shared_fh, &stx, 0, 0, myperm));

  std::vector<std::string> own_names(NTHREADS);
  std::vector<Inode*> own_in(NTHREADS, nullptr);
  std::vector<Fh*> own_fh(NTHREADS, nullptr);
  for (int t = 0; t < NTHREADS; t++) {
    own_names[t] = fmt::format("{}_t{}", prefix, t);
    ASSERT_EQ(0, client->ll_createx(root, own_names[t].c_str(), 0666,
                                    O_RDWR | O_CREAT | O_TRUNC,
                                    &own_in[t], &own_fh[t], &stx, 0, 0, myperm));
  }

  std::vector<std::thread> threads;
  for (int t = 0; t < NTHREADS; t++) {
    threads.emplace_back([&, t] {
      Writer shared{client, shared_fh, nonblocking};
      Writer own{client, own_fh[t], nonblocking};
      for (int i = 0; i < NWRITES; i++) {
        shared.write((loff_t)(i * NTHREADS + t) * BLOCK, BLOCK, fill(t, i));
        // Non-blocking records to one file must not overlap in flight: the
        // read-modify-write of encrypted files would race on the shared block.
        own.write((loff_t)i * RECORD, RECORD, fill(t, i));
        own.drain(RECORD);
      }
      shared.drain(BLOCK);
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  std::string expect_shared(NTHREADS * NWRITES * BLOCK, '\0');
  for (int t = 0; t < NTHREADS; t++) {
    for (int i = 0; i < NWRITES; i++) {
      expect_shared.replace((i * NTHREADS + t) * BLOCK, BLOCK, BLOCK, fill(t, i));
    }
  }
  check_contents(shared_fh, shared_in, expect_shared);

  for (int t = 0; t < NTHREADS; t++) {
    std::string expect_own(NWRITES * RECORD, '\0');
    for (int i = 0; i < NWRITES; i++) {
      expect_own.replace(i * RECORD, RECORD, RECORD, fill(t, i));
    }
    check_contents(own_fh[t], own_in[t], expect_own);
  }

  ASSERT_EQ(0, client->ll_release(shared_fh));
  ASSERT_EQ(0, client->ll_unlink(root, shared_name.c_str(), myperm));
  client->ll_put(shared_in);
  for (int t = 0; t < NTHREADS; t++) {
    ASSERT_EQ(0, client->ll_release(own_fh[t]));
    ASSERT_EQ(0, client->ll_unlink(root, own_names[t].c_str(), myperm));
    client->ll_put(own_in[t]);
  }
}

INSTANTIATE_TEST_SUITE_P(
  SubmitUnlocked, TestClientConcurrentWrite,
  ::testing::Combine(::testing::Bool(), ::testing::Bool()),
  [](const ::testing::TestParamInfo<std::tuple<bool, bool>>& info) {
    return fmt::format("{}_{}",
                       std::get<0>(info.param) ? "Unlocked" : "Locked",
                       std::get<1>(info.param) ? "NonBlocking" : "Blocking");
  });

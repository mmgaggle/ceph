// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd/ec_gather.h"
#include "osd/ECMsgTypes.h"

#include <cstring>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using ceph::osd::oob::take_pushed;
using pushed_t =
  std::map<std::string, std::list<std::pair<uint64_t, uint64_t>>>;
using out_t =
  std::map<std::string, std::list<std::pair<uint64_t, ceph::buffer::list>>>;

namespace {

/// A shard's sub-read result, as it pushes it: the extents' data back to
/// back in reply order, gathered from buffers of its own (bluestore hands
/// back many), and the crc32c of the whole.
struct shard_push_t {
  pushed_t pushed;
  std::string data;
  uint32_t crc = 0;

  void add(const std::string& obj, uint64_t offset, const std::string& d) {
    pushed[obj].emplace_back(offset, d.size());
    data += d;
  }
  void seal() {
    ceph::buffer::list all;
    for (size_t o = 0; o < data.size(); o += 1000) {
      all.append(ceph::buffer::copy(data.data() + o,
				    std::min<size_t>(1000, data.size() - o)));
    }
    crc = all.crc32c(-1);
  }
};

std::string pattern(char c, size_t n)
{
  std::string s(n, c);
  for (size_t i = 0; i < n; i += 7) {
    s[i] = static_cast<char>(i);
  }
  return s;
}

shard_push_t two_objects()
{
  shard_push_t p;
  p.add("a", 0, pattern('a', 8192));
  p.add("a", 65536, pattern('b', 4096));
  p.add("c", 4096, pattern('c', 12288));
  p.seal();
  return p;
}

std::string flatten(const out_t& out, const std::string& obj)
{
  std::string s;
  for (const auto& [ofs, bl] : out.at(obj)) {
    s += bl.to_str();
  }
  return s;
}

} // anonymous namespace

TEST(ECGather, TakesWhatTheShardPushed)
{
  auto p = two_objects();
  std::vector<char> win(64 << 10, 'x');
  std::memcpy(win.data(), p.data.data(), p.data.size());
  out_t out;
  auto t = take_pushed(win.data(), win.size(), p.pushed,
		       std::optional<uint32_t>(p.crc), out);
  ASSERT_TRUE(t.ok());
  EXPECT_EQ(p.data.size(), t.bytes);
  EXPECT_EQ(p.crc, t.crc);
  ASSERT_EQ(2u, out.size());
  ASSERT_EQ(2u, out.at("a").size());
  EXPECT_EQ(0u, out.at("a").front().first);
  EXPECT_EQ(65536u, out.at("a").back().first);
  EXPECT_EQ(pattern('a', 8192) + pattern('b', 4096), flatten(out, "a"));
  EXPECT_EQ(4096u, out.at("c").front().first);
  EXPECT_EQ(pattern('c', 12288), flatten(out, "c"));
}

TEST(ECGather, RefusesAWindowThatIsNotThePush)
{
  // a byte of a stray write, the tail of a push that completed short,
  // and another gather's data, in turn: each is found, and nothing of
  // the window is taken
  auto p = two_objects();
  std::vector<char> good(64 << 10, 'x');
  std::memcpy(good.data(), p.data.data(), p.data.size());

  auto stray = good;
  stray[9000] ^= 0x40;
  auto short_push = good;
  std::memset(short_push.data() + 16384, 0, p.data.size() - 16384);
  auto other = good;
  const auto o = pattern('z', p.data.size());
  std::memcpy(other.data(), o.data(), o.size());

  for (auto* w : {&stray, &short_push, &other}) {
    out_t out;
    auto t = take_pushed(w->data(), w->size(), p.pushed,
			 std::optional<uint32_t>(p.crc), out);
    EXPECT_TRUE(t.mismatch);
    EXPECT_FALSE(t.ok());
    EXPECT_NE(p.crc, t.crc);
    EXPECT_TRUE(out.empty());
  }
}

TEST(ECGather, RefusesExtentsPastTheWindow)
{
  auto p = two_objects();
  std::vector<char> win(p.data.size() - 1);
  std::memcpy(win.data(), p.data.data(), win.size());
  out_t out;
  auto t = take_pushed(win.data(), win.size(), p.pushed,
		       std::optional<uint32_t>(p.crc), out);
  EXPECT_TRUE(t.overrun);
  EXPECT_FALSE(t.ok());
  EXPECT_TRUE(out.empty());
}

TEST(ECGather, AShardWithoutAChecksumIsTaken)
{
  // a shard from before the checksum: what is in the window is taken as
  // it was before
  auto p = two_objects();
  std::vector<char> win(64 << 10, 'q');
  out_t out;
  auto t = take_pushed(win.data(), win.size(), p.pushed,
		       std::optional<uint32_t>(), out);
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(std::string(8192, 'q') + std::string(4096, 'q'),
	    flatten(out, "a"));
}

TEST(ECGather, AppendsToWhatIsThere)
{
  auto p = two_objects();
  std::vector<char> win(64 << 10);
  std::memcpy(win.data(), p.data.data(), p.data.size());
  out_t out;
  ceph::buffer::list keep;
  keep.append("k");
  out["d"].emplace_back(7, keep);
  auto t = take_pushed(win.data(), win.size(), p.pushed,
		       std::optional<uint32_t>(p.crc), out);
  ASSERT_TRUE(t.ok());
  EXPECT_EQ(3u, out.size());
  EXPECT_EQ("k", flatten(out, "d"));
}

TEST(ECGather, TheReplyCarriesTheChecksum)
{
  ECSubReadReply r;
  r.from = pg_shard_t(1, shard_id_t(1));
  r.tid = 7;
  const hobject_t hoid(sobject_t("obj", CEPH_NOSNAP));
  r.pushed[hoid].emplace_back(0, 4096);
  r.pushed_crc = 0xdeadbeef;

  ceph::buffer::list p, d;
  r.encode(p, d, CEPH_FEATURES_ALL);
  p.claim_append(d);
  ECSubReadReply got;
  auto it = std::as_const(p).begin();
  got.decode(it);
  EXPECT_EQ(r.pushed, got.pushed);
  ASSERT_TRUE(got.pushed_crc);
  EXPECT_EQ(0xdeadbeefu, *got.pushed_crc);

  // to a peer from before pushes, neither goes out
  ceph::buffer::list op, od;
  r.encode(op, od, 0);
  op.claim_append(od);
  ECSubReadReply old;
  old.pushed_crc = 1;
  auto oit = std::as_const(op).begin();
  old.decode(oit);
  EXPECT_TRUE(old.pushed.empty());
  EXPECT_FALSE(old.pushed_crc);
}

TEST(ECGather, TheReplySaysWhenNothingWasSent)
{
  // a shard whose executor refused the push before sending anything
  // replies inline and says so, and the primary lends the window again
  // at once
  ECSubReadReply r;
  r.from = pg_shard_t(1, shard_id_t(1));
  r.tid = 8;
  const hobject_t hoid(sobject_t("obj", CEPH_NOSNAP));
  ceph::buffer::list bl;
  bl.append(std::string(4096, 'i'));
  r.buffers_read[hoid].emplace_back(0, bl);
  r.push_declined = true;

  ceph::buffer::list p, d;
  r.encode(p, d, CEPH_FEATURES_ALL);
  ECSubReadReply got;
  auto pit = std::as_const(p).begin();
  auto dit = std::as_const(d).begin();
  got.decode(pit, dit);
  EXPECT_TRUE(got.push_declined);
  EXPECT_TRUE(got.pushed.empty());
  EXPECT_FALSE(got.pushed_crc);
  ASSERT_EQ(1u, got.buffers_read[hoid].size());
  EXPECT_TRUE(got.buffers_read[hoid].front().second.contents_equal(bl));

  // a shard that may have written says nothing of the kind
  r.push_declined = false;
  ceph::buffer::list p2, d2;
  r.encode(p2, d2, CEPH_FEATURES_ALL);
  ECSubReadReply wrote;
  wrote.push_declined = true;
  auto p2it = std::as_const(p2).begin();
  auto d2it = std::as_const(d2).begin();
  wrote.decode(p2it, d2it);
  EXPECT_FALSE(wrote.push_declined);

  // to a peer from before pushes, it does not go out
  r.push_declined = true;
  ceph::buffer::list op, od;
  r.encode(op, od, 0);
  op.claim_append(od);
  ECSubReadReply old;
  old.push_declined = true;
  auto oit = std::as_const(op).begin();
  old.decode(oit);
  EXPECT_FALSE(old.push_declined);
}

TEST(ECGather, AV5ReplySaysNothingWasDeclined)
{
  // a shard from before push_declined (v5) never returns a window clean:
  // its reply decodes as possibly having written
  const hobject_t hoid(sobject_t("obj", CEPH_NOSNAP));
  std::map<hobject_t, std::list<std::pair<uint64_t, uint64_t>>> pushed;
  pushed[hoid].emplace_back(0, 4096);
  ceph::buffer::list p;
  ENCODE_START(5, 2, p);
  encode(pg_shard_t(1, shard_id_t(1)), p);
  encode(ceph_tid_t(9), p);
  encode(__u32(0), p);  // buffers_read, encoded by hand from v2
  encode(std::map<hobject_t,
		  std::map<std::string, ceph::buffer::list, std::less<>>>(),
	 p);  // attrs_read
  encode(std::map<hobject_t, int>(), p);  // errors
  encode(std::map<hobject_t, ceph::buffer::list>(), p);  // omap headers
  encode(std::map<hobject_t, std::map<std::string, ceph::buffer::list>>(),
	 p);  // omap entries
  encode(std::map<hobject_t, bool>(), p);  // omaps_complete
  encode(pushed, p);
  encode(std::optional<uint32_t>(0xfeedu), p);  // pushed_crc
  ENCODE_FINISH(p);

  ECSubReadReply got;
  got.push_declined = true;
  ceph::buffer::list d;
  auto pit = std::as_const(p).begin();
  auto dit = std::as_const(d).begin();
  got.decode(pit, dit);
  EXPECT_EQ(9u, got.tid);
  EXPECT_EQ(pushed, got.pushed);
  ASSERT_TRUE(got.pushed_crc);
  EXPECT_EQ(0xfeedu, *got.pushed_crc);
  EXPECT_FALSE(got.push_declined);
}

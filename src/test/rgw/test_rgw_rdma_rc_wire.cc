// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw/rgw_rdma_rc_wire.h"

#include <gtest/gtest.h>

using namespace rgw::rdma::rc;

namespace {

token_t sample_token()
{
  token_t t;
  t.transport = transport_t::RC;
  t.qpn = 0x000123ab;
  for (int i = 0; i < 16; i++) {
    t.gid[i] = static_cast<uint8_t>(0xfe - i);
  }
  t.rkey = 0xdeadbeef;
  t.addr = 0x00007f1234567000ULL;
  t.length = 1ULL << 30;
  t.port = 1;
  t.lid = 0x0042;
  return t;
}

} // anonymous namespace

TEST(RdmaRcWire, TokenRoundTrip)
{
  const auto t = sample_token();
  const auto hex = encode_token(t);
  ASSERT_EQ(TOKEN_HEX_LEN, hex.size());
  // transport byte first, then the little-endian qpn
  EXPECT_EQ("01ab230100", hex.substr(0, 10));
  auto back = decode_token(hex);
  ASSERT_TRUE(back);
  EXPECT_EQ(t, *back);
  EXPECT_FALSE(back->gid_is_zero());
}

TEST(RdmaRcWire, TokenLayoutPinned)
{
  // the layout is fixed by the hipObject client: 1+4+16+4+8+8+1+2
  token_t t;
  t.transport = transport_t::DC;
  t.qpn = 0x04030201;
  t.rkey = 0x08070605;
  t.addr = 0x1011121314151617ULL;
  t.length = 0x2021222324252627ULL;
  t.port = 3;
  t.lid = 0x3132;
  const std::string expected =
    "00" "01020304" "00000000000000000000000000000000" "05060708"
    "1716151413121110" "2726252423222120" "03" "3231";
  EXPECT_EQ(expected, encode_token(t));
  auto back = decode_token(expected);
  ASSERT_TRUE(back);
  EXPECT_EQ(t, *back);
  EXPECT_TRUE(back->gid_is_zero());
}

TEST(RdmaRcWire, TokenDecodeAcceptsEitherCase)
{
  const auto hex = encode_token(sample_token());
  std::string upper = hex;
  for (auto& c : upper) c = std::toupper(c);
  auto a = decode_token(hex);
  auto b = decode_token(upper);
  ASSERT_TRUE(a && b);
  EXPECT_EQ(*a, *b);
}

TEST(RdmaRcWire, TokenDecodeRejectsMalformed)
{
  const auto hex = encode_token(sample_token());
  EXPECT_FALSE(decode_token(hex.substr(1)));
  EXPECT_FALSE(decode_token(hex + "0"));
  EXPECT_FALSE(decode_token(hex + ":1:2"));
  std::string bad = hex;
  bad[10] = 'g';
  EXPECT_FALSE(decode_token(bad));
  EXPECT_FALSE(decode_token(""));
}

TEST(RdmaRcWire, ZeroTokenIsLoopbackMarker)
{
  EXPECT_TRUE(token_is_zero(std::string(TOKEN_HEX_LEN, '0')));
  EXPECT_FALSE(token_is_zero(std::string(TOKEN_HEX_LEN - 1, '0')));
  EXPECT_FALSE(token_is_zero(encode_token(sample_token())));
}

TEST(RdmaRcWire, SessionId)
{
  EXPECT_TRUE(valid_session_id("0123456789abcdef0123456789ABCDEF"));
  EXPECT_FALSE(valid_session_id("0123456789abcdef0123456789ABCDE"));
  EXPECT_FALSE(valid_session_id("0123456789abcdef0123456789ABCDEFa"));
  EXPECT_FALSE(valid_session_id("0123456789abcdef0123456789ABCDEz"));
  EXPECT_FALSE(valid_session_id(""));
}

TEST(RdmaRcWire, Psn)
{
  EXPECT_EQ(1u, *parse_psn("000001"));
  EXPECT_EQ(0xffffffu, *parse_psn("ffffff"));
  EXPECT_EQ(0xa1b2cu, *parse_psn("0A1B2C"));
  EXPECT_FALSE(parse_psn("000000"));   // zero is invalid
  EXPECT_FALSE(parse_psn("1"));        // fixed width
  EXPECT_FALSE(parse_psn("0000001"));
  EXPECT_FALSE(parse_psn("00000g"));
  EXPECT_EQ("000001", format_psn(1));
  EXPECT_EQ("FFFFFF", format_psn(0xffffff));
  EXPECT_EQ("000001", format_psn(0x1000001)); // masked to 24 bits
}

TEST(RdmaRcWire, Cookie)
{
  EXPECT_EQ(0x1a2b3c4du, *parse_cookie("1a2b3c4d"));
  EXPECT_EQ(0xffffffffu, *parse_cookie("FFFFFFFF"));
  EXPECT_FALSE(parse_cookie("00000000"));
  EXPECT_FALSE(parse_cookie("1a2b3c4"));
  EXPECT_FALSE(parse_cookie("1a2b3c4d5"));
  EXPECT_EQ("1A2B3C4D", format_cookie(0x1a2b3c4d));
  EXPECT_EQ("00000001", format_cookie(1));
}

TEST(RdmaRcWire, BareHex)
{
  EXPECT_EQ(0u, *parse_hex("0"));
  EXPECT_EQ(0x7f1234567000ull, *parse_hex("7f1234567000"));
  EXPECT_EQ(UINT64_MAX, *parse_hex("ffffffffffffffff"));
  EXPECT_FALSE(parse_hex(""));
  EXPECT_FALSE(parse_hex("0x10"));
  EXPECT_FALSE(parse_hex("1ffffffffffffffff"));
  EXPECT_EQ("7f1234567000", format_hex(0x7f1234567000ull));
  EXPECT_EQ("0", format_hex(0));
}

TEST(RdmaRcWire, Decimal)
{
  EXPECT_EQ(0u, *parse_decimal("0"));
  EXPECT_EQ(4096u, *parse_decimal("4096"));
  EXPECT_EQ(18446744073709551615ull, *parse_decimal("18446744073709551615"));
  EXPECT_FALSE(parse_decimal("18446744073709551616"));
  EXPECT_FALSE(parse_decimal(""));
  EXPECT_FALSE(parse_decimal("-1"));
  EXPECT_FALSE(parse_decimal("0x10"));
  EXPECT_FALSE(parse_decimal("12 "));
}

TEST(RdmaRcWire, Target)
{
  auto t = parse_target("/bucket/dir/key.bin");
  ASSERT_TRUE(t);
  EXPECT_EQ("bucket", t->bucket);
  EXPECT_EQ("dir/key.bin", t->key);
  EXPECT_EQ("", t->query);

  t = parse_target("/b/k?versionId=abc&partNumber=2");
  ASSERT_TRUE(t);
  EXPECT_EQ("b", t->bucket);
  EXPECT_EQ("k", t->key);
  EXPECT_EQ("versionId=abc&partNumber=2", t->query);

  // percent-encoded segments decode; '+' is literal in a path
  t = parse_target("/my%2Dbucket/a%20b+c%2Fd");
  ASSERT_TRUE(t);
  EXPECT_EQ("my-bucket", t->bucket);
  EXPECT_EQ("a b+c/d", t->key);

  EXPECT_FALSE(parse_target(""));
  EXPECT_FALSE(parse_target("bucket/key"));
  EXPECT_FALSE(parse_target("/bucket"));
  EXPECT_FALSE(parse_target("/bucket/"));
  EXPECT_FALSE(parse_target("//key"));
  EXPECT_FALSE(parse_target("/bucket/key%2"));
  EXPECT_FALSE(parse_target("/bucket/key%zz"));
}

TEST(RdmaRcWire, BuildTargetRoundTrip)
{
  const std::string bucket = "my-bucket";
  const std::string key = "dir one/fi%le~.bin";
  const auto built = build_target(bucket, key, "versionId=v1");
  EXPECT_EQ("/my-bucket/dir%20one/fi%25le~.bin?versionId=v1", built);
  auto t = parse_target(built);
  ASSERT_TRUE(t);
  EXPECT_EQ(bucket, t->bucket);
  EXPECT_EQ(key, t->key);
  EXPECT_EQ("versionId=v1", t->query);
}

TEST(RdmaRcWire, ChecksumHeader)
{
  EXPECT_EQ("CRC64NVME AAAAAAAAAAA=", format_checksum_crc64nvme("AAAAAAAAAAA="));
}

TEST(RdmaRcWire, SignedHeaders)
{
  const std::string auth =
    "AWS4-HMAC-SHA256 Credential=AK/20260929/us-east-1/s3/aws4_request, "
    "SignedHeaders=host;x-amz-date;x-amz-rdma-cookie;x-amz-rdma-token, "
    "Signature=abcd";
  auto list = sigv4_signed_headers(auth);
  ASSERT_TRUE(list);
  EXPECT_EQ("host;x-amz-date;x-amz-rdma-cookie;x-amz-rdma-token", *list);
  EXPECT_TRUE(all_signed(*list, {"x-amz-rdma-cookie", "x-amz-rdma-token"}));
  EXPECT_TRUE(all_signed(*list, {}));
  EXPECT_FALSE(all_signed(*list, {"x-amz-rdma-psn"}));
  // exact entries only, not substrings
  EXPECT_FALSE(all_signed(*list, {"x-amz-rdma"}));
  EXPECT_FALSE(all_signed(*list, {"amz-date"}));

  EXPECT_FALSE(sigv4_signed_headers("AWS AK:signature"));
  EXPECT_FALSE(sigv4_signed_headers("AWS4-HMAC-SHA256 Credential=x, Signature=y"));
  // the list may be the last component
  auto last = sigv4_signed_headers("AWS4-HMAC-SHA256 Credential=x,SignedHeaders=host;x-amz-rdma-op");
  ASSERT_TRUE(last);
  EXPECT_TRUE(all_signed(*last, {"x-amz-rdma-op"}));
}

TEST(RdmaRcWire, LandedRanges)
{
  landed_ranges r;
  EXPECT_EQ(0u, r.prefix());
  r.add(100, 50);           // [100,150)
  EXPECT_EQ(0u, r.prefix());
  r.add(0, 60);             // [0,60)
  EXPECT_EQ(60u, r.prefix());
  EXPECT_EQ(2u, r.count());
  r.add(60, 40);            // bridges to [0,150)
  EXPECT_EQ(150u, r.prefix());
  EXPECT_EQ(1u, r.count());
  r.add(10, 20);            // inside
  EXPECT_EQ(150u, r.prefix());
  EXPECT_EQ(1u, r.count());
  r.add(300, 10);
  r.add(200, 10);
  r.add(150, 150);          // swallows [200,210) and touches [300,310)
  EXPECT_EQ(310u, r.prefix());
  EXPECT_EQ(1u, r.count());
  r.add(400, 0);            // empty adds nothing
  EXPECT_EQ(1u, r.count());
}

TEST(RdmaRcWire, PlanPushesStreaming)
{
  push_policy p;
  p.min_push = 100;
  p.max_write = 250;
  p.tail_hold = 10;
  // below min_push: wait
  EXPECT_TRUE(plan_pushes(50, 0, 1000, false, p)->empty());
  // enough landed: push it, split at max_write
  auto w = *plan_pushes(600, 0, 1000, false, p);
  ASSERT_EQ(3u, w.size());
  EXPECT_EQ((push_t{0, 250, false}), w[0]);
  EXPECT_EQ((push_t{250, 250, false}), w[1]);
  EXPECT_EQ((push_t{500, 100, false}), w[2]);
  // the tail is held back even when everything landed
  w = *plan_pushes(1000, 600, 1000, false, p);
  ASSERT_EQ(2u, w.size());
  EXPECT_EQ((push_t{600, 250, false}), w[0]);
  EXPECT_EQ((push_t{850, 140, false}), w[1]);
  // reaching the tail point pushes even a short run
  w = *plan_pushes(1000, 950, 1000, false, p);
  ASSERT_EQ(1u, w.size());
  EXPECT_EQ((push_t{950, 40, false}), w[0]);
  // nothing new
  EXPECT_TRUE(plan_pushes(1000, 990, 1000, false, p)->empty());
  // an object smaller than the tail streams nothing
  EXPECT_TRUE(plan_pushes(8, 0, 8, false, p)->empty());
}

TEST(RdmaRcWire, PlanPushesFinal)
{
  push_policy p;
  p.max_write = 250;
  p.tail_hold = 10;
  auto w = plan_pushes(1000, 990, 1000, true, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(1u, w->size());
  EXPECT_EQ((push_t{990, 10, true}), (*w)[0]);
  // nothing streamed: split, immediate on the last write only
  w = plan_pushes(600, 0, 600, true, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(3u, w->size());
  EXPECT_EQ((push_t{0, 250, false}), (*w)[0]);
  EXPECT_EQ((push_t{250, 250, false}), (*w)[1]);
  EXPECT_EQ((push_t{500, 100, true}), (*w)[2]);
  // everything pushed already: a zero-length write carries the cookie
  w = plan_pushes(600, 600, 600, true, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(1u, w->size());
  EXPECT_EQ((push_t{600, 0, true}), (*w)[0]);
  // a hole means the transfer is incomplete
  EXPECT_FALSE(plan_pushes(599, 0, 600, true, p));
}

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"
#include "common/crc64nvme.h"
#include "messages/MOSDOpReply.h"
#include "osdc/Objecter.h"
#include "rgw/rgw_rdma_fence.h"

#include <optional>
#include <vector>

#include "gtest/gtest.h"

// Pin the rdma delivery descriptor wire format (carried as a per-op
// vector after the v9 MOSDOp fields, aligned with the ops). Any change
// here is a wire format change and needs a struct version bump.
TEST(RdmaDelivery, WireFormat)
{
  ceph::rdma::delivery_t d;
  d.token = "deadbeef:1000:x";
  d.base_offset = 0xa1b2c3d4e5f60718ull;
  d.flags = 0;

  bufferlist bl;
  encode(d, bl);

  // exact bytes: ENCODE_START(1,1) header [u8 v, u8 compat, le32 len],
  // le32 token length + token bytes, le64 base_offset, le32 flags
  static const unsigned char expected_bytes[] = {
    0x01, 0x01, 0x1f, 0x00, 0x00, 0x00,              // struct v1, compat 1, len 31
    0x0f, 0x00, 0x00, 0x00,                          // token length (le32)
    'd', 'e', 'a', 'd', 'b', 'e', 'e', 'f', ':',
    '1', '0', '0', '0', ':', 'x',                    // token
    0x18, 0x07, 0xf6, 0xe5, 0xd4, 0xc3, 0xb2, 0xa1,  // base_offset (le64)
    0x00, 0x00, 0x00, 0x00,                          // flags (le32)
  };
  bufferlist expected;
  expected.append(reinterpret_cast<const char*>(expected_bytes),
                  sizeof(expected_bytes));
  EXPECT_TRUE(bl.contents_equal(expected))
    << "rdma delivery descriptor layout changed; this is a wire format break";

  // and it round-trips
  ceph::rdma::delivery_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_EQ(d.token, out.token);
  EXPECT_EQ(d.base_offset, out.base_offset);
  EXPECT_EQ(d.flags, out.flags);
  EXPECT_TRUE(p.end());
}

TEST(RdmaDelivery, OobResultWireFormat)
{
  ceph::rdma::oob_result_t r;
  r.bytes = 0x0102030405060708ull;
  r.crc64 = 0xae8b14860a799888ull;
  r.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME;

  r.crc32c = 0xe3069283u;

  bufferlist bl;
  encode(r, bl);
  // ENCODE_START(3,1) header, le64 bytes, le64 crc64, le32 flags,
  // then the v2 ranges vector (le32 count, empty here), then the v3
  // crc32c (le32)
  static const unsigned char expected_bytes[] = {
    0x03, 0x01, 0x1c, 0x00, 0x00, 0x00,              // v3, compat 1, len 28
    0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // bytes (le64)
    0x88, 0x98, 0x79, 0x0a, 0x86, 0x14, 0x8b, 0xae,  // crc64 (le64)
    0x01, 0x00, 0x00, 0x00,                          // flags (le32)
    0x00, 0x00, 0x00, 0x00,                          // ranges: 0 entries
    0x83, 0x92, 0x06, 0xe3,                          // crc32c (le32)
  };
  bufferlist expected;
  expected.append(reinterpret_cast<const char*>(expected_bytes),
                  sizeof(expected_bytes));
  EXPECT_TRUE(bl.contents_equal(expected))
    << "oob result layout changed; this is a wire format break";

  ceph::rdma::oob_result_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_EQ(r.bytes, out.bytes);
  EXPECT_EQ(r.crc64, out.crc64);
  EXPECT_EQ(r.flags, out.flags);
  EXPECT_EQ(r.crc32c, out.crc32c);
  EXPECT_TRUE(p.end());
}

// What an OSD of the previous release sends: a v2 result whose ranges
// are v1, without CRC-32C values. It decodes with none.
TEST(RdmaDelivery, OobResultFromOlderOsd)
{
  bufferlist bl;
  ENCODE_START(2, 1, bl);
  encode(uint64_t(4096), bl);                 // bytes
  encode(uint64_t(0x1234), bl);               // crc64
  encode(ceph::rdma::oob_result_t::FLAG_CRC64NVME |
         ceph::rdma::oob_result_t::FLAG_RANGES, bl);
  encode(uint32_t(1), bl);                    // one range, v1
  {
    ENCODE_START(1, 1, bl);
    encode(uint64_t(0), bl);
    encode(uint64_t(4096), bl);
    encode(uint64_t(0x1234), bl);
    ENCODE_FINISH(bl);
  }
  ENCODE_FINISH(bl);

  ceph::rdma::oob_result_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_TRUE(p.end());
  EXPECT_EQ(4096u, out.bytes);
  EXPECT_EQ(0x1234u, out.crc64);
  EXPECT_EQ(0u, out.crc32c);
  EXPECT_FALSE(out.flags & ceph::rdma::oob_result_t::FLAG_CRC32C);
  ASSERT_EQ(1u, out.ranges.size());
  EXPECT_EQ(0x1234u, out.ranges[0].crc64);
  EXPECT_EQ(0u, out.ranges[0].crc32c);
}

// The canonical CRC-32C is S3's, and two of them combine into the CRC of
// the concatenation, whatever the lengths and the buffer boundaries.
TEST(RdmaDelivery, Crc32cCanonicalAndCombine)
{
  bufferlist check;
  check.append("123456789");
  EXPECT_EQ(0xe3069283u, ceph::rdma::crc32c_canonical(check));
  EXPECT_EQ(0u, ceph::rdma::crc32c_canonical(bufferlist()));

  std::string data;
  for (int i = 0; i < 100000; ++i) {
    data += static_cast<char>((i * 131 + i / 7) & 0xff);
  }
  bufferlist whole;
  // several buffers, so that bufferlist's per-buffer CRC caching is used
  for (size_t o = 0; o < data.size(); o += 30001) {
    whole.append(data.data() + o, std::min<size_t>(30001, data.size() - o));
  }
  const uint32_t expect = ceph::rdma::crc32c_canonical(whole);
  for (size_t cut : {size_t(0), size_t(1), size_t(4095), size_t(4096),
                     size_t(65537), data.size() - 1, data.size()}) {
    bufferlist a, b;
    a.append(data.data(), cut);
    b.append(data.data() + cut, data.size() - cut);
    EXPECT_EQ(expect, ceph::rdma::crc32c_combine(
                ceph::rdma::crc32c_canonical(a),
                ceph::rdma::crc32c_canonical(b), b.length()))
      << "cut at " << cut;
  }
}

TEST(RdmaDelivery, FoldCrc32cRanges)
{
  // as FoldCrc64Ranges: interleaved chunks of two EC shards, out of order
  std::string data;
  for (int c = 0; c < 6; ++c) {
    data += std::string(1000, static_cast<char>('a' + c));
  }
  bufferlist whole;
  whole.append(data);
  const uint32_t expect = ceph::rdma::crc32c_canonical(whole);

  std::vector<ceph::rdma::crc_range_t> ranges;
  for (int c : {4, 0, 2, 5, 1, 3}) {
    bufferlist part;
    part.append(data.data() + c * 1000, 1000);
    ranges.push_back({uint64_t(c) * 1000, 1000, ceph::crc64nvme(part),
                      ceph::rdma::crc32c_canonical(part)});
  }
  auto folded = ceph::rdma::fold_crc32c_ranges(ranges);
  ASSERT_TRUE(folded.has_value());
  EXPECT_EQ(expect, *folded);
  // the same ranges still fold the CRC-64/NVME too
  auto folded64 = ceph::rdma::fold_crc64_ranges(ranges);
  ASSERT_TRUE(folded64.has_value());
  EXPECT_EQ(ceph::crc64nvme(whole), *folded64);

  auto gapped = ranges;
  gapped.erase(gapped.begin() + 2);
  EXPECT_FALSE(ceph::rdma::fold_crc32c_ranges(gapped).has_value());
  EXPECT_FALSE(ceph::rdma::fold_crc32c_ranges({}).has_value());
}

TEST(RdmaDelivery, PerOpVectorRoundTrip)
{
  // the descriptors ride as a per-op vector on the MOSDOp tail: empty
  // when nothing is requested, otherwise one entry per op where an
  // empty token means "inline for this op"
  std::vector<ceph::rdma::delivery_t> none;
  std::vector<ceph::rdma::delivery_t> some = {
    ceph::rdma::delivery_t{},                          // op 0: inline
    ceph::rdma::delivery_t{"aa:bb:opaque", 42, 0},     // op 1
    ceph::rdma::delivery_t{"aa:bb:opaque", 4096,
			   ceph::rdma::delivery_t::FLAG_CRC64NVME},  // op 2
  };
  EXPECT_TRUE(some[0].empty());
  EXPECT_FALSE(some[1].empty());

  bufferlist bl;
  encode(none, bl);
  encode(some, bl);

  std::vector<ceph::rdma::delivery_t> out1, out2;
  auto p = bl.cbegin();
  decode(out1, p);
  decode(out2, p);
  EXPECT_TRUE(out1.empty());
  ASSERT_EQ(3u, out2.size());
  EXPECT_TRUE(out2[0].empty());
  EXPECT_EQ("aa:bb:opaque", out2[1].token);
  EXPECT_EQ(42u, out2[1].base_offset);
  EXPECT_EQ(0u, out2[1].flags);
  EXPECT_EQ(4096u, out2[2].base_offset);
  EXPECT_EQ(ceph::rdma::delivery_t::FLAG_CRC64NVME, out2[2].flags);
  EXPECT_TRUE(p.end());
}

// The descriptors ride on a v9 MOSDOp after its v9 fields, and only
// when some op carries one: an OSD that does not know them (upstream
// Umbrella, which advertises SERVER_UMBRELLA too) stops decoding before
// them and serves the request inline, and a request without any is the
// plain v9 encoding. The reply's results trail a v8 MOSDOpReply alike.
TEST(RdmaDelivery, MessagesCarryRdmaFieldsAsSuffix)
{
  const hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0x1234, 1, "");
  spg_t pgid(pg_t(0x1234, 1));
  const std::vector<ceph::rdma::delivery_t> deliveries = {
    ceph::rdma::delivery_t{},                          // op 0: inline
    ceph::rdma::delivery_t{"aa:bb:opaque", 4096,
                           ceph::rdma::delivery_t::FLAG_CRC64NVME},
  };

  // what a client sends, with and without descriptors (the Objecter's
  // MOSDOp; an OSD decodes into the std::vector one)
  using ClientMOSDOp = _mosdop::MOSDOp<osdc_opvec>;
  auto encode_op = [&](bool with_deliveries) {
    auto m = ceph::make_message<ClientMOSDOp>(
      1, 42, hobj, pgid, 10, CEPH_OSD_FLAG_READ, CEPH_FEATURES_ALL);
    m->ops.resize(2);
    m->ops[0].op.op = CEPH_OSD_OP_STAT;
    m->ops[1].op.op = CEPH_OSD_OP_READ;
    m->ops[1].op.extent.offset = 0;
    m->ops[1].op.extent.length = 4096;
    if (with_deliveries) {
      m->set_rdma_deliveries(std::vector(deliveries));
    }
    m->encode_payload(CEPH_FEATURES_ALL);
    return m;
  };
  auto plain = encode_op(false);
  auto with = encode_op(true);
  EXPECT_EQ(9, plain->get_header().version);
  EXPECT_EQ(9, with->get_header().version);

  // the plain request, then the descriptors and nothing else
  bufferlist tail;
  encode(deliveries, tail);
  const auto plain_len = plain->get_payload().length();
  ASSERT_EQ(plain_len + tail.length(), with->get_payload().length());
  bufferlist prefix, suffix;
  prefix.substr_of(with->get_payload(), 0, plain_len);
  suffix.substr_of(with->get_payload(), plain_len, tail.length());
  EXPECT_TRUE(prefix.contents_equal(plain->get_payload()));
  EXPECT_TRUE(suffix.contents_equal(tail));

  // an OSD of this release decodes them back, and finds none in a
  // plain request
  auto decode_op = [](const ClientMOSDOp& sent) {
    auto m = ceph::make_message<MOSDOp>();
    m->set_header(sent.get_header());
    bufferlist bl = sent.get_payload();
    m->set_payload(bl);
    m->decode_payload();
    m->finish_decode();
    return m;
  };
  EXPECT_FALSE(decode_op(*plain)->has_rdma_delivery());
  auto req = decode_op(*with);
  ASSERT_EQ(2u, req->ops.size());
  EXPECT_EQ(4096u, req->ops[1].op.extent.length);
  ASSERT_EQ(2u, req->get_rdma_deliveries().size());
  EXPECT_TRUE(req->get_rdma_deliveries()[0].empty());
  EXPECT_EQ("aa:bb:opaque", req->get_rdma_deliveries()[1].token);
  EXPECT_EQ(4096u, req->get_rdma_deliveries()[1].base_offset);
  EXPECT_EQ(ceph::rdma::delivery_t::FLAG_CRC64NVME,
            req->get_rdma_deliveries()[1].flags);

  // and its reply
  std::vector<ceph::rdma::oob_result_t> results(2);
  results[1].bytes = 4096;
  results[1].crc64 = 0x1234;
  results[1].flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
                     ceph::rdma::oob_result_t::FLAG_LANDED;
  auto encode_reply = [&](bool with_results) {
    auto r = ceph::make_message<MOSDOpReply>(
      req.get(), 0, 10, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, false);
    if (with_results) {
      r->set_oob_results(std::vector(results));
    }
    r->encode_payload(CEPH_FEATURES_ALL);
    return r;
  };
  auto plain_reply = encode_reply(false);
  auto with_reply = encode_reply(true);
  EXPECT_EQ(8, plain_reply->get_header().version);
  EXPECT_EQ(8, with_reply->get_header().version);

  bufferlist rtail;
  encode(results, rtail);
  const auto plain_reply_len = plain_reply->get_payload().length();
  ASSERT_EQ(plain_reply_len + rtail.length(),
            with_reply->get_payload().length());
  bufferlist rprefix, rsuffix;
  rprefix.substr_of(with_reply->get_payload(), 0, plain_reply_len);
  rsuffix.substr_of(with_reply->get_payload(), plain_reply_len,
                    rtail.length());
  EXPECT_TRUE(rprefix.contents_equal(plain_reply->get_payload()));
  EXPECT_TRUE(rsuffix.contents_equal(rtail));

  auto decode_reply = [](const MOSDOpReply& sent) {
    auto r = ceph::make_message<MOSDOpReply>();
    r->set_header(sent.get_header());
    bufferlist bl = sent.get_payload();
    r->set_payload(bl);
    r->decode_payload();
    return r;
  };
  EXPECT_TRUE(decode_reply(*plain_reply)->get_oob_results().empty());
  auto reply = decode_reply(*with_reply);
  ASSERT_EQ(2u, reply->get_oob_results().size());
  EXPECT_EQ(0u, reply->get_oob_results()[0].bytes);
  EXPECT_EQ(4096u, reply->get_oob_results()[1].bytes);
  EXPECT_EQ(0x1234u, reply->get_oob_results()[1].crc64);
  EXPECT_EQ(results[1].flags, reply->get_oob_results()[1].flags);
}

// Earlier development builds sent every request to a SERVER_UMBRELLA
// peer as v10: the v9 encoding with the descriptor vector always
// appended. Such a request still decodes, so that those daemons can be
// restarted one at a time, but is served inline: its sender reads
// results only from a v9 reply, and an OSD of this release replies v8.
TEST(RdmaDelivery, EarlierV10RequestIsServedInline)
{
  const hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0x1234, 1, "");
  spg_t pgid(pg_t(0x1234, 1));
  const osd_reqid_t reqid(entity_name_t::CLIENT(7), 1, 42);
  using ClientMOSDOp = _mosdop::MOSDOp<osdc_opvec>;
  auto sent = ceph::make_message<ClientMOSDOp>(
    1, 42, hobj, pgid, 10, CEPH_OSD_FLAG_READ, CEPH_FEATURES_ALL);
  sent->set_reqid(reqid);
  sent->ops.resize(1);
  sent->ops[0].op.op = CEPH_OSD_OP_READ;
  sent->ops[0].op.extent.offset = 0;
  sent->ops[0].op.extent.length = 4096;
  sent->encode_payload(CEPH_FEATURES_ALL);
  ASSERT_EQ(9, sent->get_header().version);

  for (const auto& deliveries : {
	 std::vector<ceph::rdma::delivery_t>{},
	 std::vector<ceph::rdma::delivery_t>{
	   ceph::rdma::delivery_t{"aa:bb:opaque", 0, 0}}}) {
    bufferlist bl = sent->get_payload();
    encode(deliveries, bl);
    auto header = sent->get_header();
    header.version = 10;
    auto m = ceph::make_message<MOSDOp>();
    m->set_header(header);
    m->set_payload(bl);
    m->decode_payload();
    EXPECT_EQ(pgid, m->get_spg());
    EXPECT_EQ(10u, m->get_map_epoch());
    EXPECT_EQ(CEPH_OSD_FLAG_READ, m->get_flags());
    EXPECT_EQ(reqid, m->get_reqid());
    m->finish_decode();
    EXPECT_EQ(object_t("obj"), m->get_oid());
    ASSERT_EQ(1u, m->ops.size());
    EXPECT_EQ(CEPH_OSD_OP_READ, m->ops[0].op.op);
    EXPECT_EQ(4096u, m->ops[0].op.extent.length);
    EXPECT_FALSE(m->has_rdma_delivery());
  }
}

TEST(RdmaDelivery, Crc64ValidityIsSeparateFromCombinability)
{
  // distinct bits: a scattered placement reports a checksum of the
  // bytes it moved without claiming it folds with its neighbours
  static_assert(ceph::rdma::oob_result_t::FLAG_CRC64NVME !=
		ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE);

  ceph::rdma::oob_result_t scattered;
  scattered.bytes = 1048576;
  scattered.crc64 = 0x0123456789abcdefull;
  scattered.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME;
  EXPECT_TRUE(scattered.flags & ceph::rdma::oob_result_t::FLAG_CRC64NVME);
  EXPECT_FALSE(scattered.flags &
	       ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE);

  ceph::rdma::oob_result_t linear = scattered;
  linear.flags |= ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE;

  // the extra bit rides in the existing flags field, so neither
  // combination changes the encoded length
  bufferlist a, b;
  encode(scattered, a);
  encode(linear, b);
  EXPECT_EQ(a.length(), b.length());

  ceph::rdma::oob_result_t out;
  auto p = b.cbegin();
  decode(out, p);
  EXPECT_EQ(linear.crc64, out.crc64);
  EXPECT_TRUE(out.flags & ceph::rdma::oob_result_t::FLAG_CRC64NVME);
  EXPECT_TRUE(out.flags & ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE);
  EXPECT_TRUE(p.end());
}

TEST(RdmaDelivery, OobResultDecodesV1)
{
  // a v1 result (no ranges) from an older peer decodes with an empty
  // ranges vector rather than reading past the struct
  static const unsigned char v1_bytes[] = {
    0x01, 0x01, 0x14, 0x00, 0x00, 0x00,              // v1, compat 1, len 20
    0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // bytes (le64)
    0x88, 0x98, 0x79, 0x0a, 0x86, 0x14, 0x8b, 0xae,  // crc64 (le64)
    0x01, 0x00, 0x00, 0x00,                          // flags (le32)
  };
  bufferlist bl;
  bl.append(reinterpret_cast<const char*>(v1_bytes), sizeof(v1_bytes));
  ceph::rdma::oob_result_t out;
  out.ranges = {{0, 1, 2}};  // must be cleared, not left over
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_TRUE(p.end());
  EXPECT_EQ(0x0102030405060708ull, out.bytes);
  EXPECT_EQ(0xae8b14860a799888ull, out.crc64);
  EXPECT_EQ(ceph::rdma::oob_result_t::FLAG_CRC64NVME, out.flags);
  EXPECT_TRUE(out.ranges.empty());
}

TEST(RdmaDelivery, OobResultRangesRoundTrip)
{
  ceph::rdma::oob_result_t r;
  r.bytes = 3 * 65536;
  r.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
	    ceph::rdma::oob_result_t::FLAG_CRC64_RANGES;
  r.ranges = {{0, 65536, 1}, {131072, 65536, 2}, {262144, 65536, 3}};
  r.crc64 = 42;

  bufferlist bl;
  encode(r, bl);
  ceph::rdma::oob_result_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_TRUE(p.end());
  EXPECT_EQ(r.bytes, out.bytes);
  EXPECT_EQ(r.flags, out.flags);
  ASSERT_EQ(3u, out.ranges.size());
  EXPECT_EQ(r.ranges, out.ranges);

  // an empty result round-trips with no ranges
  ceph::rdma::oob_result_t empty, eout;
  bufferlist ebl;
  encode(empty, ebl);
  auto q = ebl.cbegin();
  decode(eout, q);
  EXPECT_TRUE(q.end());
  EXPECT_EQ(0u, eout.bytes);
  EXPECT_TRUE(eout.ranges.empty());
}

TEST(RdmaDelivery, FoldCrc64Ranges)
{
  // three chunks of a 6-chunk logical range, as two EC shards would
  // place them: shard 0 holds chunks 0,2,4 and shard 1 holds 1,3,5
  std::string data;
  for (int c = 0; c < 6; ++c) {
    data += std::string(1000, static_cast<char>('a' + c));
  }
  bufferlist whole;
  whole.append(data);
  const uint64_t expect = ceph::crc64nvme(whole);

  std::vector<ceph::rdma::crc_range_t> ranges;
  for (int c : {4, 0, 2, 5, 1, 3}) {  // deliberately out of order
    bufferlist part;
    part.append(data.data() + c * 1000, 1000);
    ranges.push_back({uint64_t(c) * 1000, 1000, ceph::crc64nvme(part)});
  }
  auto folded = ceph::rdma::fold_crc64_ranges(ranges);
  ASSERT_TRUE(folded.has_value());
  EXPECT_EQ(expect, *folded);

  // a single range folds to itself
  auto one = ceph::rdma::fold_crc64_ranges({ranges[1]});
  ASSERT_TRUE(one.has_value());
  EXPECT_EQ(ranges[1].crc64, *one);

  // a gap or an overlap is not a contiguous extent
  auto gapped = ranges;
  gapped.erase(gapped.begin() + 2);
  EXPECT_FALSE(ceph::rdma::fold_crc64_ranges(gapped).has_value());
  auto overlapped = ranges;
  overlapped[0].ofs -= 1;
  EXPECT_FALSE(ceph::rdma::fold_crc64_ranges(overlapped).has_value());
  EXPECT_FALSE(ceph::rdma::fold_crc64_ranges({}).has_value());
}

// An op's out-of-band result reaches its caller in the form the caller
// asked for. librados takes the callback form; the split-read completion
// used to copy the aggregate through the pointer form only, so every
// shard-direct passthrough read told librados (and RGW) that nothing had
// been delivered. Both the reply path and the split completion go
// through rdma_oob_wanted() and deliver_rdma_oob_result(); this pins
// what those promise, on an Op built the way prepare_read_op() builds it.
TEST(RdmaDelivery, ResultReachesEitherForm)
{
  ::ObjectOperation op;
  ceph::buffer::list bl0, bl1, bl2;
  int rv0 = 0, rv1 = 0, rv2 = 0;

  ceph::rdma::oob_result_t by_pointer;
  op.read(0, 4096, &bl0, &rv0, nullptr);
  op.set_rdma_delivery("t0", 0, 0, &by_pointer);

  std::optional<ceph::rdma::oob_result_t> by_callback;
  int calls = 0;
  op.read(4096, 4096, &bl1, &rv1, nullptr);
  op.set_rdma_delivery("t1", 4096,
                       ceph::rdma::delivery_t::FLAG_CRC64NVME,
                       [&](const ceph::rdma::oob_result_t& r) {
                         by_callback = r;
                         ++calls;
                       });

  op.read(8192, 4096, &bl2, &rv2, nullptr);  // no descriptor

  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), 0, (Context*)nullptr,
                             nullptr);
  o->rdma_delivery.swap(op.rdma_delivery);
  o->rdma_oob_result.swap(op.rdma_oob_result);
  o->rdma_oob_handler.swap(op.rdma_oob_handler);

  EXPECT_TRUE(Objecter::rdma_oob_wanted(o, 0));
  // the callback form alone: the case the split completion skipped
  EXPECT_TRUE(Objecter::rdma_oob_wanted(o, 1));
  EXPECT_FALSE(Objecter::rdma_oob_wanted(o, 2));
  EXPECT_FALSE(Objecter::rdma_oob_wanted(o, 7));

  ceph::rdma::oob_result_t r0;
  r0.bytes = 4096;
  Objecter::deliver_rdma_oob_result(o, 0, r0);
  EXPECT_EQ(4096u, by_pointer.bytes);

  ceph::rdma::oob_result_t r1;
  r1.bytes = 4096;
  r1.crc64 = 0x1234;
  r1.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
             ceph::rdma::oob_result_t::FLAG_CRC64_RANGES;
  r1.ranges = {{4096, 2048, 0x11}, {6144, 2048, 0x22}};
  Objecter::deliver_rdma_oob_result(o, 1, r1);
  ASSERT_TRUE(by_callback.has_value());
  EXPECT_EQ(1, calls);
  EXPECT_EQ(4096u, by_callback->bytes);
  EXPECT_EQ(0x1234u, by_callback->crc64);
  EXPECT_EQ(r1.flags, by_callback->flags);
  ASSERT_EQ(2u, by_callback->ranges.size());
  EXPECT_EQ(6144u, by_callback->ranges[1].ofs);

  // the callback is one-shot: a later delivery for the same op (a
  // resend's reply) does not call it again
  Objecter::deliver_rdma_oob_result(o, 1, r1);
  EXPECT_EQ(1, calls);
  EXPECT_FALSE(Objecter::rdma_oob_wanted(o, 1));

  // an op with no descriptor and no result asked for is left alone
  Objecter::deliver_rdma_oob_result(o, 2, r1);

  o->put();
}

// A write whose payload the primary pulls out of client memory carries no
// data, only its length and a pull descriptor, and the builders of write
// ops hand its descriptor and result handler to the Op as read builders
// do. They once did not: the OSD then saw a write without data, and
// failed it.
TEST(RdmaDelivery, PulledWriteCarriesItsDescriptor)
{
  ::ObjectOperation op;
  op.create(false);
  op.write_full_pulled(4 << 20);
  std::optional<ceph::rdma::oob_result_t> got;
  op.set_rdma_delivery("t", 8 << 20,
                       ceph::rdma::delivery_t::FLAG_PULL |
                       ceph::rdma::delivery_t::FLAG_CRC64NVME,
                       [&](const ceph::rdma::oob_result_t& r) { got = r; });
  ASSERT_EQ(2u, op.ops.size());
  EXPECT_EQ(CEPH_OSD_OP_WRITEFULL, op.ops[1].op.op);
  EXPECT_EQ(uint64_t(4 << 20), op.ops[1].op.extent.length);
  EXPECT_EQ(0u, op.ops[1].indata.length());

  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), CEPH_OSD_FLAG_WRITE,
                             (Context*)nullptr, nullptr);
  Objecter::take_rdma(o, op);
  ASSERT_TRUE(o->has_rdma_delivery());
  EXPECT_TRUE(o->rdma_delivery[0].empty());
  EXPECT_TRUE(o->rdma_delivery[1].is_pull());
  EXPECT_EQ(uint64_t(8 << 20), o->rdma_delivery[1].base_offset);
  EXPECT_FALSE(op.has_rdma_delivery());

  // the pulled bytes and their checksum reach the caller
  EXPECT_TRUE(Objecter::rdma_oob_wanted(o, 1));
  ceph::rdma::oob_result_t r;
  r.bytes = 4 << 20;
  r.crc64 = 0xabcdef;
  r.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
            ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE;
  Objecter::deliver_rdma_oob_result(o, 1, r);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(uint64_t(4 << 20), got->bytes);
  EXPECT_EQ(0xabcdefu, got->crc64);
  o->put();
}

// A result for an op sent more than once is marked resent: an earlier
// attempt may have started a transfer the reply knows nothing of, and a
// caller must then fence its window. One sent once is not.
TEST(RdmaDelivery, ResentResultsAreMarked)
{
  ::ObjectOperation op;
  ceph::buffer::list bl;
  int rv = 0;
  ceph::rdma::oob_result_t got;
  op.read(0, 4096, &bl, &rv, nullptr);
  op.set_rdma_delivery("t0", 0, 0, &got);
  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), 0, (Context*)nullptr,
                             nullptr);
  o->rdma_delivery.swap(op.rdma_delivery);
  o->rdma_oob_result.swap(op.rdma_oob_result);

  ceph::rdma::oob_result_t declined;
  declined.flags = ceph::rdma::oob_result_t::FLAG_DECLINED;
  o->attempts = 1;
  Objecter::deliver_rdma_oob_result(o, 0, declined);
  EXPECT_EQ(ceph::rdma::oob_result_t::FLAG_DECLINED, got.flags);

  o->attempts = 2;
  Objecter::deliver_rdma_oob_result(o, 0, declined);
  EXPECT_EQ(ceph::rdma::oob_result_t::FLAG_DECLINED |
            ceph::rdma::oob_result_t::FLAG_RESENT, got.flags);

  // a resend whose earlier attempts were all answered declined or
  // landed went out vouched for, and its result is not resent
  o->rdma_retry_settled = true;
  Objecter::deliver_rdma_oob_result(o, 0, declined);
  EXPECT_EQ(ceph::rdma::oob_result_t::FLAG_DECLINED, got.flags);
  o->put();
}

// An attempt that went out without its descriptors - withheld while the
// cluster's require_osd_release is below umbrella - moved nothing out of
// band, and its results say declined, so a caller need not fence its
// window. A resend withheld again is not resent; one withheld after an
// attempt that went out unanswered is.
TEST(RdmaDelivery, WithheldDescriptorsReadDeclined)
{
  using R = ceph::rdma::oob_result_t;
  ::ObjectOperation op;
  ceph::buffer::list bl0, bl1;
  int rv0 = 0, rv1 = 0;
  R got;
  op.read(0, 4096, &bl0, &rv0, nullptr);
  op.set_rdma_delivery("t0", 0, 0, &got);
  op.read(4096, 4096, &bl1, &rv1, nullptr);  // no descriptor
  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), 0, (Context*)nullptr,
                             nullptr);
  Objecter::take_rdma(o, op);

  // attempts as _prepare_osd_op() counts them before stamping
  o->attempts = 1;
  EXPECT_TRUE(Objecter::stamp_rdma_attempt(o, false).empty());
  EXPECT_TRUE(o->rdma_withheld);
  // the OSD saw no descriptors, so its reply carries no results
  Objecter::deliver_rdma_oob_results(o, {});
  EXPECT_EQ(R::FLAG_DECLINED, got.flags);
  EXPECT_EQ(0u, got.bytes);

  o->attempts = 2;
  EXPECT_TRUE(Objecter::stamp_rdma_attempt(o, false).empty());
  Objecter::deliver_rdma_oob_results(o, {});
  EXPECT_EQ(R::FLAG_DECLINED, got.flags);

  // once every OSD may honor them, the descriptors go out, vouched for
  // as no earlier attempt carried any, and a reply without results
  // reads as before
  o->attempts = 3;
  auto d = Objecter::stamp_rdma_attempt(o, true);
  EXPECT_FALSE(o->rdma_withheld);
  ASSERT_EQ(2u, d.size());
  EXPECT_EQ("t0", d[0].token);
  EXPECT_TRUE(d[0].flags & ceph::rdma::delivery_t::FLAG_PRIOR_SETTLED);
  EXPECT_TRUE(d[1].empty());
  Objecter::deliver_rdma_oob_results(o, {});
  EXPECT_EQ(0u, got.flags);

  // that attempt had no settled reply: a later withheld one is declined
  // for itself, but resent
  o->attempts = 4;
  EXPECT_TRUE(Objecter::stamp_rdma_attempt(o, false).empty());
  Objecter::deliver_rdma_oob_results(o, {});
  EXPECT_EQ(R::FLAG_DECLINED | R::FLAG_RESENT, got.flags);

  // an op without a descriptor carries nothing to withhold
  ::ObjectOperation plain;
  plain.read(0, 4096, &bl0, &rv0, nullptr);
  auto* p = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(plain.ops), 0, (Context*)nullptr,
                             nullptr);
  Objecter::take_rdma(p, plain);
  p->attempts = 1;
  EXPECT_TRUE(Objecter::stamp_rdma_attempt(p, false).empty());
  EXPECT_FALSE(p->rdma_withheld);
  p->put();
  o->put();
}

// FLAG_PRIOR_SETTLED is the client library's to set, from the attempts it
// sent, and the OSD keeps a resend inline without it. A request that
// carries it already (librados's RDMA_DELIVERY_COMBINABLE result flag has
// its value) goes out without it, on a first attempt and on a resend of
// an unanswered one, and so cannot switch off that guard. A resend of a
// settled attempt carries it, the caller's own bits kept.
TEST(RdmaDelivery, OnlyTheClientLibraryVouchesForEarlierAttempts)
{
  using D = ceph::rdma::delivery_t;
  auto make_op = [](uint32_t flags) {
    ::ObjectOperation op;
    op.write_full_pulled(4096);
    op.set_rdma_delivery("t0", 0, flags | D::FLAG_PULL, nullptr);
    auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                               std::move(op.ops), CEPH_OSD_FLAG_WRITE,
                               (Context*)nullptr, nullptr);
    Objecter::take_rdma(o, op);
    return o;
  };

  auto* o = make_op(D::FLAG_CRC64NVME | D::FLAG_PRIOR_SETTLED);
  o->attempts = 1;
  auto d = Objecter::stamp_rdma_attempt(o, true);
  ASSERT_EQ(1u, d.size());
  EXPECT_EQ(D::FLAG_CRC64NVME | D::FLAG_PULL, d[0].flags);

  // no reply came: a write of that attempt may still land, and its
  // resend goes out unvouched
  o->attempts = 2;
  d = Objecter::stamp_rdma_attempt(o, true);
  ASSERT_EQ(1u, d.size());
  EXPECT_EQ(D::FLAG_CRC64NVME | D::FLAG_PULL, d[0].flags);
  EXPECT_FALSE(ceph::rdma::prior_attempts_settled(d));
  o->put();

  auto* s = make_op(D::FLAG_CRC32C);
  s->attempts = 1;
  d = Objecter::stamp_rdma_attempt(s, true);
  ASSERT_EQ(1u, d.size());
  EXPECT_EQ(D::FLAG_CRC32C | D::FLAG_PULL, d[0].flags);
  // its reply declined the op, as handle_osd_op_reply() records
  s->rdma_attempt_unsettled = false;
  s->attempts = 2;
  d = Objecter::stamp_rdma_attempt(s, true);
  ASSERT_EQ(1u, d.size());
  EXPECT_EQ(D::FLAG_CRC32C | D::FLAG_PULL | D::FLAG_PRIOR_SETTLED,
            d[0].flags);
  EXPECT_TRUE(ceph::rdma::prior_attempts_settled(d));
  s->put();
}

// ObjectOperation::dup() takes over another request's ops, as an OSD
// does when it proxies a read or copies a manifest object's chunks, and
// keeps every per-op vector aligned with them, so that an op added
// afterwards passes add_op()'s checks. The copied ops go inline.
TEST(RdmaDelivery, DupKeepsPerOpVectorsAligned)
{
  std::vector<OSDOp> sops(2);
  sops[0].op.op = CEPH_OSD_OP_READ;
  sops[0].op.extent.length = 4096;
  sops[1].op.op = CEPH_OSD_OP_STAT;

  ::ObjectOperation op;
  ceph::buffer::list bl;
  int rv = 0;
  ceph::rdma::oob_result_t got;
  op.read(0, 8192, &bl, &rv, nullptr);
  op.set_rdma_delivery("t0", 0, 0, &got);
  op.dup(sops);
  ASSERT_EQ(2u, op.ops.size());
  EXPECT_EQ(2u, op.rdma_delivery.size());
  EXPECT_EQ(2u, op.rdma_oob_result.size());
  EXPECT_FALSE(op.has_rdma_delivery());
  EXPECT_EQ(nullptr, op.rdma_oob_result[0]);

  op.assert_version(1);
  ASSERT_EQ(3u, op.ops.size());
  EXPECT_EQ(3u, op.rdma_delivery.size());
  EXPECT_EQ(3u, op.rdma_oob_result.size());
}

// An attempt is settled - nothing of it can land after its reply - when
// every descriptor-bearing op of it was declined or landed and none was
// resent. A replica's -EAGAIN bounce reports declined for each op.
TEST(RdmaDelivery, AttemptSettled)
{
  using R = ceph::rdma::oob_result_t;
  using ceph::rdma::attempt_settled;
  // no results at all (an old OSD, no reply): not settled
  EXPECT_FALSE(attempt_settled({}));
  EXPECT_TRUE(attempt_settled({R::FLAG_DECLINED}));
  EXPECT_TRUE(attempt_settled({R::FLAG_LANDED | R::FLAG_CRC64NVME}));
  // a split whose one shard bounced while the other landed its share
  EXPECT_TRUE(attempt_settled({R::FLAG_DECLINED, R::FLAG_LANDED}));
  // a push cut off after its writes went out, or a result that says
  // nothing: not settled
  EXPECT_FALSE(attempt_settled({R::FLAG_DECLINED, 0}));
  EXPECT_FALSE(attempt_settled({R::FLAG_CRC64NVME}));
  // a part that was itself resent may hide an unanswered attempt
  EXPECT_FALSE(attempt_settled({R::FLAG_DECLINED,
                                R::FLAG_LANDED | R::FLAG_RESENT}));
}

// An OSD delivers a resend out of band only when every descriptor it
// carries is vouched for; the bit is one the OSD knows, so a first
// attempt carrying it is not refused for an unknown flag.
TEST(RdmaDelivery, PriorAttemptsSettled)
{
  using D = ceph::rdma::delivery_t;
  using ceph::rdma::prior_attempts_settled;
  EXPECT_NE(0u, D::KNOWN_FLAGS & D::FLAG_PRIOR_SETTLED);
  D vouched{"t0", 0, D::FLAG_PRIOR_SETTLED | D::FLAG_CRC64NVME};
  D plain{"t1", 4096, D::FLAG_CRC64NVME};
  D none;  // an op of the request without a descriptor
  EXPECT_TRUE(prior_attempts_settled({vouched}));
  EXPECT_TRUE(prior_attempts_settled({none, vouched, none}));
  EXPECT_FALSE(prior_attempts_settled({vouched, plain}));
  EXPECT_FALSE(prior_attempts_settled({plain}));
  // nothing to deliver: nothing vouched for
  EXPECT_FALSE(prior_attempts_settled({none}));
  EXPECT_FALSE(prior_attempts_settled({}));
}

// A split read's result: declined only when every sub-read declined,
// landed only when every one landed and bytes moved, resent when any was.
TEST(RdmaDelivery, FoldTransferFlags)
{
  using R = ceph::rdma::oob_result_t;
  using ceph::rdma::fold_transfer_flags;
  EXPECT_EQ(0u, fold_transfer_flags({}, 0));
  EXPECT_EQ(R::FLAG_DECLINED,
            fold_transfer_flags({R::FLAG_DECLINED, R::FLAG_DECLINED}, 0));
  // one sub-read started a transfer: not declined
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_DECLINED, 0}, 0));
  EXPECT_EQ(R::FLAG_LANDED,
            fold_transfer_flags({R::FLAG_LANDED, R::FLAG_LANDED}, 8192));
  // a sub-read delivered without the promise: not landed
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_LANDED, 0}, 8192));
  // landed needs bytes
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_LANDED}, 0));
  EXPECT_EQ(R::FLAG_DECLINED | R::FLAG_RESENT,
            fold_transfer_flags({R::FLAG_DECLINED,
                                 R::FLAG_DECLINED | R::FLAG_RESENT}, 0));
  // the crc flags are not folded here
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_CRC64NVME}, 4096));
}

// A stripe read whose push its OSD cut off - per plan, after the writes
// went out - from the OSD's result, through the Objecter, into the slot a
// gateway reads: the data comes back inline with a result that is neither
// declined nor landed, which sends the GET to its HTTP fallback behind
// the fence. It does not fail the GET.
TEST(RdmaDelivery, CutOffPushFallsBackToHttp)
{
  using R = ceph::rdma::oob_result_t;
  using L = librados::ObjectReadOperation;
  using ceph::rdma::attempt_flags;
  using rgw::rdma::classify_stripe;
  using rgw::rdma::stripe_reply;

  // what reaches the gateway's slot for an OSD result, the way librados
  // copies it in (librados_cxx.cc)
  auto through_objecter = [](const R& osd, int attempts) {
    ::ObjectOperation op;
    ceph::buffer::list bl;
    int rv = 0;
    L::rdma_delivery_result slot;
    op.read(0, 4096, &bl, &rv, nullptr);
    op.set_rdma_delivery("t", 0, 0, [&slot](const R& r) {
      slot.bytes = r.bytes;
      slot.crc64 = r.crc64;
      slot.flags = r.flags;
    });
    auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                               std::move(op.ops), 0, (Context*)nullptr,
                               nullptr);
    o->rdma_delivery.swap(op.rdma_delivery);
    o->rdma_oob_result.swap(op.rdma_oob_result);
    o->rdma_oob_handler.swap(op.rdma_oob_handler);
    o->attempts = attempts;
    Objecter::deliver_rdma_oob_result(o, 0, osd);
    o->put();
    return slot;
  };

  // cut off after its writes went out: inline, and the fence holds
  R cut;
  cut.flags = attempt_flags(false, true, false);
  auto slot = through_objecter(cut, 1);
  EXPECT_EQ(stripe_reply::inline_data, classify_stripe(0, 4096));
  EXPECT_EQ(0u, slot.bytes);
  EXPECT_TRUE(rgw::rdma::fence_needed(std::vector{slot}));

  // cut off before anything went out: inline, declined, no fence
  R refused;
  refused.flags = attempt_flags(false, false, false);
  slot = through_objecter(refused, 1);
  EXPECT_EQ(L::RDMA_DELIVERY_DECLINED, slot.flags);
  EXPECT_FALSE(rgw::rdma::fence_needed(std::vector{slot}));

  // a resend of a read that was declined: an earlier attempt may have
  // pushed, so the fence holds
  slot = through_objecter(refused, 2);
  EXPECT_TRUE(rgw::rdma::fence_needed(std::vector{slot}));

  // placed with the promise: nothing inline, landed, no fence
  R placed;
  placed.bytes = 4096;
  placed.flags = attempt_flags(true, true, true);
  slot = through_objecter(placed, 1);
  EXPECT_EQ(stripe_reply::placed, classify_stripe(0, 0));
  EXPECT_EQ(L::RDMA_DELIVERY_LANDED, slot.flags);
  EXPECT_FALSE(rgw::rdma::fence_needed(std::vector{slot}));

  // a read that failed outright fails the GET, inline data or not
  EXPECT_EQ(stripe_reply::failed, classify_stripe(-EIO, 0));
  EXPECT_EQ(stripe_reply::failed, classify_stripe(-EIO, 4096));
}

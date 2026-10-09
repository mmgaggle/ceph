// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"

#include <string>

#include "gtest/gtest.h"

using ceph::rdma::parse_rdma_token;
using ceph::rdma::RDMA_TOKEN_MAX_LEN;

// the shape emitted by cuObject clients:
// raddr:rsize:rkey:lid:qp:has_gid:gid
static const std::string valid_token =
  "0102030405060708:01020304:0102aabb:0102:010203:1:"
  "0102030405060708090a0b0c0d0e0f10";

TEST(RdmaToken, ParseValid)
{
  auto w = parse_rdma_token(valid_token);
  ASSERT_TRUE(w);
  EXPECT_EQ(0x0102030405060708ull, w->addr);
  EXPECT_EQ(0x01020304ull, w->size);
}

TEST(RdmaToken, ParseMinimalFields)
{
  // only the leading addr:size fields are interpreted
  auto w = parse_rdma_token("ff:10:rest-is-opaque");
  ASSERT_TRUE(w);
  EXPECT_EQ(0xffull, w->addr);
  EXPECT_EQ(0x10ull, w->size);
}

TEST(RdmaToken, RejectMalformed)
{
  EXPECT_FALSE(parse_rdma_token(""));
  EXPECT_FALSE(parse_rdma_token("deadbeef"));           // no colon
  EXPECT_FALSE(parse_rdma_token("deadbeef:"));          // no second colon
  EXPECT_FALSE(parse_rdma_token(":1234:rkey"));         // empty addr
  EXPECT_FALSE(parse_rdma_token("1234::rkey"));         // empty size
  EXPECT_FALSE(parse_rdma_token("xyz:1234:rkey"));      // non-hex addr
  EXPECT_FALSE(parse_rdma_token("1234:no pe:rkey"));    // non-hex size
  EXPECT_FALSE(parse_rdma_token("11112222333344445:1:x")); // >16 hex digits
  EXPECT_FALSE(parse_rdma_token(std::string(RDMA_TOKEN_MAX_LEN + 1, '1')));
}

TEST(RdmaToken, MaxValues)
{
  auto w = parse_rdma_token("ffffffffffffffff:ffffffff:x");
  ASSERT_TRUE(w);
  EXPECT_EQ(~0ull, w->addr);
  EXPECT_EQ(0xffffffffull, w->size);
}

TEST(RdmaToken, TransportList)
{
  using ceph::rdma::parse_transport_list;
  using V = std::vector<std::string>;
  EXPECT_EQ(parse_transport_list(""), V{});
  EXPECT_EQ(parse_transport_list("ofi"), V{"ofi"});
  // order is preference; separators are commas, spaces and tabs
  EXPECT_EQ(parse_transport_list("cuobj,ofi"), (V{"cuobj", "ofi"}));
  EXPECT_EQ(parse_transport_list(" OFI ,\tcuobj , "), (V{"ofi", "cuobj"}));
  // duplicates and empty entries drop; unknown names stay for the
  // caller to report
  EXPECT_EQ(parse_transport_list("ofi,,ofi,foo"), (V{"ofi", "foo"}));
}

TEST(RdmaToken, OfiShape)
{
  using ceph::rdma::is_ofi_token;
  const std::string ofi =
    "7f0012345000:c00000:ofi1:rxm.1:020012340a00000100ff:1f";
  EXPECT_TRUE(is_ofi_token(ofi));
  EXPECT_TRUE(is_ofi_token("0:10:ofi1:xnet.1:0200:1"));
  // any version of the family: one this build cannot parse is still not
  // a cuObject descriptor, and is declined by the libfabric executor
  EXPECT_TRUE(is_ofi_token("0:10:ofi10:xnet.1:0200:1"));
  EXPECT_TRUE(is_ofi_token("0:10:ofi2:x"));
  // a libfabric token has the addr:size prefix too, so parsing the
  // window cannot tell the two apart
  auto w = parse_rdma_token(ofi);
  ASSERT_TRUE(w);
  EXPECT_EQ(0xc00000ull, w->size);

  EXPECT_FALSE(is_ofi_token(valid_token));
  EXPECT_FALSE(is_ofi_token(""));
  EXPECT_FALSE(is_ofi_token("ofi1"));
  EXPECT_FALSE(is_ofi_token("0:10"));
  EXPECT_FALSE(is_ofi_token("0:10:"));
  // only the third field counts
  EXPECT_FALSE(is_ofi_token("ofi1:0:10:tcp"));
  EXPECT_FALSE(is_ofi_token("0:10:ofi:xnet.1:0200:1"));
  EXPECT_FALSE(is_ofi_token("0:10:ofi1x:xnet.1:0200:1"));
  EXPECT_FALSE(is_ofi_token("0:10:1:ofi1:0200:1"));
}

TEST(RdmaToken, CuobjDescriptor)
{
  using ceph::rdma::is_cuobj_descriptor;
  EXPECT_TRUE(is_cuobj_descriptor(valid_token));
  EXPECT_TRUE(is_cuobj_descriptor("ff:10:rest-is-opaque"));
  // a libfabric token parses, but is not one
  EXPECT_FALSE(is_cuobj_descriptor("0:10:ofi1:xnet.1:0200:1"));
  EXPECT_FALSE(is_cuobj_descriptor("0:10:ofi2:xnet.1:0200:1"));
  // neither is a token whose window does not parse, such as an RC
  // queue-pair token, which has no colons
  EXPECT_FALSE(is_cuobj_descriptor(std::string(88, 'a')));
  EXPECT_FALSE(is_cuobj_descriptor(""));
  EXPECT_FALSE(is_cuobj_descriptor("xyz:1234:rkey"));
  EXPECT_FALSE(is_cuobj_descriptor("0:10"));
}

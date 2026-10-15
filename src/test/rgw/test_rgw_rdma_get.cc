// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * How RGWGetObj picks the out-of-band mode of a GET with
 * x-amz-rdma-token, and how it leaves it when the store streams the
 * data instead, without a store behind it. And the checksum headers of
 * an out-of-band transfer.
 */

#include "rgw_client_io.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_rdma_cksum.h"
#include "rgw_rest_s3.h"
#ifdef WITH_CUOBJ
#include "rgw_cuobj.h"
#endif

#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"

// rgw_op.cc
std::optional<rgw::cksum::Cksum> stored_full_crc(
  const rgw::sal::Attrs& attrs, const std::optional<int>& part_num,
  const std::optional<int>& parts_count);

namespace {

// a cuObject descriptor (raddr:rsize:rkey:lid:qp:has_gid:gid) of a 1 MiB
// window
constexpr const char* TOKEN_1M = "7f0000000000:100000:1a2b:0:4d2:0:0";
constexpr uint64_t WINDOW = 1 << 20;

// an S3 GET of the plain chain: no filter engaged
constexpr bool PLAIN_CHAIN = true;

/// OSD-direct delivery of cuObject descriptors on, in a context of its
/// own: oob_client_transports cannot change once a context has started
struct OsdDirect {
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};
  OsdDirect() {
    cct->_conf.set_val_or_die("rgw_rdma_osd_direct", "true");
    cct->_conf.set_val_or_die("oob_client_transports", "cuobj");
    cct->_conf.apply_changes(nullptr);
  }
};

/// a GET whose response body is what send_response_data() receives
class TestGetObj : public RGWGetObj {
public:
  using RGWGetObj::RdmaMode;
  using RGWGetObj::rdma_token;
  using RGWGetObj::rdma_mode;
  using RGWGetObj::rdma_switched_from;
  using RGWGetObj::rdma_fence_state;
  using RGWGetObj::total_len;
  using RGWGetObj::rdma_buf;
  using RGWGetObj::rdma_buf_offset;
  using RGWGetObj::select_rdma_mode;
  using RGWGetObj::note_rdma_reads;
  using RGWGetObj::rdma_write_staged;

  std::string body;
  bool eligible = true;

  explicit TestGetObj(req_state* s) {
    init(nullptr, s, nullptr);
    set_get_data(true);
  }

  int get_params(optional_yield) override { return 0; }
  int send_response_data_error(optional_yield) override { return 0; }
  int send_response_data(bufferlist& bl, off_t ofs, off_t len) override {
    if (len > 0) {
      body.append(bl.c_str() + ofs, len);
    }
    return 0;
  }
  bool rdma_eligible() override { return eligible; }
};

/// an S3 GetObject, to ask whether its data may go out of band
class TestGetObjS3 : public RGWGetObj_ObjStore_S3 {
public:
  using RGWGetObj_ObjStore_S3::attrs;
  using RGWGetObj_ObjStore_S3::rdma_eligible;

  explicit TestGetObjS3(req_state* s) {
    init(nullptr, s, nullptr);
  }
};

#define DEFINE_REQ_STATE(cct) RGWProcessEnv pe; \
  RGWEnv e; \
  req_state s(cct, pe, &e, 0); \
  s.op = OP_GET

using rgw::cksum::Cksum;

// the ETags of a PUT and of a multipart upload of 3 parts
const std::string ETAG = "0123456789abcdef0123456789abcdef";
const std::string ETAG_3 = ETAG + "-3";

constexpr char RAW[8] = {1, 2, 3, 4, 5, 6, 7, 8};

/// the attrs of an object with a stored checksum of the given type and
/// flags, and the ETag as an attr holds it
rgw::sal::Attrs cksum_attrs(rgw::cksum::Type type, uint16_t flags,
                            const std::string& etag)
{
  Cksum ck(type, RAW, Cksum::CtorStyle::raw);
  ck.flags = flags;
  rgw::sal::Attrs attrs;
  ck.encode(attrs[RGW_ATTR_CKSUM]);
  attrs[RGW_ATTR_ETAG].append(etag.c_str(), etag.size() + 1);
  return attrs;
}

/// the same, as a release before CRC combining encoded the checksum:
/// version 1, without flags
rgw::sal::Attrs legacy_cksum_attrs(rgw::cksum::Type type,
                                   const std::string& etag)
{
  const uint16_t size = Cksum::checksums[uint16_t(type)].digest_size;
  rgw::sal::Attrs attrs;
  bufferlist& bl = attrs[RGW_ATTR_CKSUM];
  ENCODE_START(1, 1, bl);
  encode(uint16_t(type), bl);
  encode(size, bl);
  bl.append(RAW, size);
  ENCODE_FINISH(bl);
  attrs[RGW_ATTR_ETAG].append(etag.c_str(), etag.size() + 1);
  return attrs;
}

constexpr std::optional<int> WHOLE; // no partNumber
constexpr std::optional<int> NO_PARTS; // an atomic manifest

/// a client that keeps the headers of the response
class HeaderClient : public rgw::io::RestfulClient {
  RGWEnv env;

protected:
  int init_env(CephContext*) override { return 0; }

public:
  std::map<std::string, std::string> headers;

  RGWEnv& get_env() noexcept override { return env; }
  size_t complete_request() override { return 0; }
  size_t send_100_continue() override { return 0; }
  size_t send_status(int, const char*) override { return 0; }
  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override {
    headers.emplace(name, value);
    return 0;
  }
  size_t send_content_length(uint64_t) override { return 0; }
  size_t complete_header() override { return 0; }
  size_t recv_body(char*, size_t) override { return 0; }
  size_t send_body(const char*, size_t len) override { return len; }
  void flush() override {}
};

} // anonymous namespace

using Mode = TestGetObj::RdmaMode;

TEST(RgwRdmaGet, PassthroughWhenTheResponseFitsTheWindow)
{
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;

  op.total_len = WINDOW;
  op.select_rdma_mode(PLAIN_CHAIN);
  EXPECT_EQ(Mode::PASSTHROUGH, op.rdma_mode);

  // the OSDs write each stripe at its offset in the window, which must
  // hold the whole response
  op.total_len = WINDOW + 1;
  op.select_rdma_mode(PLAIN_CHAIN);
  EXPECT_EQ(Mode::NONE, op.rdma_mode);
}

TEST(RgwRdmaGet, NoOutOfBandModeWhenNotEligible)
{
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 4096;
  op.eligible = false;
  op.select_rdma_mode(PLAIN_CHAIN);
  EXPECT_EQ(Mode::NONE, op.rdma_mode);
}

TEST(RgwRdmaGet, InlineDataSwitchesToAFallbackInPlace)
{
  // a store or filter that ignores the token (as D4N does) streams the
  // data: the GET serves it in a fallback mode, here over HTTP, instead
  // of failing the read to run it again
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  op.select_rdma_mode(PLAIN_CHAIN);
  ASSERT_EQ(Mode::PASSTHROUGH, op.rdma_mode);

  bufferlist bl;
  bl.append("xx01234567");
  EXPECT_EQ(0, op.get_data_cb(bl, 2, 5));
  EXPECT_EQ(Mode::NONE, op.rdma_mode);
  EXPECT_EQ(Mode::PASSTHROUGH, op.rdma_switched_from);
  EXPECT_EQ(0, op.get_data_cb(bl, 7, 3));
  EXPECT_EQ("01234567", op.body);
  EXPECT_EQ(Mode::PASSTHROUGH, op.rdma_switched_from);
}

TEST(RgwRdmaGet, ReadsOfASwitchedGetWroteTheClientWindow)
{
  // reads that a store sent with the token before it streamed the data
  // anyway went out in passthrough: the response and an error wait for
  // their writes to the client's window
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  op.select_rdma_mode(PLAIN_CHAIN);
  bufferlist bl;
  bl.append("01234567");
  ASSERT_EQ(0, op.get_data_cb(bl, 0, 8));
  ASSERT_EQ(Mode::NONE, op.rdma_mode);

  op.note_rdma_reads(true, true, false, 0.25);
  EXPECT_TRUE(op.rdma_fence_state.sent);
  EXPECT_TRUE(op.rdma_fence_state.client_window);
  EXPECT_EQ(250u, op.rdma_fence_state.wait_ms);
}

TEST(RgwRdmaGet, ReadsOfAFallbackWroteNoClientWindow)
{
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = WINDOW + 1;
  op.select_rdma_mode(PLAIN_CHAIN);
  ASSERT_EQ(Mode::NONE, op.rdma_mode);
  op.note_rdma_reads(true, true, false, 0.25);
  EXPECT_FALSE(op.rdma_fence_state.client_window);

  op.total_len = WINDOW;
  op.select_rdma_mode(PLAIN_CHAIN);
  ASSERT_EQ(Mode::PASSTHROUGH, op.rdma_mode);
  op.note_rdma_reads(true, true, false, 0.25);
  EXPECT_TRUE(op.rdma_fence_state.client_window);
}

TEST(RgwRdmaGet, OnlyGetObjectIsEligible)
{
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObjS3 op(&s);
  EXPECT_TRUE(op.rdma_eligible());

  // SelectObjectContent, a POST, runs the data through its query engine
  s.op = OP_POST;
  EXPECT_FALSE(op.rdma_eligible());
  s.op = OP_GET;

  // the website endpoint serves an object that redirects with a 301
  s.prot_flags |= RGW_REST_WEBSITE;
  EXPECT_TRUE(op.rdma_eligible());
  bufferlist location;
  location.append("/elsewhere");
  op.attrs[RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION] = location;
  EXPECT_FALSE(op.rdma_eligible());

  // which the S3 endpoint ignores
  s.prot_flags &= ~RGW_REST_WEBSITE;
  EXPECT_TRUE(op.rdma_eligible());
}

TEST(RgwRdmaGet, NoErrorDocumentIsEligible)
{
  // the website endpoint serves an ErrorDocument with the 404 of the
  // error it answers, which the client window cannot carry
  DEFINE_REQ_STATE(g_ceph_context);
  s.prot_flags |= RGW_REST_WEBSITE;
  TestGetObjS3 op(&s);
  EXPECT_TRUE(op.rdma_eligible());
  op.set_custom_http_response(404);
  EXPECT_FALSE(op.rdma_eligible());
}

TEST(RgwRdmaGet, StoredCrcOfAWholeObject)
{
  using rgw::cksum::Type;
  // a PUT over HTTP stores its CRC without flags
  auto attrs = cksum_attrs(Type::crc32c, 0, ETAG);
  auto stored = stored_full_crc(attrs, WHOLE, NO_PARTS);
  ASSERT_TRUE(stored);
  EXPECT_EQ(Type::crc32c, stored->type);
  // and a release before CRC combining with version 1
  EXPECT_TRUE(stored_full_crc(legacy_cksum_attrs(Type::crc32c, ETAG),
                              WHOLE, NO_PARTS));
  // an OSD-direct PUT stores the CRC its OSDs report, under an ETag of
  // the multipart form
  attrs = cksum_attrs(Type::crc64nvme, Cksum::FLAG_V2, ETAG_3);
  stored = stored_full_crc(attrs, WHOLE, NO_PARTS);
  ASSERT_TRUE(stored);
  EXPECT_EQ(Type::crc64nvme, stored->type);
  // CRCs combined from those of the parts
  EXPECT_TRUE(stored_full_crc(
      cksum_attrs(Type::crc32c, Cksum::FLAG_V2 | Cksum::FULL_OBJECT_MASK,
                  ETAG_3),
      WHOLE, 3));

  // no CRC the OSDs compute
  EXPECT_FALSE(stored_full_crc(
      cksum_attrs(Type::sha256, Cksum::FLAG_V2, ETAG), WHOLE, NO_PARTS));
  attrs.erase(RGW_ATTR_CKSUM);
  EXPECT_FALSE(stored_full_crc(attrs, WHOLE, NO_PARTS));
}

TEST(RgwRdmaGet, NoStoredCrcThatIsComposite)
{
  using rgw::cksum::Type;
  // composite by its flags, with or without a multipart manifest
  const uint16_t composite = Cksum::FLAG_V2 | Cksum::COMPOSITE_MASK;
  EXPECT_FALSE(stored_full_crc(cksum_attrs(Type::crc32c, composite, ETAG_3),
                               WHOLE, 3));
  EXPECT_FALSE(stored_full_crc(cksum_attrs(Type::crc32c, composite, ETAG),
                               WHOLE, NO_PARTS));

  // a multipart upload's from before CRC combining has no flags
  const auto legacy = legacy_cksum_attrs(Type::crc32c, ETAG_3);
  EXPECT_FALSE(stored_full_crc(legacy, WHOLE, 3));
  // nor once a transition, sync or copy rewrote it with an atomic
  // manifest, which keeps the ETag
  EXPECT_FALSE(stored_full_crc(legacy, WHOLE, NO_PARTS));
  // where part 1 is the whole object
  EXPECT_FALSE(stored_full_crc(legacy, 1, NO_PARTS));
}

TEST(RgwRdmaGet, StoredCrcOfAPart)
{
  using rgw::cksum::Type;
  // a part's attrs are its own, but for the ETag of the whole upload
  EXPECT_TRUE(stored_full_crc(
      cksum_attrs(Type::crc32c, Cksum::FLAG_FULL_OBJECT, ETAG_3), 2, 3));
  EXPECT_TRUE(stored_full_crc(legacy_cksum_attrs(Type::crc32c, ETAG_3),
                              2, 3));
  // one of a composite upload is reported as composite
  EXPECT_FALSE(stored_full_crc(
      cksum_attrs(Type::crc32c, Cksum::FLAG_COMPOSITE, ETAG_3), 2, 3));
}

TEST(RgwRdmaGet, ChecksumAlgorithmOfTheRequest)
{
  DEFINE_REQ_STATE(g_ceph_context);
  rgw::rdma::cksum_want want;
  // CRC64NVME when the request has no x-amz-rdma-checksum-algorithm
  ASSERT_EQ(0, rgw::rdma::parse_checksum_header(&s, &want));
  EXPECT_TRUE(want.crc64nvme);
  EXPECT_FALSE(want.crc32c);

  e.set("HTTP_X_AMZ_RDMA_CHECKSUM_ALGORITHM", "crc32c");
  ASSERT_EQ(0, rgw::rdma::parse_checksum_header(&s, &want));
  EXPECT_FALSE(want.crc64nvme);
  EXPECT_TRUE(want.crc32c);
  EXPECT_TRUE(s.err.message.empty());

  // an S3 checksum the OSDs do not compute
  e.set("HTTP_X_AMZ_RDMA_CHECKSUM_ALGORITHM", "CRC32");
  EXPECT_EQ(-EINVAL, rgw::rdma::parse_checksum_header(&s, &want));
  EXPECT_EQ("x-amz-rdma-checksum-algorithm must be CRC64NVME or CRC32C",
            s.err.message);
}

TEST(RgwRdmaGet, ChecksumHeaders)
{
  using Headers = std::map<std::string, std::string>;
  DEFINE_REQ_STATE(g_ceph_context);
  HeaderClient client;
  s.cio = &client;

  // the check values of "123456789", each in its own header
  rgw::rdma::dump_checksums(&s, 0xae8b14860a799888ull, std::nullopt);
  EXPECT_EQ((Headers{{"x-amz-rdma-checksum-crc64nvme", "rosUhgp5mIg="}}),
            client.headers);
  client.headers.clear();
  rgw::rdma::dump_checksums(&s, std::nullopt, 0xe3069283u);
  EXPECT_EQ((Headers{{"x-amz-rdma-checksum-crc32c", "4waSgw=="}}),
            client.headers);
  client.headers.clear();
  rgw::rdma::dump_checksums(&s, 0xae8b14860a799888ull, 0xe3069283u);
  EXPECT_EQ((Headers{{"x-amz-rdma-checksum-crc64nvme", "rosUhgp5mIg="},
                     {"x-amz-rdma-checksum-crc32c", "4waSgw=="}}),
            client.headers);
  client.headers.clear();
  rgw::rdma::dump_checksums(&s, std::nullopt, std::nullopt);
  EXPECT_TRUE(client.headers.empty());
  s.cio = nullptr;
}

#ifdef WITH_CUOBJ
TEST(RgwRdmaGet, StagingStaysInsideTheResponse)
{
  // staged mode copies the data into a buffer that holds the response,
  // which fits the client's window, and sends nothing over HTTP
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  std::vector<char> mem(16);
  RGWCuObjServer::RDMABufEntry entry;
  entry.ptr = mem.data();
  entry.size = mem.size();
  op.rdma_buf = &entry;
  op.rdma_mode = Mode::STAGED;

  bufferlist bl;
  bl.append("xx01234567yy");
  EXPECT_EQ(0, op.get_data_cb(bl, 2, 5));
  EXPECT_EQ(0, op.get_data_cb(bl, 7, 3));
  EXPECT_EQ(8u, op.rdma_buf_offset);
  EXPECT_EQ("01234567", std::string(mem.data(), 8));
  EXPECT_TRUE(op.body.empty());

  // nothing past the response
  EXPECT_EQ(-EIO, op.get_data_cb(bl, 0, 1));
  EXPECT_EQ(8u, op.rdma_buf_offset);
  op.rdma_buf = nullptr;
}

TEST(RgwRdmaGet, StagingCopiesEachSegment)
{
  // data in several segments, as a decompressed or a RADOS read can
  // come, is staged without first being copied into one segment
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  std::vector<char> mem(8);
  RGWCuObjServer::RDMABufEntry entry;
  entry.ptr = mem.data();
  entry.size = mem.size();
  op.rdma_buf = &entry;
  op.rdma_mode = Mode::STAGED;

  bufferlist bl;
  bl.append(buffer::copy("xx012", 5));
  bl.append(buffer::copy("345", 3));
  bl.append(buffer::copy("67yy", 4));
  ASSERT_EQ(3u, bl.get_num_buffers());
  EXPECT_EQ(0, op.get_data_cb(bl, 2, 8));
  EXPECT_EQ(8u, op.rdma_buf_offset);
  EXPECT_EQ("01234567", std::string(mem.data(), 8));
  EXPECT_EQ(3u, bl.get_num_buffers());
  op.rdma_buf = nullptr;
}

TEST(RgwRdmaGet, StagingStaysInsideTheBuffer)
{
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  std::vector<char> mem(4);
  RGWCuObjServer::RDMABufEntry entry;
  entry.ptr = mem.data();
  entry.size = mem.size();
  op.rdma_buf = &entry;
  op.rdma_mode = Mode::STAGED;

  bufferlist bl;
  bl.append("01234567");
  EXPECT_EQ(-EIO, op.get_data_cb(bl, 0, 5));
  EXPECT_EQ(0u, op.rdma_buf_offset);
  op.rdma_buf = nullptr;
}

TEST(RgwRdmaGet, NoStagedWriteWithoutTheServer)
{
  // the response goes out after the write, and fails without it
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  std::vector<char> mem(8);
  RGWCuObjServer::RDMABufEntry entry;
  entry.ptr = mem.data();
  entry.size = mem.size();
  op.rdma_buf = &entry;
  op.rdma_mode = Mode::STAGED;
  op.rdma_buf_offset = 8;
  if (!RGWCuObjServer::get_instance()) {
    EXPECT_EQ(-EIO, op.rdma_write_staged());
  }
  op.rdma_buf = nullptr;
}
#endif

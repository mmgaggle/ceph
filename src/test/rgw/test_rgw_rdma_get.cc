// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * How RGWGetObj picks the out-of-band mode of a GET with
 * x-amz-rdma-token, and how it leaves it when the store streams the
 * data instead, without a store behind it.
 */

#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_rest_s3.h"
#ifdef WITH_CUOBJ
#include "rgw_cuobj.h"
#endif

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"

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

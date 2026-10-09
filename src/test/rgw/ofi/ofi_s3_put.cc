// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * S3 PUT with OSD-direct pulls over libfabric (test client).
 *
 * Registers the object's bytes as a window that peers may read, puts
 * its endpoint name and key in x-amz-rdma-token, and sends a SigV4 S3
 * PUT with no body, with the object's CRC-64/NVME in
 * x-amz-checksum-crc64nvme, or its CRC-32C in x-amz-checksum-crc32c when
 * OFI_S3_CKSUM=CRC32C. With rgw_rdma_osd_put, the gateway writes
 * the object as stripes, and the primary OSD of each RDMA-reads its
 * stripe out of the window. A progress thread polls the provider, so
 * that a provider with manual progress serves the reads. The client then
 * reads the object back over plain HTTP and compares it with the file.
 * A 501 with x-amz-rdma-reply 501 means the gateway declined, and the
 * client sends the body instead, as a real client would. A pulled PUT
 * must report the stored checksum, and the OSDs' CRCs of what they
 * pulled in x-amz-rdma-checksum-<algorithm>, all equal to this client's.
 *
 * For tests of the failure paths: OFI_S3_PUT_CRC=<base64> sends that
 * checksum instead of the object's, which the gateway must refuse with
 * 400 and store nothing; OFI_S3_PUT_WINDOW=write registers the window for
 * writes only, so the OSDs' reads fail and the gateway declines.
 *
 *   ceph_test_rgw_ofi_put <provider> <domain|-> <node|-> <endpoint> \
 *     <bucket> <key> <access> <secret> <file>
 */

#include <curl/curl.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <strings.h>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "common/ofi_rma.h"
#include "ofi_s3_cksum.h"

namespace {

struct http_result {
  long status = 0;
  std::string reply;      // x-amz-rdma-reply
  std::string bytes;      // x-amz-rdma-bytes-transferred
  std::string rdma_crc64;  // x-amz-rdma-checksum-crc64nvme
  std::string rdma_crc32c; // x-amz-rdma-checksum-crc32c
  std::string cksum;       // x-amz-checksum-crc64nvme or -crc32c
  std::string etag;
  std::string body;
  std::string error;
};

size_t on_header(char* p, size_t size, size_t n, void* arg)
{
  auto* r = static_cast<http_result*>(arg);
  std::string line(p, size * n);
  auto colon = line.find(':');
  if (colon != std::string::npos) {
    std::string name = line.substr(0, colon);
    for (auto& c : name) c = std::tolower(c);
    std::string value = line.substr(colon + 1);
    while (!value.empty() && value.front() == ' ') value.erase(0, 1);
    while (!value.empty() && (value.back() == '\r' || value.back() == '\n'))
      value.pop_back();
    if (name == "x-amz-rdma-reply") r->reply = value;
    if (name == "x-amz-rdma-bytes-transferred") r->bytes = value;
    if (name == "x-amz-rdma-checksum-crc64nvme") r->rdma_crc64 = value;
    if (name == "x-amz-rdma-checksum-crc32c") r->rdma_crc32c = value;
    if (name == "x-amz-checksum-crc64nvme" || name == "x-amz-checksum-crc32c")
      r->cksum = value;
    if (name == "etag") r->etag = value;
  }
  return size * n;
}

size_t on_body(char* p, size_t size, size_t n, void* arg)
{
  static_cast<http_result*>(arg)->body.append(p, size * n);
  return size * n;
}

struct upload_t {
  const char* p;
  size_t left;
};

size_t on_read(char* buf, size_t size, size_t n, void* arg)
{
  auto* u = static_cast<upload_t*>(arg);
  const size_t k = std::min(size * n, u->left);
  std::memcpy(buf, u->p, k);
  u->p += k;
  u->left -= k;
  return k;
}

std::string arg_or_empty(const char* a)
{
  return std::strcmp(a, "-") == 0 ? std::string{} : std::string{a};
}

/// a signed S3 request; body, when given, is sent as the PUT's body
http_result request(const std::string& url, const std::string& userpwd,
		    const char* method, const std::vector<std::string>& headers,
		    const std::vector<char>* body)
{
  http_result res;
  CURL* c = curl_easy_init();
  struct curl_slist* hdrs = nullptr;
  for (const auto& h : headers) {
    hdrs = curl_slist_append(hdrs, h.c_str());
  }
  upload_t up{body ? body->data() : nullptr, body ? body->size() : 0};
  curl_easy_setopt(c, CURLOPT_URL, url.c_str());
  curl_easy_setopt(c, CURLOPT_AWS_SIGV4, "aws:amz:us-east-1:s3");
  curl_easy_setopt(c, CURLOPT_USERPWD, userpwd.c_str());
  if (std::strcmp(method, "PUT") == 0) {
    curl_easy_setopt(c, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(c, CURLOPT_READFUNCTION, on_read);
    curl_easy_setopt(c, CURLOPT_READDATA, &up);
    curl_easy_setopt(c, CURLOPT_INFILESIZE_LARGE,
		     static_cast<curl_off_t>(up.left));
    // no "Expect: 100-continue" for an empty body
    hdrs = curl_slist_append(hdrs, "Expect:");
  }
  curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
  curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, on_header);
  curl_easy_setopt(c, CURLOPT_HEADERDATA, &res);
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, on_body);
  curl_easy_setopt(c, CURLOPT_WRITEDATA, &res);
  if (CURLcode cc = curl_easy_perform(c); cc != CURLE_OK) {
    res.error = curl_easy_strerror(cc);
  }
  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &res.status);
  curl_slist_free_all(hdrs);
  curl_easy_cleanup(c);
  return res;
}

} // anonymous namespace

int main(int argc, char** argv)
{
  if (argc != 10) {
    std::fprintf(stderr, "usage: %s <provider> <domain|-> <node|-> <endpoint> "
		 "<bucket> <key> <access> <secret> <file>\n", argv[0]);
    return 2;
  }
  if (!ofi_s3::self_test()) {
    std::fprintf(stderr, "CRC self-test failed\n");
    return 2;
  }
  const char* alg_env = std::getenv("OFI_S3_CKSUM");
  const bool use32c = alg_env && strcasecmp(alg_env, "CRC32C") == 0;
  const std::string cksum_hdr =
    use32c ? "x-amz-checksum-crc32c: " : "x-amz-checksum-crc64nvme: ";
  ceph::ofi::config_t cfg;
  cfg.provider = argv[1];
  cfg.domain = arg_or_empty(argv[2]);
  cfg.node = arg_or_empty(argv[3]);
  // the OSDs read the window: a provider with manual progress serves
  // their reads only while someone polls
  cfg.progress_thread = true;
  cfg.reads = true;
  const std::string endpoint = argv[4], bucket = argv[5], key = argv[6];
  const std::string userpwd = std::string(argv[7]) + ":" + argv[8];
  std::ifstream in(argv[9], std::ios::binary);
  const std::vector<char> data{std::istreambuf_iterator<char>(in), {}};
  const size_t size = data.size();
  if (size == 0) {
    std::fprintf(stderr, "empty file\n");
    return 2;
  }

  std::string err;
  auto ep = ceph::ofi::Endpoint::open(cfg, &err);
  if (!ep) {
    std::fprintf(stderr, "libfabric endpoint: %s\n", err.c_str());
    return 1;
  }
  if (!ep->reads()) {
    std::fprintf(stderr, "%s offers no RMA reads here\n",
		 ep->provider().c_str());
    return 1;
  }
  void* mem = nullptr;
  if (posix_memalign(&mem, 4096, size) != 0) {
    return 1;
  }
  char* window = static_cast<char*>(mem);
  std::memcpy(window, data.data(), size);
  ceph::ofi::Endpoint::window_t w;
  const char* access_env = std::getenv("OFI_S3_PUT_WINDOW");
  const unsigned access =
    access_env && std::strcmp(access_env, "write") == 0 ?
    ceph::ofi::Endpoint::remote_write : ceph::ofi::Endpoint::remote_read;
  if (int r = ep->register_window(window, size, ceph::ofi::memory_t{},
				  access, &w);
      r < 0) {
    std::fprintf(stderr, "register window: %s\n", ep->last_error().c_str());
    return 1;
  }
  const std::string token = ep->window_token(w, 0, size);
  const std::string crc64 = ofi_s3::armor_crc64nvme(window, size);
  const std::string crc32c = ofi_s3::armor_crc32c(window, size);
  const std::string crc = use32c ? crc32c : crc64;
  const char* crc_env = std::getenv("OFI_S3_PUT_CRC");
  const std::string sent_crc = crc_env ? crc_env : crc;
  std::printf("%s\nwindow %zu bytes, CRC64NVME %s, CRC32C %s, token %s\n",
	      ep->describe().c_str(), size, crc64.c_str(), crc32c.c_str(),
	      token.c_str());

  const std::string url = endpoint + "/" + bucket + "/" + key;
  const auto t0 = std::chrono::steady_clock::now();
  http_result res = request(url, userpwd, "PUT",
			    {"x-amz-rdma-token: " + token,
			     cksum_hdr + sent_crc},
			    nullptr);
  const double ms = std::chrono::duration<double, std::milli>(
    std::chrono::steady_clock::now() - t0).count();
  std::printf("PUT: HTTP %ld, x-amz-rdma-reply %s, bytes %s, ETag %s, "
	      "%s%s, x-amz-rdma-checksum-crc64nvme %s, "
	      "x-amz-rdma-checksum-crc32c %s, %.1f ms\n",
	      res.status, res.reply.c_str(), res.bytes.c_str(),
	      res.etag.c_str(), cksum_hdr.c_str(), res.cksum.c_str(),
	      res.rdma_crc64.c_str(), res.rdma_crc32c.c_str(), ms);
  bool pulled = true;
  if (!res.error.empty()) {
    std::printf("FAIL: PUT failed: %s\n", res.error.c_str());
    return 1;
  }
  if (res.status == 501 && res.reply == "501") {
    // declined: nothing was stored; send the body, as a client would
    std::printf("declined: %s\n", res.body.c_str());
    pulled = false;
    res = request(url, userpwd, "PUT", {cksum_hdr + crc},
		  &data);
    std::printf("PUT with body: HTTP %ld\n", res.status);
  }
  if (crc_env && sent_crc != crc) {
    // the wrong checksum must fail the PUT, and store nothing
    if (res.status == 400 && res.body.find("BadDigest") != std::string::npos) {
      std::printf("PASS: a wrong checksum was refused with 400 BadDigest\n");
      return 0;
    }
    std::printf("FAIL: a wrong checksum got HTTP %ld\n", res.status);
    return 1;
  }
  if (res.status != 200) {
    std::printf("FAIL: PUT returned %ld: %s\n", res.status, res.body.c_str());
    return 1;
  }
  if (pulled) {
    if (res.reply != "200" || res.bytes != std::to_string(size)) {
      std::printf("FAIL: the reply does not report an OSD-direct PUT of %zu "
		  "bytes\n", size);
      return 1;
    }
    if (res.cksum != crc) {
      std::printf("FAIL: the stored checksum %s is not the client's %s\n",
		  res.cksum.c_str(), crc.c_str());
      return 1;
    }
    // the OSDs' CRCs of what they pulled: CRC-64/NVME always, for the
    // ETag, and CRC-32C when the client chose it
    if (res.rdma_crc64 != crc64 || (use32c && res.rdma_crc32c != crc32c) ||
	(!use32c && !res.rdma_crc32c.empty())) {
      std::printf("FAIL: the OSDs' checksums (%s, %s) are not this client's "
		  "(%s, %s)\n", res.rdma_crc64.c_str(), res.rdma_crc32c.c_str(),
		  crc64.c_str(), use32c ? crc32c.c_str() : "none");
      return 1;
    }
  }
  // the window may be reused from here: re-key it, so that the token
  // stops letting anyone read this memory
  ep->rekey_window(w);

  // read the object back over plain HTTP
  http_result got = request(url, userpwd, "GET", {}, nullptr);
  if (got.status != 200 || got.body.size() != size ||
      std::memcmp(got.body.data(), data.data(), size) != 0) {
    size_t i = 0;
    while (i < std::min(got.body.size(), size) && got.body[i] == data[i]) {
      i++;
    }
    std::printf("FAIL: GET returned %ld, %zu bytes, first difference at "
		"byte %zu\n", got.status, got.body.size(), i);
    return 1;
  }
  std::printf("PASS: %zu bytes stored and read back (%s)\n", size,
	      pulled ? "pulled by the OSDs over libfabric" : "HTTP body");
  return 0;
}

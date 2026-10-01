// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * S3 GET with OSD-direct delivery over libfabric (test client).
 *
 * Registers a window with a libfabric provider, puts its endpoint name
 * and key in x-amz-rdma-token, and sends an ordinary SigV4 S3 GET. The
 * gateway forwards the token to the OSDs, which RMA-write the object's
 * stripes into the window; this client never learns who wrote. A
 * progress thread polls the provider so the writes land while the
 * gateway waits. Then the window is checked against a local copy of
 * the object.
 *
 *   ceph_test_rgw_ofi_get <provider> <domain|-> <node|-> <endpoint> \
 *     <bucket> <key> <access> <secret> <expected-file>
 */

#include <curl/curl.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "common/ofi_rma.h"

namespace {

struct http_result {
  long status = 0;
  std::string reply;      // x-amz-rdma-reply
  std::string bytes;      // x-amz-rdma-bytes-transferred
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
  }
  return size * n;
}

size_t on_body(char* p, size_t size, size_t n, void* arg)
{
  static_cast<http_result*>(arg)->body.append(p, size * n);
  return size * n;
}

std::string arg_or_empty(const char* a)
{
  return std::strcmp(a, "-") == 0 ? std::string{} : std::string{a};
}

} // anonymous namespace

int main(int argc, char** argv)
{
  if (argc != 10) {
    std::fprintf(stderr, "usage: %s <provider> <domain|-> <node|-> <endpoint> "
		 "<bucket> <key> <access> <secret> <expected-file>\n", argv[0]);
    return 2;
  }
  ceph::ofi::config_t cfg;
  cfg.provider = argv[1];
  cfg.domain = arg_or_empty(argv[2]);
  cfg.node = arg_or_empty(argv[3]);
  cfg.progress_thread = true;
  const std::string endpoint = argv[4], bucket = argv[5], key = argv[6];
  const std::string userpwd = std::string(argv[7]) + ":" + argv[8];
  std::ifstream in(argv[9], std::ios::binary);
  const std::vector<char> expected{std::istreambuf_iterator<char>(in), {}};
  const size_t size = expected.size();
  if (size == 0) {
    std::fprintf(stderr, "empty expected file\n");
    return 2;
  }

  std::string err;
  auto ep = ceph::ofi::Endpoint::open(cfg, &err);
  if (!ep) {
    std::fprintf(stderr, "libfabric endpoint: %s\n", err.c_str());
    return 1;
  }
  void* mem = nullptr;
  if (posix_memalign(&mem, 4096, size) != 0) {
    return 1;
  }
  std::memset(mem, 0, size);
  char* window = static_cast<char*>(mem);
  ceph::ofi::Endpoint::window_t w;
  if (int r = ep->register_window(window, size, &w); r < 0) {
    std::fprintf(stderr, "register window: %s\n", ep->last_error().c_str());
    return 1;
  }
  const std::string token = ep->window_token(w, 0, size);
  std::printf("%s\nwindow %zu bytes, token %s\n", ep->describe().c_str(), size,
	      token.c_str());

  http_result res;
  const auto t0 = std::chrono::steady_clock::now();
  CURL* c = curl_easy_init();
  std::string url = endpoint + "/" + bucket + "/" + key;
  struct curl_slist* hdrs = nullptr;
  hdrs = curl_slist_append(hdrs, ("x-amz-rdma-token: " + token).c_str());
  curl_easy_setopt(c, CURLOPT_URL, url.c_str());
  curl_easy_setopt(c, CURLOPT_AWS_SIGV4, "aws:amz:us-east-1:s3");
  curl_easy_setopt(c, CURLOPT_USERPWD, userpwd.c_str());
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
  // writes placed before the reply was sent are in the window; take
  // the endpoint's lock so this thread reads after its progress thread
  ep->sync();
  const double ms = std::chrono::duration<double, std::milli>(
    std::chrono::steady_clock::now() - t0).count();

  std::printf("HTTP %ld, x-amz-rdma-reply %s, bytes %s, body %zu, %.1f ms\n",
	      res.status, res.reply.c_str(), res.bytes.c_str(),
	      res.body.size(), ms);
  if (!res.error.empty() || res.status != 200) {
    std::printf("FAIL: request failed %s\n", res.error.c_str());
    return 1;
  }
  const char* got = window;
  size_t got_len = size;
  if (res.reply != "200") {
    // the gateway fell back to HTTP: the body is the object
    got = res.body.data();
    got_len = res.body.size();
    std::printf("fallback: object came back over HTTP\n");
  }
  if (got_len != size || std::memcmp(got, expected.data(), size) != 0) {
    size_t i = 0;
    while (i < std::min(got_len, size) && got[i] == expected[i]) i++;
    std::printf("FAIL: mismatch at byte %zu\n", i);
    return 1;
  }
  std::printf("PASS: %zu bytes verified (%s)\n", size,
	      res.reply == "200" ? "placed by the OSDs over libfabric"
				 : "HTTP fallback");
  return 0;
}

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * S3 GET with OSD-direct delivery over UET (mock-up client).
 *
 * Registers a window with the UEC reference provider, puts its
 * endpoint and key in x-amz-rdma-token, and sends an ordinary SigV4 S3
 * GET. The gateway forwards the token to the OSDs, which RMA-write the
 * object's stripes into the window over RUDI; this client never learns
 * who wrote. The software provider places incoming data only while the
 * application polls, so the main thread polls until the HTTP reply is
 * in, then verifies the window against a local copy of the object.
 *
 *   UET_IFNAME=<if> UET_PDS=pds ceph_test_rgw_uet_get \
 *     <endpoint> <bucket> <key> <access> <secret> <expected-file>
 */

#include <arpa/inet.h>
#include <curl/curl.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include "osd/uet_shim.h"

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
    while (!value.empty() && (value.front() == ' ')) value.erase(0, 1);
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

} // anonymous namespace

int main(int argc, char** argv)
{
  if (argc != 7) {
    std::fprintf(stderr, "usage: %s <endpoint> <bucket> <key> <access> "
		 "<secret> <expected-file>\n", argv[0]);
    return 2;
  }
  const std::string endpoint = argv[1], bucket = argv[2], key = argv[3];
  const std::string userpwd = std::string(argv[4]) + ":" + argv[5];
  std::ifstream in(argv[6], std::ios::binary);
  const std::vector<char> expected{std::istreambuf_iterator<char>(in), {}};
  const size_t size = expected.size();
  if (size == 0) {
    std::fprintf(stderr, "empty expected file\n");
    return 2;
  }

  // the window: a region the OSDs may write, idempotent-safe so writes
  // can use RUDI; no peer is ever inserted
  char err[256] = "";
  uet_shim* shim = uet_shim_open(size, 1, err, sizeof(err));
  if (!shim) {
    std::fprintf(stderr, "uet window: %s\n", err);
    return 1;
  }
  char* window = uet_shim_buffer(shim);

  char ip[INET_ADDRSTRLEN];
  struct in_addr a;
  a.s_addr = htonl(uet_shim_ipv4(shim));
  inet_ntop(AF_INET, &a, ip, sizeof(ip));
  char token[256];
  std::snprintf(token, sizeof(token), "0:%zx:uet1:%s:%llx", size, ip,
		static_cast<unsigned long long>(uet_shim_key(shim)));
  std::printf("window %zu bytes at %s, token %s\n", size, ip, token);

  // the S3 GET runs on its own thread; this one keeps the provider
  // progressing so the OSDs' writes land while the gateway waits
  http_result res;
  std::atomic<bool> done{false};
  const auto t0 = std::chrono::steady_clock::now();
  std::thread http([&] {
    CURL* c = curl_easy_init();
    std::string url = endpoint + "/" + bucket + "/" + key;
    struct curl_slist* hdrs = nullptr;
    hdrs = curl_slist_append(hdrs,
      (std::string("x-amz-rdma-token: ") + token).c_str());
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_AWS_SIGV4, "aws:amz:us-east-1:s3");
    curl_easy_setopt(c, CURLOPT_USERPWD, userpwd.c_str());
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, on_header);
    curl_easy_setopt(c, CURLOPT_HEADERDATA, &res);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, on_body);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &res);
    CURLcode cc = curl_easy_perform(c);
    if (cc != CURLE_OK) {
      res.error = curl_easy_strerror(cc);
    }
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &res.status);
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(c);
    done = true;
  });
  while (!done) {
    uet_shim_poll_rx(shim);
  }
  http.join();
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
	      res.reply == "200" ? "placed by the OSDs over UET"
				 : "HTTP fallback");
  return 0;
}

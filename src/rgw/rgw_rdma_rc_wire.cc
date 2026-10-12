// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rdma_rc_wire.h"

#include <algorithm>
#include <array>
#include <cstring>

#include <fmt/format.h>

namespace rgw::rdma::rc {

namespace {

int hex_nibble(char c)
{
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

bool all_hex(std::string_view s)
{
  return !s.empty() &&
    std::all_of(s.begin(), s.end(), [](char c) { return hex_nibble(c) >= 0; });
}

// fixed-width hex field of exactly `digits` digits with a value in
// [1, max]
std::optional<uint32_t> parse_fixed_hex(std::string_view s, size_t digits,
                                        uint32_t max)
{
  if (s.size() != digits || !all_hex(s)) {
    return std::nullopt;
  }
  uint32_t v = 0;
  for (char c : s) {
    v = (v << 4) | hex_nibble(c);
  }
  if (v == 0 || v > max) {
    return std::nullopt;
  }
  return v;
}

void put_le(uint8_t* p, uint64_t v, size_t bytes)
{
  for (size_t i = 0; i < bytes; i++) {
    p[i] = static_cast<uint8_t>(v >> (8 * i));
  }
}

uint64_t get_le(const uint8_t* p, size_t bytes)
{
  uint64_t v = 0;
  for (size_t i = 0; i < bytes; i++) {
    v |= static_cast<uint64_t>(p[i]) << (8 * i);
  }
  return v;
}

// RFC 3986 unreserved characters pass through unescaped
bool unreserved(unsigned char c)
{
  return std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~';
}

std::string percent_encode(std::string_view s)
{
  static constexpr char hex[] = "0123456789ABCDEF";
  std::string out;
  out.reserve(s.size());
  for (unsigned char c : s) {
    if (unreserved(c) || c == '/') {
      out.push_back(static_cast<char>(c));
    } else {
      out.push_back('%');
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 0xf]);
    }
  }
  return out;
}

// path-style unescape: "%XX" only, '+' stays literal; nullopt on a
// truncated or non-hex escape
std::optional<std::string> percent_decode(std::string_view s)
{
  std::string out;
  out.reserve(s.size());
  for (size_t i = 0; i < s.size(); i++) {
    if (s[i] != '%') {
      out.push_back(s[i]);
      continue;
    }
    if (i + 2 >= s.size()) {
      return std::nullopt;
    }
    int hi = hex_nibble(s[i + 1]);
    int lo = hex_nibble(s[i + 2]);
    if (hi < 0 || lo < 0) {
      return std::nullopt;
    }
    out.push_back(static_cast<char>((hi << 4) | lo));
    i += 2;
  }
  return out;
}

} // anonymous namespace

bool token_t::gid_is_zero() const
{
  return std::all_of(std::begin(gid), std::end(gid),
                     [](uint8_t b) { return b == 0; });
}

std::string encode_token(const token_t& t)
{
  std::array<uint8_t, TOKEN_BINARY_LEN> buf;
  size_t off = 0;
  buf[off++] = static_cast<uint8_t>(t.transport);
  put_le(&buf[off], t.qpn, 4); off += 4;
  std::memcpy(&buf[off], t.gid, 16); off += 16;
  put_le(&buf[off], t.rkey, 4); off += 4;
  put_le(&buf[off], t.addr, 8); off += 8;
  put_le(&buf[off], t.length, 8); off += 8;
  buf[off++] = t.port;
  put_le(&buf[off], t.lid, 2); off += 2;

  static constexpr char hex[] = "0123456789abcdef";
  std::string out;
  out.reserve(TOKEN_HEX_LEN);
  for (uint8_t b : buf) {
    out.push_back(hex[b >> 4]);
    out.push_back(hex[b & 0xf]);
  }
  return out;
}

std::optional<token_t> decode_token(std::string_view hex)
{
  if (hex.size() != TOKEN_HEX_LEN || !all_hex(hex)) {
    return std::nullopt;
  }
  std::array<uint8_t, TOKEN_BINARY_LEN> buf;
  for (size_t i = 0; i < TOKEN_BINARY_LEN; i++) {
    buf[i] = static_cast<uint8_t>((hex_nibble(hex[2 * i]) << 4) |
                                  hex_nibble(hex[2 * i + 1]));
  }
  token_t t;
  size_t off = 0;
  t.transport = static_cast<transport_t>(buf[off++]);
  t.qpn = static_cast<uint32_t>(get_le(&buf[off], 4)); off += 4;
  std::memcpy(t.gid, &buf[off], 16); off += 16;
  t.rkey = static_cast<uint32_t>(get_le(&buf[off], 4)); off += 4;
  t.addr = get_le(&buf[off], 8); off += 8;
  t.length = get_le(&buf[off], 8); off += 8;
  t.port = buf[off++];
  t.lid = static_cast<uint16_t>(get_le(&buf[off], 2));
  return t;
}

bool token_is_zero(std::string_view hex)
{
  return hex.size() == TOKEN_HEX_LEN &&
    std::all_of(hex.begin(), hex.end(), [](char c) { return c == '0'; });
}

bool valid_session_id(std::string_view s)
{
  return s.size() == SESSION_HEX_LEN && all_hex(s);
}

std::optional<uint32_t> parse_psn(std::string_view s)
{
  return parse_fixed_hex(s, 6, 0xffffff);
}

std::string format_psn(uint32_t psn)
{
  return fmt::format("{:06X}", psn & 0xffffff);
}

std::optional<uint32_t> parse_cookie(std::string_view s)
{
  return parse_fixed_hex(s, 8, 0xffffffff);
}

std::string format_cookie(uint32_t cookie)
{
  return fmt::format("{:08X}", cookie);
}

std::optional<uint64_t> parse_hex(std::string_view s)
{
  if (s.empty() || s.size() > 16 || !all_hex(s)) {
    return std::nullopt;
  }
  uint64_t v = 0;
  for (char c : s) {
    v = (v << 4) | hex_nibble(c);
  }
  return v;
}

std::string format_hex(uint64_t v)
{
  return fmt::format("{:x}", v);
}

std::optional<uint64_t> parse_decimal(std::string_view s)
{
  if (s.empty() || s.size() > 20 ||
      !std::all_of(s.begin(), s.end(),
                   [](char c) { return c >= '0' && c <= '9'; })) {
    return std::nullopt;
  }
  uint64_t v = 0;
  for (char c : s) {
    const uint64_t d = c - '0';
    if (v > (UINT64_MAX - d) / 10) {
      return std::nullopt;
    }
    v = v * 10 + d;
  }
  return v;
}

std::optional<target_t> parse_target(std::string_view target)
{
  if (target.empty() || target.front() != '/') {
    return std::nullopt;
  }
  target.remove_prefix(1);
  target_t t;
  if (auto q = target.find('?'); q != std::string_view::npos) {
    t.query = std::string{target.substr(q + 1)};
    target = target.substr(0, q);
  }
  const auto slash = target.find('/');
  if (slash == std::string_view::npos || slash == 0 ||
      slash + 1 == target.size()) {
    return std::nullopt;
  }
  auto bucket = percent_decode(target.substr(0, slash));
  auto key = percent_decode(target.substr(slash + 1));
  if (!bucket || !key || bucket->empty() || key->empty()) {
    return std::nullopt;
  }
  t.bucket = std::move(*bucket);
  t.key = std::move(*key);
  return t;
}

std::string build_target(std::string_view bucket, std::string_view key,
                         std::string_view query)
{
  std::string t = "/";
  t += percent_encode(bucket);
  t += '/';
  t += percent_encode(key);
  if (!query.empty()) {
    t += '?';
    t += query;
  }
  return t;
}

std::optional<std::string_view> sigv4_signed_headers(std::string_view auth)
{
  constexpr std::string_view scheme = "AWS4-HMAC-SHA256";
  if (auth.substr(0, scheme.size()) != scheme) {
    return std::nullopt;
  }
  constexpr std::string_view key = "SignedHeaders=";
  auto pos = auth.find(key);
  if (pos == std::string_view::npos) {
    return std::nullopt;
  }
  auto list = auth.substr(pos + key.size());
  list = list.substr(0, list.find(','));
  while (!list.empty() && (list.back() == ' ' || list.back() == '\t')) {
    list.remove_suffix(1);
  }
  return list;
}

bool all_signed(std::string_view signed_list,
                const std::vector<std::string>& names)
{
  for (const auto& name : names) {
    bool found = false;
    std::string_view rest = signed_list;
    while (!rest.empty() && !found) {
      const auto semi = rest.find(';');
      found = rest.substr(0, semi) == name;
      rest = semi == std::string_view::npos ? std::string_view{}
                                            : rest.substr(semi + 1);
    }
    if (!found) {
      return false;
    }
  }
  return true;
}

void landed_ranges::add(uint64_t ofs, uint64_t len)
{
  if (len == 0) {
    return;
  }
  uint64_t start = ofs;
  uint64_t stop = ofs + len;
  auto it = ranges.upper_bound(start);
  if (it != ranges.begin()) {
    auto prev = std::prev(it);
    if (prev->second >= start) {
      start = prev->first;
      stop = std::max(stop, prev->second);
      it = ranges.erase(prev);
    }
  }
  while (it != ranges.end() && it->first <= stop) {
    stop = std::max(stop, it->second);
    it = ranges.erase(it);
  }
  ranges.emplace(start, stop);
}

uint64_t landed_ranges::prefix() const
{
  if (ranges.empty() || ranges.begin()->first != 0) {
    return 0;
  }
  return ranges.begin()->second;
}

std::optional<std::vector<push_t>> plan_pushes(uint64_t prefix, uint64_t pushed,
                                               uint64_t total, bool final,
                                               const push_policy& p)
{
  std::vector<push_t> out;
  const uint64_t max_write = std::max<uint64_t>(p.max_write, 1);
  if (!final) {
    const uint64_t stop = total - std::min(total, p.tail_hold);
    const uint64_t limit = std::min(prefix, stop);
    if (limit <= pushed || (limit - pushed < p.min_push && limit < stop)) {
      return out;
    }
    for (uint64_t o = pushed; o < limit; ) {
      const uint64_t n = std::min(limit - o, max_write);
      out.push_back({o, n, false});
      o += n;
    }
    return out;
  }
  if (prefix < total || pushed > total) {
    return std::nullopt;
  }
  uint64_t o = pushed;
  while (total - o > max_write) {
    out.push_back({o, max_write, false});
    o += max_write;
  }
  out.push_back({o, total - o, true});
  return out;
}

} // namespace rgw::rdma::rc

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"
#include "common/crc64nvme.h"

#include <algorithm>
#include <cctype>
#include <charconv>

namespace ceph::rdma {

namespace {

std::optional<uint64_t> parse_hex_field(std::string_view field)
{
  if (field.empty() || field.size() > 16) {
    return std::nullopt;
  }
  uint64_t v = 0;
  auto [ptr, ec] = std::from_chars(field.begin(), field.end(), v, 16);
  if (ec != std::errc() || ptr != field.end()) {
    return std::nullopt;
  }
  return v;
}

} // anonymous namespace

std::optional<token_window> parse_rdma_token(std::string_view token)
{
  if (token.empty() || token.size() > RDMA_TOKEN_MAX_LEN) {
    return std::nullopt;
  }
  const auto first = token.find(':');
  if (first == std::string_view::npos) {
    return std::nullopt;
  }
  const auto second = token.find(':', first + 1);
  if (second == std::string_view::npos) {
    return std::nullopt;
  }
  auto addr = parse_hex_field(token.substr(0, first));
  auto size = parse_hex_field(token.substr(first + 1, second - first - 1));
  if (!addr || !size) {
    return std::nullopt;
  }
  return token_window{*addr, *size};
}

bool is_ofi_token(std::string_view token)
{
  const auto first = token.find(':');
  if (first == std::string_view::npos) {
    return false;
  }
  const auto second = token.find(':', first + 1);
  if (second == std::string_view::npos) {
    return false;
  }
  // the third field runs to the next colon, or to the end: "ofi" and a
  // version, which no cuObject memory key can be, since 'o' is not hex
  auto tag = token.substr(second + 1);
  tag = tag.substr(0, tag.find(':'));
  if (tag.size() < 4 || tag.substr(0, 3) != "ofi") {
    return false;
  }
  return std::all_of(tag.begin() + 3, tag.end(), [](char c) {
    return c >= '0' && c <= '9';
  });
}

bool is_cuobj_descriptor(std::string_view token)
{
  return !is_ofi_token(token) && parse_rdma_token(token).has_value();
}

std::optional<uint64_t> fold_crc64_ranges(std::vector<crc_range_t> ranges)
{
  if (ranges.empty()) {
    return std::nullopt;
  }
  std::sort(ranges.begin(), ranges.end(),
	    [](const crc_range_t& a, const crc_range_t& b) {
	      return a.ofs < b.ofs;
	    });
  uint64_t crc = ranges.front().crc64;
  uint64_t next = ranges.front().ofs + ranges.front().len;
  for (size_t i = 1; i < ranges.size(); ++i) {
    const auto& r = ranges[i];
    if (r.ofs != next) {
      return std::nullopt;  // gap or overlap
    }
    crc = crc64nvme_combine(crc, r.crc64, r.len);
    next += r.len;
  }
  return crc;
}

std::vector<std::string> parse_transport_list(std::string_view list)
{
  std::vector<std::string> out;
  size_t pos = 0;
  while (pos <= list.size()) {
    const size_t end = list.find_first_of(", \t", pos);
    auto name = list.substr(pos, end == std::string_view::npos ?
			    std::string_view::npos : end - pos);
    if (!name.empty()) {
      std::string n{name};
      std::transform(n.begin(), n.end(), n.begin(),
		     [](unsigned char c) { return std::tolower(c); });
      if (std::find(out.begin(), out.end(), n) == out.end()) {
	out.push_back(std::move(n));
      }
    }
    if (end == std::string_view::npos) {
      break;
    }
    pos = end + 1;
  }
  return out;
}

} // namespace ceph::rdma

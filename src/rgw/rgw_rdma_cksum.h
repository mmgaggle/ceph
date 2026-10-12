// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <boost/algorithm/string/predicate.hpp>

#include "include/crc32c.h"
#include "rgw_cksum.h"
#include "rgw_crc_digest.h"

/**
 * Checksums of out-of-band transfers: the CRCs that the OSDs compute of
 * the bytes they deliver into, or pull out of, a client's memory, folded
 * in object order. A response carries each one the gateway has in its
 * own header, x-amz-rdma-checksum-<algorithm>, with the value rendered
 * as S3 renders x-amz-checksum-<algorithm>. Unlike S3's header, which
 * names the stored checksum of the whole object, it covers exactly the
 * bytes of this transfer: a range of a GET too.
 */
namespace rgw::rdma {

inline constexpr const char* HDR_CRC64NVME = "x-amz-rdma-checksum-crc64nvme";
inline constexpr const char* HDR_CRC32C = "x-amz-rdma-checksum-crc32c";

/// the CRCs a request asks the OSDs for
struct cksum_want {
  bool crc64nvme = false;
  bool crc32c = false;
  bool any() const { return crc64nvme || crc32c; }
};

/**
 * What a request's x-amz-rdma-checksum-algorithm asks for, given its
 * value or null: CRC64NVME, the default when the header is absent, or
 * CRC32C, in any case. -EINVAL for any other value.
 */
inline int parse_checksum_algorithm(const char* value, cksum_want* want)
{
  *want = cksum_want{};
  if (!value || boost::iequals(std::string_view(value), "CRC64NVME")) {
    want->crc64nvme = true;
    return 0;
  }
  if (boost::iequals(std::string_view(value), "CRC32C")) {
    want->crc32c = true;
    return 0;
  }
  return -EINVAL;
}

/// S3's rendering of a canonical CRC-64/NVME: base64 of its big-endian
/// bytes, as combine_crc_cksum() keeps them
inline rgw::cksum::Cksum cksum_crc64nvme(uint64_t canonical)
{
  uint64_t swapped = rgw::digest::byteswap(canonical);
  return rgw::cksum::Cksum(rgw::cksum::Type::crc64nvme,
                           reinterpret_cast<char*>(&swapped),
                           rgw::cksum::Cksum::CtorStyle::raw);
}

/// the same for a canonical CRC-32C
inline rgw::cksum::Cksum cksum_crc32c(uint32_t canonical)
{
  uint32_t swapped = rgw::digest::byteswap(canonical);
  return rgw::cksum::Cksum(rgw::cksum::Type::crc32c,
                           reinterpret_cast<char*>(&swapped),
                           rgw::cksum::Cksum::CtorStyle::raw);
}

/// the canonical CRC-32C of len bytes at p, as rgw::digest::Crc32c
/// computes it
inline uint32_t crc32c_of(const void* p, uint64_t len)
{
  const auto* b = static_cast<const unsigned char*>(p);
  uint32_t crc = 0xffffffff;
  while (len > 0) {
    const unsigned n = static_cast<unsigned>(std::min<uint64_t>(len, 1u << 30));
    crc = ceph_crc32c(crc, b, n);
    b += n;
    len -= n;
  }
  return crc ^ 0xffffffff;
}

} // namespace rgw::rdma

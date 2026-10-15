// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstddef>
#include <cstdint>

#include "include/buffer_fwd.h"

namespace ceph {

/**
 * Canonical CRC-64/NVME (a.k.a. Rocksoft) helpers.
 *
 * Values are the canonical checksum (seed 0 yields the standard
 * result, e.g. "123456789" -> 0xae8b14860a799888); callers that need
 * the AWS/at-rest byte order (rgw's stored digests) byteswap at that
 * boundary. Incremental use: pass the previous return value as crc;
 * empty input returns crc unchanged.
 *
 * Computed with ISA-L's carry-less multiply kernels when ceph-common is
 * built with ISA-L and the CPU has PCLMUL and SSE4.1 (x86-64) or PMULL
 * (aarch64), else with madler's tables, eight bytes at a time on a
 * little-endian host and one at a time on a big-endian one; the values
 * are the same.
 */
uint64_t crc64nvme(uint64_t crc, const void* data, size_t len);
uint64_t crc64nvme(const ceph::buffer::list& bl);

/**
 * crc64nvme(bl), and bl.crc32c(*crc32c) into *crc32c, in one walk:
 * each buffer of bl is read for its CRC-64/NVME and then at once for
 * its CRC-32C, so that a buffer that fits in the CPU's cache is read
 * from memory once, where a pass over all of a large bl for each CRC
 * reads it twice. The CRC-32C goes through each buffer's CRC cache, as
 * bl.crc32c() does: a buffer whose CRC-32C is cached costs only its
 * CRC-64, and one whose is not leaves it cached, for a data digest of
 * the same bytes later. A bl whose buffers are under 4 KiB on average
 * is read twice, a pass for each CRC: reaching the cache of so many
 * buffers one at a time costs more than the second read.
 */
uint64_t crc64nvme_crc32c(const ceph::buffer::list& bl, uint32_t* crc32c);

/// checksum of the concatenation A||B given crc(A), crc(B) and len(B)
uint64_t crc64nvme_combine(uint64_t crc_a, uint64_t crc_b, uint64_t len_b);

} // namespace ceph

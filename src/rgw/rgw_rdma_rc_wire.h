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

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

/**
 * Wire encoding of the hipobj-rc-v2 control protocol.
 *
 * hipobj-rc-v2 carries S3 object data over an RDMA Reliable Connection
 * (RC) queue pair. Unlike cuObject's Dynamically Connected transport,
 * where a client token alone lets any initiator write into the
 * client's memory, an RC transfer needs a paired queue pair on each
 * side, so the protocol exchanges the pairing parameters over three
 * SigV4-signed HTTP requests on a dedicated control path:
 *
 *   POST /.hipobj-rc/prepare  client token, PSN, cookie, op, target,
 *                             size, offset  ->  session id, server
 *                             token, server PSN and QP number
 *   POST /.hipobj-rc/ready    session, cookie, client QP number and
 *                             memory region  ->  the server runs the
 *                             transfer and answers with the outcome
 *   POST /.hipobj-rc/cancel   session  ->  tears the session down
 *
 * Everything here is pure: parsers and formatters for the header
 * values, with no transport or request state, so the encoding can be
 * unit tested on hosts without RDMA hardware. The protocol is defined
 * by AMD's hipObject client library; the header names and value
 * formats below mirror its reference server.
 */
namespace rgw::rdma::rc {

/// value of the x-amz-rdma-protocol header on every control request
inline constexpr std::string_view PROTOCOL = "hipobj-rc-v2";

/// control path prefix the routes live under
inline constexpr std::string_view CONTROL_PREFIX = "/.hipobj-rc";

/// value of x-amz-rdma-protocol-status on a 501 that tells the client
/// to fall back to plain HTTP
inline constexpr std::string_view STATUS_UNSUPPORTED = "unsupported";

/// largest transfer a session may describe (2^31-1 bytes)
inline constexpr uint64_t MAX_TRANSFER_SIZE = 0x7fffffff;

/// transport named by the first byte of a token
enum class transport_t : uint8_t {
  DC = 0x00,
  RC = 0x01,
};

/**
 * A fixed-width RDMA endpoint token: 44 bytes, hex-encoded as 88
 * characters. Multi-byte fields are little-endian; the GID is copied
 * verbatim. The client sends one in PREPARE naming its queue pair (and,
 * for a PUT, the memory region the server may write into); the server
 * answers with one naming its own queue pair.
 */
struct token_t {
  transport_t transport = transport_t::RC;
  uint32_t qpn = 0;
  uint8_t gid[16] = {};
  uint32_t rkey = 0;
  uint64_t addr = 0;
  uint64_t length = 0;
  uint8_t port = 1;
  uint16_t lid = 0;

  bool operator==(const token_t&) const = default;

  /// true when the GID is all zeroes (no routable peer named)
  bool gid_is_zero() const;
};

inline constexpr size_t TOKEN_BINARY_LEN = 44;
inline constexpr size_t TOKEN_HEX_LEN = 2 * TOKEN_BINARY_LEN;

/// encode as 88 lowercase hex characters
std::string encode_token(const token_t& token);

/// decode exactly 88 hex characters (either case); nullopt otherwise
std::optional<token_t> decode_token(std::string_view hex);

/// true for the all-zero token, the protocol's explicit loopback marker
bool token_is_zero(std::string_view hex);

/// session ids are 128 random bits as 32 hex characters
inline constexpr size_t SESSION_HEX_LEN = 32;
bool valid_session_id(std::string_view s);

/// packet sequence numbers are 24-bit, sent as 6 hex digits, never 0
std::optional<uint32_t> parse_psn(std::string_view s);
std::string format_psn(uint32_t psn);

/// the cookie is a 32-bit client nonce, sent as 8 hex digits, never 0;
/// it rides the transfer as the immediate value so a completion can be
/// matched to its session
std::optional<uint32_t> parse_cookie(std::string_view s);
std::string format_cookie(uint32_t cookie);

/// bare hex (no 0x prefix), up to 16 digits: QP numbers, memory region
/// addresses and remote keys
std::optional<uint64_t> parse_hex(std::string_view s);
std::string format_hex(uint64_t v);

/// unsigned decimal: sizes and offsets
std::optional<uint64_t> parse_decimal(std::string_view s);

/// the object a session transfers, from the x-amz-rdma-target header
struct target_t {
  std::string bucket;
  std::string key;
  std::string query;  ///< canonical query string, possibly empty

  bool operator==(const target_t&) const = default;
};

/// parse "/bucket/key[?query]" (bucket and key percent-encoded);
/// nullopt when the shape is wrong or an escape is malformed
std::optional<target_t> parse_target(std::string_view target);

/// build the canonical target value from its parts
std::string build_target(std::string_view bucket, std::string_view key,
                         std::string_view query = {});

/// the SignedHeaders list of a SigV4 Authorization header value
/// ("AWS4-HMAC-SHA256 Credential=..., SignedHeaders=a;b, Signature=...");
/// nullopt when the header is not SigV4 or carries no list
std::optional<std::string_view> sigv4_signed_headers(std::string_view authorization);

/// true when every name (lowercase) is an entry of the ';'-separated
/// signed-headers list. Unsigned protocol headers could be rewritten by
/// anything on the path without breaking the signature, so the control
/// routes require all x-amz-rdma-* request headers to be signed.
bool all_signed(std::string_view signed_list,
                const std::vector<std::string>& names);

/**
 * The byte ranges of a relay window that hold object data. OSDs place
 * stripes in any order and the staged path appends; the relay can push
 * only the contiguous run from the window's start.
 */
class landed_ranges {
  std::map<uint64_t, uint64_t> ranges;  ///< start -> end, merged

 public:
  void add(uint64_t ofs, uint64_t len);
  /// bytes complete from offset 0
  uint64_t prefix() const;
  size_t count() const { return ranges.size(); }
};

/// one RDMA write of window bytes [ofs, ofs+len) to the client
struct push_t {
  uint64_t ofs = 0;
  uint64_t len = 0;
  bool with_imm = false;  ///< the last write, carrying the session cookie

  bool operator==(const push_t&) const = default;
};

struct push_policy {
  /// stream in writes of at least this much, so small stripes do not
  /// become small writes
  uint64_t min_push = 1ull << 20;
  /// and at most this much, so pushing overlaps the object read
  uint64_t max_write = 8ull << 20;
  /// hold back this much of the end to ride the final write, so the
  /// completion the client waits for always carries data
  uint64_t tail_hold = 64ull << 10;
};

/**
 * The writes to post now, given the complete prefix and what was
 * already pushed. While streaming (final false) that is the prefix less
 * the held-back tail, once at least min_push is ready or the tail is
 * reached. At the end it is everything left, with the immediate on the
 * last write; nullopt then when the prefix does not cover total.
 */
std::optional<std::vector<push_t>> plan_pushes(uint64_t prefix, uint64_t pushed,
                                               uint64_t total, bool final,
                                               const push_policy& p = {});

/// value of the FINAL response's X-Amz-Rdma-Checksum header for a
/// CRC-64/NVME rendered the way S3 does (base64 of the 8 big-endian
/// bytes, 12 characters)
std::string format_checksum_crc64nvme(std::string_view armored);

} // namespace rgw::rdma::rc

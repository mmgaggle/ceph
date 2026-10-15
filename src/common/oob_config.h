// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>

#include "common/rdma_token.h"

class CephContext;

/**
 * Where a daemon runs its out-of-band transports, from oob_network,
 * oob_network_interface and oob_addr, and what it reads of the other
 * out-of-band options.
 */
namespace ceph::oob {

/// a daemon's out-of-band address, and what carries it
struct location_t {
  /// numeric address, or empty when the daemon has none
  std::string addr;
  /// the option it came from: oob_addr, oob_network or the public address
  std::string source;
  /// the network interface that carries addr, or empty
  std::string iface;
  /// the RDMA device whose RoCE GID holds addr, or under iface
  /// (InfiniBand), or empty
  std::string rdma_device;
  unsigned rdma_port = 0;
  /// that GID's index, or -1
  int gid_index = -1;

  std::string describe() const;
};

/**
 * Find the daemon's out-of-band address: oob_addr, else the first
 * address of the host in oob_network (limited to the interfaces in
 * oob_network_interface), else fallback, the daemon's public address,
 * which can be empty. Then find the interface that carries it, and the
 * RDMA device, port and GID.
 *
 * Returns 0, -EINVAL with *err for a malformed option, or -EADDRNOTAVAIL
 * with *err when no address of the host is in oob_network.
 */
int resolve(CephContext* cct, std::string_view fallback, location_t* out,
	    std::string* err);

/**
 * The RDMA device, port and GID index whose GID holds addr, from the
 * GID tables under sysfs (/sys normally): a RoCE v2 GID when one holds
 * it, else any. Returns false when none does.
 */
bool find_rdma_gid(const std::string& sysfs, const std::string& addr,
		   std::string* device, unsigned* port, int* gid_index);

/**
 * The RDMA device and port under a network interface, from sysfs, for an
 * address that no GID holds, such as an IPoIB address: the interface's
 * own device, at its dev_port plus one, or for an InfiniBand bond its
 * active slave's, else the first of its lower interfaces by name that
 * has one. A bond of bonds is followed too. Returns false when none does.
 */
bool find_rdma_port(const std::string& sysfs, const std::string& iface,
		    std::string* device, unsigned* port);

/// whether a libfabric provider runs over verbs, and so needs an RDMA
/// device
bool is_verbs_provider(std::string_view provider);

/**
 * The libfabric domain and node for a provider at loc: the RDMA device
 * and the address for verbs, the interface for uet, nothing for shm
 * and efa, and the address for others. Empty values let the provider
 * choose.
 */
void ofi_where(const location_t& loc, std::string_view provider,
	       std::string* domain, std::string* node);

/// public_addr's address, for a daemon without a messenger of its own,
/// or empty
std::string public_ip(CephContext* cct);

/// the two transport options, parsed
ceph::rdma::oob_transports read_transports(CephContext* cct);

/// oob_memory
uint64_t memory(CephContext* cct);

/**
 * Parse a cuobj_dc_key value: hexadecimal after 0x, as cufile.json
 * gives rdma_dc_key, else decimal. Returns false when it is neither, or
 * does not fit in 64 bits. Inline because the option's validator calls
 * it too, and crimson builds the options without oob_config.cc.
 */
inline bool parse_dc_key(std::string_view v, uint64_t* key)
{
  // not strtoull(), which takes a sign and leading space, and in base 0
  // reads a leading 0 as octal
  int base = 10;
  if (v.size() > 2 && v[0] == '0' && (v[1] == 'x' || v[1] == 'X')) {
    v.remove_prefix(2);
    base = 16;
  }
  uint64_t k = 0;
  const auto [end, ec] =
    std::from_chars(v.data(), v.data() + v.size(), k, base);
  if (v.empty() || ec != std::errc{} || end != v.data() + v.size()) {
    return false;
  }
  *key = k;
  return true;
}

/**
 * cuobj_dc_key, the key of every cuObject DC initiator and target.
 * Returns 0, or -EINVAL with *err when parse_dc_key() cannot read it,
 * which the option's validator should prevent.
 */
int dc_key(CephContext* cct, uint64_t* key, std::string* err);

} // namespace ceph::oob

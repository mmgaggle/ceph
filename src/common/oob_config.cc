// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/oob_config.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <vector>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/pick_address.h"
#include "msg/msg_types.h"
#include "include/ipaddr.h"
#include "include/scope_guard.h"
#include "include/str_list.h"

namespace fs = std::filesystem;

namespace ceph::oob {

namespace {

/// addr as the 16 bytes of a GID: an IPv6 address, or an IPv4 address
/// mapped into IPv6
bool addr_to_gid(const std::string& addr, uint8_t gid[16])
{
  in_addr v4;
  if (inet_pton(AF_INET, addr.c_str(), &v4) == 1) {
    std::memset(gid, 0, 16);
    gid[10] = gid[11] = 0xff;
    std::memcpy(gid + 12, &v4, 4);
    return true;
  }
  in6_addr v6;
  if (inet_pton(AF_INET6, addr.c_str(), &v6) == 1) {
    std::memcpy(gid, &v6, 16);
    return true;
  }
  return false;
}

/// a GID as sysfs writes it, "0000:0000:0000:0000:0000:ffff:0a5a:0001"
bool parse_gid(const std::string& s, uint8_t gid[16])
{
  std::string hex;
  for (char c : s) {
    if (c == ':' || c == '\n') {
      continue;
    }
    hex.push_back(c);
  }
  if (hex.size() != 32) {
    return false;
  }
  for (int i = 0; i < 16; i++) {
    unsigned v = 0;
    for (int k = 0; k < 2; k++) {
      const char c = hex[2 * i + k];
      v <<= 4;
      if (c >= '0' && c <= '9') {
	v |= c - '0';
      } else if (c >= 'a' && c <= 'f') {
	v |= c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
	v |= c - 'A' + 10;
      } else {
	return false;
      }
    }
    gid[i] = static_cast<uint8_t>(v);
  }
  return true;
}

std::string read_line(const fs::path& p)
{
  std::ifstream in(p);
  std::string s;
  if (in) {
    std::getline(in, s);
  }
  return s;
}

/// the numeric entries of a directory, in order
std::vector<unsigned> numbered(const fs::path& dir)
{
  std::vector<unsigned> out;
  std::error_code ec;
  for (const auto& e : fs::directory_iterator(dir, ec)) {
    const auto n = e.path().filename().string();
    if (!n.empty() && std::all_of(n.begin(), n.end(), [](unsigned char c) { return std::isdigit(c); })) {
      out.push_back(static_cast<unsigned>(std::stoul(n)));
    }
  }
  std::sort(out.begin(), out.end());
  return out;
}

/// the RDMA device of a network interface, from sysfs, or empty
std::string device_of_iface(const std::string& sysfs, const std::string& iface)
{
  std::error_code ec;
  std::vector<std::string> devs;
  for (const auto& e : fs::directory_iterator(
	 fs::path(sysfs) / "class/net" / iface / "device/infiniband", ec)) {
    devs.push_back(e.path().filename().string());
  }
  std::sort(devs.begin(), devs.end());
  return devs.empty() ? std::string{} : devs.front();
}

/// what is wrong with oob_network, empty when nothing is: an entry that
/// is not a subnet, on which find_ip_in_subnet_list() would exit rather
/// than return, or no entry, which would match every address (and with
/// oob_network_interface exit too). oob_network_interface needs no
/// check: a name that no interface has matches none
std::string check_networks(const std::string& nets)
{
  const auto subnets = get_str_list(nets);
  if (subnets.empty()) {
    return "oob_network '" + nets + "' names no subnet";
  }
  for (const auto& n : subnets) {
    sockaddr_storage net;
    unsigned prefix_len;
    if (!parse_network(n.c_str(), &net, &prefix_len)) {
      return "oob_network entry '" + n + "' is not a subnet, such as "
	"10.0.0.0/24 or fd00::/64";
    }
  }
  return {};
}

/// the interface that carries addr, or empty
std::string iface_of_addr(const std::string& addr)
{
  uint8_t want[16];
  if (!addr_to_gid(addr, want)) {
    return {};
  }
  ifaddrs* ifa = nullptr;
  if (getifaddrs(&ifa) < 0) {
    return {};
  }
  auto free_ifa = make_scope_guard([ifa] { freeifaddrs(ifa); });
  for (auto* a = ifa; a; a = a->ifa_next) {
    if (!a->ifa_addr) {
      continue;
    }
    char buf[INET6_ADDRSTRLEN];
    const void* raw = nullptr;
    if (a->ifa_addr->sa_family == AF_INET) {
      raw = &reinterpret_cast<const sockaddr_in*>(a->ifa_addr)->sin_addr;
    } else if (a->ifa_addr->sa_family == AF_INET6) {
      raw = &reinterpret_cast<const sockaddr_in6*>(a->ifa_addr)->sin6_addr;
    } else {
      continue;
    }
    uint8_t have[16];
    if (inet_ntop(a->ifa_addr->sa_family, raw, buf, sizeof(buf)) &&
	addr_to_gid(buf, have) && std::memcmp(have, want, 16) == 0) {
      return a->ifa_name;
    }
  }
  return {};
}

} // anonymous namespace

std::string location_t::describe() const
{
  if (addr.empty()) {
    return "no out-of-band address";
  }
  std::string s = addr + " (" + source + ")";
  if (!iface.empty()) {
    s += " on " + iface;
  }
  if (!rdma_device.empty()) {
    s += ", RDMA device " + rdma_device + " port " +
      std::to_string(rdma_port) + " GID " + std::to_string(gid_index);
  }
  return s;
}

bool find_rdma_gid(const std::string& sysfs, const std::string& addr,
		   std::string* device, unsigned* port, int* gid_index)
{
  uint8_t want[16];
  if (!addr_to_gid(addr, want)) {
    return false;
  }
  std::error_code ec;
  std::vector<std::string> devs;
  for (const auto& e : fs::directory_iterator(
	 fs::path(sysfs) / "class/infiniband", ec)) {
    devs.push_back(e.path().filename().string());
  }
  std::sort(devs.begin(), devs.end());
  // a RoCE v2 GID first: it is the one that routes
  for (const bool v2_only : {true, false}) {
    for (const auto& d : devs) {
      const auto pdir = fs::path(sysfs) / "class/infiniband" / d / "ports";
      for (const unsigned p : numbered(pdir)) {
	const auto gdir = pdir / std::to_string(p);
	for (const unsigned i : numbered(gdir / "gids")) {
	  uint8_t gid[16];
	  if (!parse_gid(read_line(gdir / "gids" / std::to_string(i)), gid) ||
	      std::memcmp(gid, want, 16) != 0) {
	    continue;
	  }
	  const auto type =
	    read_line(gdir / "gid_attrs/types" / std::to_string(i));
	  if (v2_only && type != "RoCE v2") {
	    continue;
	  }
	  *device = d;
	  *port = p;
	  *gid_index = static_cast<int>(i);
	  return true;
	}
      }
    }
  }
  return false;
}

int resolve(CephContext* cct, std::string_view fallback, location_t* out,
	    std::string* err)
{
  *out = location_t{};
  const auto& conf = cct->_conf;
  const auto addr = conf.get_val<std::string>("oob_addr");
  const auto nets = conf.get_val<std::string>("oob_network");
  const auto ifaces = conf.get_val<std::string>("oob_network_interface");
  if (!addr.empty()) {
    uint8_t g[16];
    if (!addr_to_gid(addr, g)) {
      *err = "oob_addr '" + addr + "' is not an IP address";
      return -EINVAL;
    }
    out->addr = addr;
    out->source = "oob_addr";
  } else if (!nets.empty()) {
    if (auto bad = check_networks(nets); !bad.empty()) {
      *err = std::move(bad);
      return -EINVAL;
    }
    ifaddrs* ifa = nullptr;
    if (getifaddrs(&ifa) < 0) {
      const int e = errno;
      *err = std::string("getifaddrs: ") + std::strerror(e);
      return -e;
    }
    auto free_ifa = make_scope_guard([ifa] { freeifaddrs(ifa); });
    const sockaddr* sa = find_ip_in_subnet_list(
      cct, ifa, CEPH_PICK_ADDRESS_IPV4 | CEPH_PICK_ADDRESS_IPV6, nets,
      ifaces, -1);
    if (!sa) {
      *err = "no address of this host is in oob_network " + nets +
	(ifaces.empty() ? std::string{} :
	 " on the interfaces " + ifaces);
      return -EADDRNOTAVAIL;
    }
    char buf[INET6_ADDRSTRLEN];
    const void* raw = sa->sa_family == AF_INET ?
      static_cast<const void*>(
	&reinterpret_cast<const sockaddr_in*>(sa)->sin_addr) :
      static_cast<const void*>(
	&reinterpret_cast<const sockaddr_in6*>(sa)->sin6_addr);
    if (!inet_ntop(sa->sa_family, raw, buf, sizeof(buf))) {
      *err = std::string("inet_ntop: ") + std::strerror(errno);
      return -EINVAL;
    }
    out->addr = buf;
    out->source = "oob_network";
  } else if (!ifaces.empty()) {
    *err = "oob_network_interface needs oob_network";
    return -EINVAL;
  } else if (!fallback.empty()) {
    out->addr = std::string(fallback);
    out->source = "public address";
  } else {
    return 0;
  }
  out->iface = iface_of_addr(out->addr);
  if (!find_rdma_gid("/sys", out->addr, &out->rdma_device, &out->rdma_port,
		     &out->gid_index) &&
      !out->iface.empty()) {
    // InfiniBand: the GIDs do not hold the IPoIB address, but the
    // interface names its device
    out->rdma_device = device_of_iface("/sys", out->iface);
    if (!out->rdma_device.empty()) {
      out->rdma_port = 1;
    }
  }
  return 0;
}

bool is_verbs_provider(std::string_view provider)
{
  return provider == "verbs" || provider.substr(0, 6) == "verbs;";
}

void ofi_where(const location_t& loc, std::string_view provider,
	       std::string* domain, std::string* node)
{
  domain->clear();
  node->clear();
  if (is_verbs_provider(provider)) {
    *domain = loc.rdma_device;
    *node = loc.addr;
  } else if (provider == "uet") {
    *domain = loc.iface;
  } else if (provider != "shm" && provider != "efa") {
    *node = loc.addr;
  }
}

std::string public_ip(CephContext* cct)
{
  const auto a = cct->_conf.get_val<entity_addr_t>("public_addr");
  return a.is_blank_ip() ? std::string{} : a.ip_only_to_str();
}

ceph::rdma::oob_transports read_transports(CephContext* cct)
{
  return ceph::rdma::parse_oob_transports(
    cct->_conf.get_val<std::string>("oob_client_transports"),
    cct->_conf.get_val<std::string>("oob_cluster_transport"));
}

uint64_t memory(CephContext* cct)
{
  return cct->_conf.get_val<Option::size_t>("oob_memory");
}

} // namespace ceph::oob

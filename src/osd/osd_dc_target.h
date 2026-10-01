// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "include/common_fwd.h"

struct ibv_context;
struct ibv_pd;
struct ibv_mr;
struct ibv_cq;
struct ibv_srq;
struct ibv_qp;

/**
 * A cuObject Dynamically Connected target over one registered pool.
 *
 * When this OSD is the primary of an erasure-coded read gathered out of
 * band, its peers push their shard data with the cuObject server library,
 * which writes to DC targets. This exposes the pool as one: a DCT on the
 * mlx5 device whose GID carries the OSD's RDMA address, with the pool
 * registered for remote writes. token() names a window of the pool in
 * the descriptor form the cuObject server parses,
 * "raddr:rsize:rkey:lid:dctn:has_gid:gid". The same bring-up serves the
 * gateway's relay target (rgw_rdma_dc_target).
 */
class OSDDcTarget {
public:
  OSDDcTarget() = default;
  ~OSDDcTarget();
  OSDDcTarget(const OSDDcTarget&) = delete;
  OSDDcTarget& operator=(const OSDDcTarget&) = delete;

  /// open the device carrying rdma_ip, register pool_size bytes and
  /// bring the DCT to RTR; negative errno on failure
  int open(CephContext* cct, const std::string& rdma_ip, size_t pool_size,
	   uint64_t dc_key);

  char* pool() const { return m_pool; }
  size_t pool_size() const { return m_pool_size; }

  /// descriptor for the window [ofs, ofs+len) of the pool
  std::string token(size_t ofs, size_t len) const;

private:
  void close();

  ibv_context* m_ctx = nullptr;
  ibv_pd* m_pd = nullptr;
  ibv_mr* m_mr = nullptr;
  ibv_cq* m_cq = nullptr;
  ibv_srq* m_srq = nullptr;
  ibv_qp* m_dct = nullptr;
  char* m_pool = nullptr;
  size_t m_pool_size = 0;
  uint32_t m_dctn = 0;
  uint16_t m_lid = 0;
  uint8_t m_gid[16] = {};
};

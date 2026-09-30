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

#include "rgw_rdma_dc_target.h"

// Provenance: the DCT bring-up and the descriptor format follow the
// host-memory DC client in versitygw's cuwrapper (Apache-2.0), which
// mirrors what NVIDIA's cuObject client library registers.

#include <cerrno>
#include <cstring>

#include <infiniband/mlx5dv.h>
#include <fmt/format.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::rdma::dc {

Target::~Target()
{
  close();
}

int Target::open(CephContext* cct, rc::Device& dev, uint64_t dc_key)
{
  cq = ibv_create_cq(dev.context(), 1, nullptr, nullptr, 0);
  if (!cq) {
    lderr(cct) << "rgw_rdma_dc: ibv_create_cq failed: " << cpp_strerror(errno)
               << dendl;
    return -EIO;
  }
  ibv_srq_init_attr srq_attr;
  std::memset(&srq_attr, 0, sizeof(srq_attr));
  srq_attr.attr.max_wr = 1;
  srq_attr.attr.max_sge = 1;
  srq = ibv_create_srq(dev.protection_domain(), &srq_attr);
  if (!srq) {
    lderr(cct) << "rgw_rdma_dc: ibv_create_srq failed: " << cpp_strerror(errno)
               << dendl;
    close();
    return -EIO;
  }

  ibv_qp_init_attr_ex attr_ex;
  std::memset(&attr_ex, 0, sizeof(attr_ex));
  attr_ex.pd = dev.protection_domain();
  attr_ex.send_cq = cq;
  attr_ex.recv_cq = cq;
  attr_ex.srq = srq;
  attr_ex.qp_type = IBV_QPT_DRIVER;
  attr_ex.comp_mask = IBV_QP_INIT_ATTR_PD;

  mlx5dv_qp_init_attr dv_attr;
  std::memset(&dv_attr, 0, sizeof(dv_attr));
  dv_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
  dv_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
  dv_attr.dc_init_attr.dct_access_key = dc_key;

  dct = mlx5dv_create_qp(dev.context(), &attr_ex, &dv_attr);
  if (!dct) {
    // expected on anything but mlx5: DC is a ConnectX feature
    ldout(cct, 1) << "rgw_rdma_dc: mlx5dv_create_qp (DCT) failed on "
                  << dev.name() << ": " << cpp_strerror(errno) << dendl;
    close();
    return -EOPNOTSUPP;
  }

  ibv_qp_attr qpa;
  std::memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_INIT;
  qpa.pkey_index = 0;
  qpa.port_num = dev.port_num();
  qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
  if (ibv_modify_qp(dct, &qpa, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                    IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
    lderr(cct) << "rgw_rdma_dc: DCT -> INIT failed: " << cpp_strerror(errno)
               << dendl;
    close();
    return -EIO;
  }

  // a target only needs INIT -> RTR
  std::memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_RTR;
  qpa.path_mtu = dev.path_mtu();
  qpa.min_rnr_timer = 12;
  qpa.ah_attr.is_global = 1;
  qpa.ah_attr.port_num = dev.port_num();
  qpa.ah_attr.grh.hop_limit = 4;
  qpa.ah_attr.grh.sgid_index = static_cast<uint8_t>(dev.gid_idx());
  qpa.ah_attr.grh.traffic_class = 0;
  if (ibv_modify_qp(dct, &qpa, IBV_QP_STATE | IBV_QP_MIN_RNR_TIMER |
                    IBV_QP_AV | IBV_QP_PATH_MTU)) {
    lderr(cct) << "rgw_rdma_dc: DCT -> RTR failed: " << cpp_strerror(errno)
               << dendl;
    close();
    return -EIO;
  }

  dct_num = dct->qp_num;
  if (dct_num == 0) {
    lderr(cct) << "rgw_rdma_dc: DCT number is zero after RTR" << dendl;
    close();
    return -EIO;
  }
  lid = dev.lid();
  gid = dev.gid();
  ldout(cct, 1) << "rgw_rdma_dc: DC target dctn=" << dct_num << " on "
                << dev.name() << dendl;
  return 0;
}

void Target::close()
{
  if (dct) {
    ibv_destroy_qp(dct);
    dct = nullptr;
  }
  if (srq) {
    ibv_destroy_srq(srq);
    srq = nullptr;
  }
  if (cq) {
    ibv_destroy_cq(cq);
    cq = nullptr;
  }
  dct_num = 0;
}

std::string Target::token(const void* addr, uint32_t size, uint32_t rkey) const
{
  std::string gid_hex;
  gid_hex.reserve(32);
  for (uint8_t b : gid.raw) {
    gid_hex += fmt::format("{:02x}", b);
  }
  return fmt::format("{:016x}:{:08x}:{:08x}:{:04x}:{:06x}:1:{}",
                     reinterpret_cast<uintptr_t>(addr), size, rkey, lid,
                     dct_num, gid_hex);
}

} // namespace rgw::rdma::dc

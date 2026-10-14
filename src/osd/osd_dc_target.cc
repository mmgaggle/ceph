// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

// Provenance: the DCT bring-up and the descriptor format follow the
// host-memory DC client in versitygw's cuwrapper (Apache-2.0), as the
// gateway's relay target does.

#include "osd/osd_dc_target.h"

#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>

#include <fmt/format.h>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/oob_config.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_dc_target: "

OSDDcTarget::~OSDDcTarget()
{
  close();
}

void OSDDcTarget::close()
{
  if (m_dct) ibv_destroy_qp(m_dct);
  if (m_srq) ibv_destroy_srq(m_srq);
  if (m_cq) ibv_destroy_cq(m_cq);
  if (m_mr) ibv_dereg_mr(m_mr);
  if (m_pd) ibv_dealloc_pd(m_pd);
  if (m_ctx) ibv_close_device(m_ctx);
  m_dct = nullptr; m_srq = nullptr; m_cq = nullptr; m_mr = nullptr;
  m_pd = nullptr; m_ctx = nullptr; m_pool = nullptr;
}

int OSDDcTarget::open(CephContext* cct, const std::string& rdma_ip,
		      char* pool, size_t pool_size, uint64_t dc_key)
{
  // the device, port and GID that carry our RDMA address, as the OSD's
  // out-of-band location names them: a RoCE v2 GID when one holds the
  // address, since only v2 routes, and an IPv6 address's own GID
  std::string dev_name;
  unsigned port = 0;
  int gid_index = -1;
  if (!ceph::oob::find_rdma_gid("/sys", rdma_ip, &dev_name, &port,
				&gid_index)) {
    derr << "no RDMA device carries " << rdma_ip << dendl;
    return -ENODEV;
  }
  int num = 0;
  ibv_device** list = ibv_get_device_list(&num);
  for (int i = 0; list && i < num && !m_ctx; i++) {
    if (dev_name == ibv_get_device_name(list[i])) {
      m_ctx = ibv_open_device(list[i]);
    }
  }
  if (list) {
    ibv_free_device_list(list);
  }
  if (!m_ctx) {
    derr << "cannot open RDMA device " << dev_name << ", which carries "
	 << rdma_ip << dendl;
    return -ENODEV;
  }
  ibv_port_attr pa;
  ibv_gid gid;
  if (int r = ibv_query_port(m_ctx, port, &pa); r != 0) {
    derr << "querying " << dev_name << " port " << port << ": "
	 << cpp_strerror(r) << dendl;
    close();
    return -EIO;
  }
  if (ibv_query_gid(m_ctx, port, gid_index, &gid) != 0) {
    derr << "querying GID " << gid_index << " of " << dev_name << " port "
	 << port << ": " << cpp_strerror(errno) << dendl;
    close();
    return -EIO;
  }
  m_lid = pa.lid;
  std::memcpy(m_gid, gid.raw, 16);

  m_pool_size = pool_size;
  m_pool = pool;
  if (!m_pool || !(m_pd = ibv_alloc_pd(m_ctx))) {
    close();
    return -ENOMEM;
  }
  // peers only write into the windows: none may read back what a
  // window holds, another client's gathered data perhaps
  m_mr = ibv_reg_mr(m_pd, m_pool, pool_size,
		    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  m_cq = ibv_create_cq(m_ctx, 1, nullptr, nullptr, 0);
  ibv_srq_init_attr srq_attr;
  std::memset(&srq_attr, 0, sizeof(srq_attr));
  srq_attr.attr.max_wr = 1;
  srq_attr.attr.max_sge = 1;
  m_srq = m_cq ? ibv_create_srq(m_pd, &srq_attr) : nullptr;
  if (!m_mr || !m_cq || !m_srq) {
    derr << "window pool setup failed: " << cpp_strerror(errno) << dendl;
    close();
    return -EIO;
  }

  ibv_qp_init_attr_ex attr_ex;
  std::memset(&attr_ex, 0, sizeof(attr_ex));
  attr_ex.pd = m_pd;
  attr_ex.send_cq = m_cq;
  attr_ex.recv_cq = m_cq;
  attr_ex.srq = m_srq;
  attr_ex.qp_type = IBV_QPT_DRIVER;
  attr_ex.comp_mask = IBV_QP_INIT_ATTR_PD;
  mlx5dv_qp_init_attr dv_attr;
  std::memset(&dv_attr, 0, sizeof(dv_attr));
  dv_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
  dv_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
  dv_attr.dc_init_attr.dct_access_key = dc_key;
  m_dct = mlx5dv_create_qp(m_ctx, &attr_ex, &dv_attr);
  if (!m_dct) {
    derr << "mlx5dv_create_qp (DCT): " << cpp_strerror(errno) << dendl;
    close();
    return -EOPNOTSUPP;
  }
  ibv_qp_attr qpa;
  std::memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_INIT;
  qpa.port_num = static_cast<uint8_t>(port);
  qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
  if (ibv_modify_qp(m_dct, &qpa, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
		    IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
    derr << "DCT -> INIT: " << cpp_strerror(errno) << dendl;
    close();
    return -EIO;
  }
  std::memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_RTR;
  qpa.path_mtu = pa.active_mtu;
  qpa.min_rnr_timer = 12;
  qpa.ah_attr.is_global = 1;
  qpa.ah_attr.port_num = static_cast<uint8_t>(port);
  // the DCT's ACKs carry this as their RoCE v2 TTL; leave room for a
  // routed multi-tier fabric, as the gateway's RC transport does
  qpa.ah_attr.grh.hop_limit = 64;
  qpa.ah_attr.grh.sgid_index = static_cast<uint8_t>(gid_index);
  if (ibv_modify_qp(m_dct, &qpa, IBV_QP_STATE | IBV_QP_MIN_RNR_TIMER |
		    IBV_QP_AV | IBV_QP_PATH_MTU)) {
    derr << "DCT -> RTR: " << cpp_strerror(errno) << dendl;
    close();
    return -EIO;
  }
  m_dctn = m_dct->qp_num;
  dout(1) << "DC target dctn=" << m_dctn << " on " << rdma_ip << " ("
	  << dev_name << " port " << port << " GID " << gid_index
	  << "), pool " << pool_size << " bytes" << dendl;
  return 0;
}

std::string OSDDcTarget::token(size_t ofs, size_t len) const
{
  std::string gid_hex;
  for (uint8_t b : m_gid) {
    gid_hex += fmt::format("{:02x}", b);
  }
  return fmt::format("{:016x}:{:08x}:{:08x}:{:04x}:{:06x}:1:{}",
		     reinterpret_cast<uintptr_t>(m_pool + ofs),
		     static_cast<uint32_t>(len), m_mr->rkey, m_lid, m_dctn,
		     gid_hex);
}

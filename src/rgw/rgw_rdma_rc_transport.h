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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

#include <infiniband/verbs.h>

#include "include/common_fwd.h"

/**
 * Reliable Connection transport for the hipobj-rc-v2 data phase.
 *
 * One Device is opened at startup: the verbs context, a protection
 * domain, and the port topology (GID, LID) that every session shares.
 * Each session owns one Connection: a completion queue and an RC queue
 * pair that PREPARE leaves in INIT and READY pairs against the client's
 * queue pair before the transfer. Memory the sessions transfer from is
 * registered once on the device's protection domain, so the same
 * registration also serves a DC target created on it (see
 * rgw_rdma_dc_target.h).
 */
namespace rgw::rdma::rc {

struct DeviceConfig {
  std::string device_name;  ///< verbs device, e.g. mlx5_0; empty = first
  std::string gid_hint;     ///< dotted GID prefix selecting the device
  uint8_t port = 1;
  int gid_index = -1;       ///< -1 = pick a RoCEv2 (or the port) GID
};

class Device {
 public:
  Device() = default;
  ~Device();
  Device(const Device&) = delete;
  Device& operator=(const Device&) = delete;

  /// open the device and the protection domain; negative errno on failure
  int open(CephContext* cct, const DeviceConfig& cfg);
  void close();

  bool is_open() const { return ctx != nullptr; }

  ibv_context* context() const { return ctx; }
  ibv_pd* protection_domain() const { return pd; }
  uint8_t port_num() const { return port; }
  int gid_idx() const { return gid_index; }
  const ibv_gid& gid() const { return local_gid; }
  uint16_t lid() const { return local_lid; }
  ibv_mtu path_mtu() const { return mtu; }
  const std::string& name() const { return dev_name; }

  /// register [ptr, ptr+len) for local and remote read/write access
  ibv_mr* register_memory(void* ptr, size_t len);
  static void deregister_memory(ibv_mr* mr);

  /// ibv_post_send() that works from any thread. See post_proxy.
  int post_send(ibv_qp* qp, ibv_send_wr* wr, ibv_send_wr** bad);

 private:
  int select_gid(CephContext* cct, int wanted);
  void start_post_proxy();
  void stop_post_proxy();

  /**
   * Software providers (soft-RoCE, siw) ring the send doorbell with a
   * write() on the device file, and the kernel refuses that write from
   * a thread whose credentials are not the very ones that opened the
   * file. radosgw request threads never qualify: setuid() after the
   * frontends bind (--setuser) gives every thread its own credentials,
   * and so does creating the process keyring after the thread pool is
   * up. Hardware providers ring doorbells through mapped memory and
   * post directly. For software providers every post goes through one
   * thread started by the opener, which shares its credentials.
   */
  struct post_job {
    ibv_qp* qp;
    ibv_send_wr* wr;
    ibv_send_wr** bad;
    int result = 0;
    bool done = false;
  };
  bool post_proxy = false;
  std::thread proxy;
  std::mutex proxy_mtx;
  std::condition_variable proxy_cv;
  std::condition_variable proxy_done;
  std::deque<post_job*> proxy_jobs;
  bool proxy_stop = false;

  ibv_context* ctx = nullptr;
  ibv_pd* pd = nullptr;
  uint8_t port = 1;
  int gid_index = 0;
  ibv_gid local_gid = {};
  uint16_t local_lid = 0;
  ibv_mtu mtu = IBV_MTU_1024;
  std::string dev_name;
};

/// the peer a Connection is paired against
struct PeerEndpoint {
  uint32_t qpn = 0;
  uint32_t psn = 0;       ///< the peer's send PSN (our receive PSN)
  ibv_gid gid = {};
  uint16_t lid = 0;       ///< 0 on RoCE, where the GRH routes alone
};

/// outcome of waiting for one completion
enum class Completion {
  OK,
  TIMEOUT,     ///< nothing completed before the deadline
  BUSY,        ///< the peer was not ready (RNR or retry exhaustion)
  WIRE_ERROR,  ///< any other failed completion or a poll error
};

class Connection {
 public:
  Connection() = default;
  ~Connection();
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  /// create the queue pair on dev and move it to INIT; dev must
  /// outlive the connection
  int create(CephContext* cct, Device& dev, uint32_t send_depth,
             uint32_t recv_depth);
  void destroy();

  /// INIT -> RTR against the peer -> RTS with our send PSN
  int pair(CephContext* cct, Device& dev, const PeerEndpoint& peer,
           uint32_t local_psn);
  /// back to INIT through RESET so a later pair() can retry
  int rearm(CephContext* cct, Device& dev);

  uint32_t qpn() const { return qp ? qp->qp_num : 0; }
  bool created() const { return qp != nullptr; }

  /// RDMA write [ptr, ptr+len) of mr into the peer's (remote_addr,
  /// rkey); with_imm attaches the immediate (network byte order is
  /// applied here) and signals a completion on the peer
  int post_write(ibv_mr* mr, const void* ptr, uint32_t len,
                 uint64_t remote_addr, uint32_t rkey,
                 bool with_imm, uint32_t imm, uint64_t wr_id,
                 bool signaled);

  /// receive buffer for the peer's write-with-immediate
  int post_recv(ibv_mr* mr, void* ptr, uint32_t len, uint64_t wr_id);

  /// wait for one completion; on OK, wc holds it
  Completion poll_one(ibv_wc& wc, std::chrono::steady_clock::time_point deadline);

  /// count of signaled sends not yet reaped by poll_one()
  uint32_t outstanding() const { return inflight; }
  void reaped() { if (inflight) --inflight; }

 private:
  Device* dev = nullptr;
  ibv_cq* cq = nullptr;
  ibv_qp* qp = nullptr;
  uint32_t inflight = 0;
};

} // namespace rgw::rdma::rc

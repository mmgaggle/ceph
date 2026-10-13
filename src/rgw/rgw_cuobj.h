// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "include/common_fwd.h"

struct rdma_buffer;
namespace ceph::oob { class Arena; }
class cuObjServer;

class RGWCuObjServer {
public:
  /// a staging buffer: a buffer of the arena, which the library knows
  /// as offset in the arena's registration, handle
  struct RDMABufEntry {
    void* ptr = nullptr;
    size_t size = 0;
    uint64_t offset = 0;
    struct rdma_buffer* handle = nullptr;
  };

  ~RGWCuObjServer();

  /// arena: the gateway's out-of-band memory, which the server registers
  /// once and stages each transfer in; it must outlive the server
  static int init(CephContext* cct, ceph::oob::Arena* arena);
  static void shutdown();
  static RGWCuObjServer* get_instance();

  /// the server is up, and the calling thread has a channel, which this
  /// allocates on the thread's first call
  bool is_available();

  /// the calling thread's channel (DC initiator), allocated on first
  /// use; invalid_channel when the library has none left
  uint16_t get_channel_id();
  void release_channel_id();
  static constexpr uint16_t invalid_channel = UINT16_MAX;

  /// a buffer of the arena of at least needed_size bytes, or null when
  /// the arena has none free, none that large, or the calling thread no
  /// channel
  RDMABufEntry* acquire_buffer(size_t needed_size);
  void release_buffer(RDMABufEntry* buf);

  static size_t parse_rdma_descriptor_size(const std::string& rdma_descr);

  ssize_t rdma_read_from_client(const std::string& key,
                                RDMABufEntry* buf,
                                uint64_t remote_offset,
                                size_t size,
                                const std::string& rdma_descr);

  ssize_t rdma_write_to_client(const std::string& key,
                               RDMABufEntry* buf,
                               uint64_t remote_offset,
                               size_t size,
                               const std::string& rdma_descr);

private:
  RGWCuObjServer() = default;

  int do_init(CephContext* cct);
  void do_shutdown();

  std::unique_ptr<cuObjServer> m_server;
  ceph::oob::Arena* m_arena = nullptr;
  /// the arena, registered with the library once
  struct rdma_buffer* m_arena_handle = nullptr;
  CephContext* m_cct = nullptr;
  /// DC initiators: one for each frontend thread
  int m_num_dcis = 0;

  static std::unique_ptr<RGWCuObjServer> s_instance;
  static thread_local uint16_t tls_channel_id;
  static thread_local bool tls_channel_valid;
  /// the library had no channel left for this thread; reported once
  static thread_local bool tls_channel_refused;
};

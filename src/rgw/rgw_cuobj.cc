// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_cuobj.h"

#include <cuobjserver.h>

#include <algorithm>
#include <cstdlib>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/oob_arena.h"
#include "common/oob_config.h"
#include "common/rdma_token.h"

#define dout_subsys ceph_subsys_rgw

std::unique_ptr<RGWCuObjServer> RGWCuObjServer::s_instance;
thread_local uint16_t RGWCuObjServer::tls_channel_id = 0;
thread_local bool RGWCuObjServer::tls_channel_valid = false;
thread_local bool RGWCuObjServer::tls_channel_refused = false;

static constexpr size_t MAX_RDMA_OP_SIZE = 1ULL << 30; // 1 GiB per cuObj API
// DC initiators, at the least
static constexpr int MIN_DCIS = 128;

RGWCuObjServer::~RGWCuObjServer()
{
  do_shutdown();
}

int RGWCuObjServer::init(CephContext* cct, ceph::oob::Arena* arena)
{
  if (s_instance) {
    return 0;
  }
  s_instance.reset(new RGWCuObjServer());
  s_instance->m_arena = arena;
  int r = s_instance->do_init(cct);
  if (r < 0) {
    s_instance.reset();
  }
  return r;
}

void RGWCuObjServer::shutdown()
{
  s_instance.reset();
}

RGWCuObjServer* RGWCuObjServer::get_instance()
{
  return s_instance.get();
}

int RGWCuObjServer::do_init(CephContext* cct)
{
  m_cct = cct;

  ceph::oob::location_t loc;
  std::string err;
  if (int r = ceph::oob::resolve(cct, ceph::oob::public_ip(cct), &loc, &err); r < 0) {
    lderr(cct) << "rgw_cuobj: ERROR: " << err << dendl;
    return r;
  }
  if (loc.addr.empty()) {
    lderr(cct) << "rgw_cuobj: ERROR: no out-of-band address: set oob_network"
               << dendl;
    return -EINVAL;
  }
  const std::string rdma_ip = loc.addr;

  // the library chooses its rdma_cm port; no client connects to it
  const unsigned short rdma_port = 0;
  // a DC initiator for each frontend thread: each thread keeps the
  // channel that it takes on its first transfer, and beast moves a
  // request from thread to thread as it waits, so a request that staged
  // on one thread can write to the client from any other
  m_num_dcis = std::max<int>(MIN_DCIS, cct->_conf->rgw_thread_pool_size);

  cuObjRDMATunable params;
  params.setNumDcis(m_num_dcis);
  params.setDcKey(cct->_conf.get_val<uint64_t>("cuobj_dc_key"));

  ldout(cct, 1) << "rgw_cuobj: initializing cuObjServer on "
                << rdma_ip << ":" << rdma_port
                << " dcis=" << m_num_dcis << ", staging in "
                << m_arena->size() << " bytes" << dendl;

  try {
    m_server = std::make_unique<cuObjServer>(rdma_ip.c_str(), rdma_port, CUOBJ_PROTO_RDMA_DC_V1, params);
  } catch (const std::exception& e) {
    lderr(cct) << "rgw_cuobj: ERROR: cuObjServer construction failed: " << e.what() << dendl;
    return -EIO;
  }

  cuObjServer::setupTelemetry(false, &std::cerr);
  cuObjServer::setTelemFlags(CUOBJ_LOG_PATH_INFO | CUOBJ_LOG_PATH_DEBUG | CUOBJ_LOG_PATH_ERROR);

  if (!m_server->isConnected()) {
    lderr(cct) << "rgw_cuobj: ERROR: cuObjServer failed to connect" << dendl;
    m_server.reset();
    return -ECONNREFUSED;
  }

  // the whole arena, once: each transfer stages in a buffer of it, which
  // the library addresses by its offset
  m_arena_handle = m_server->registerBuffer(m_arena->base(), m_arena->size());
  if (!m_arena_handle) {
    lderr(cct) << "rgw_cuobj: ERROR: registerBuffer failed for "
               << m_arena->size() << " bytes of out-of-band memory" << dendl;
    do_shutdown();
    return -EIO;
  }
  ldout(cct, 1) << "rgw_cuobj: staging in " << m_arena->size()
                << " bytes of out-of-band memory" << dendl;
  return 0;
}

void RGWCuObjServer::do_shutdown()
{
  ldout(m_cct, 1) << "rgw_cuobj: shutting down cuObjServer" << dendl;
  // the arena, which holds the memory, goes after the server
  if (m_arena_handle && m_server) {
    m_server->deRegisterBuffer(m_arena_handle);
  }
  m_arena_handle = nullptr;
  m_server.reset();
}

bool RGWCuObjServer::is_available()
{
  // a frontend thread that the library has no channel for serves its
  // requests over HTTP: asked before the first transfer on the thread
  // too, so that a PUT without a body is declined rather than failed
  return m_server && m_server->isConnected() &&
    get_channel_id() != invalid_channel;
}

// descriptor token: leading "addr:size:" hex fields, remainder opaque;
// see common/rdma_token.h
size_t RGWCuObjServer::parse_rdma_descriptor_size(const std::string& rdma_descr)
{
  auto window = ceph::rdma::parse_rdma_token(rdma_descr);
  if (!window) {
    lderr(g_ceph_context) << "rgw_cuobj: ERROR: failed to parse RDMA descriptor" << dendl;
    return 0;
  }
  ldout(g_ceph_context, 21) << "rgw_cuobj: parsed size from RDMA descriptor: "
                            << window->size << dendl;
  return window->size;
}

static uint64_t parse_rdma_descriptor_addr(const std::string& rdma_descr)
{
  auto window = ceph::rdma::parse_rdma_token(rdma_descr);
  if (!window) {
    lderr(g_ceph_context) << "rgw_cuobj: ERROR: failed to parse RDMA descriptor for address" << dendl;
    return 0;
  }
  ldout(g_ceph_context, 21) << "rgw_cuobj: parsed address from RDMA descriptor: "
                            << window->addr << dendl;
  return window->addr;
}

uint16_t RGWCuObjServer::get_channel_id()
{
  if (!tls_channel_valid) {
    const uint16_t id = m_server->allocateChannelId();
    if (id == invalid_channel) {
      if (!tls_channel_refused) {
        tls_channel_refused = true;
        lderr(m_cct) << "rgw_cuobj: ERROR: cuObject channel allocation "
                     << "failed (" << m_num_dcis << " DC initiators, one for "
                     << "each frontend thread); this thread serves its "
                     << "requests over HTTP" << dendl;
      }
      return invalid_channel;
    }
    tls_channel_id = id;
    tls_channel_valid = true;
    ldout(m_cct, 21) << "rgw_cuobj: allocated channel ID " << id << dendl;
  }
  return tls_channel_id;
}

void RGWCuObjServer::release_channel_id()
{
  if (tls_channel_valid) {
    ldout(m_cct, 21) << "rgw_cuobj: releasing channel ID " << tls_channel_id << dendl;
    m_server->freeChannelId(tls_channel_id);
    tls_channel_valid = false;
  }
}

RGWCuObjServer::RDMABufEntry* RGWCuObjServer::acquire_buffer(size_t needed_size)
{
  ldout(m_cct, 21) << "rgw_cuobj: acquiring RDMA buffer for size " << needed_size << dendl;
  // a buffer only for a thread with a channel to move it: a GET takes
  // its buffer before its headers go out, and without one it is served
  // over HTTP
  if (get_channel_id() == invalid_channel) {
    return nullptr;
  }
  // a buffer of the arena at the transfer's size; a larger transfer than
  // Arena::MAX, or one that finds the arena full, gets none
  const auto slice = m_arena->alloc(needed_size);
  if (!slice) {
    ldout(m_cct, 4) << "rgw_cuobj: no out-of-band buffer of " << needed_size
                    << " bytes free" << dendl;
    return nullptr;
  }
  auto entry = new RDMABufEntry;
  entry->ptr = m_arena->ptr(slice);
  entry->size = slice.len;
  entry->offset = slice.offset;
  entry->handle = m_arena_handle;
  return entry;
}

void RGWCuObjServer::release_buffer(RDMABufEntry* buf)
{
  ldout(m_cct, 21) << "rgw_cuobj: releasing RDMA buffer" << dendl;
  if (buf) {
    m_arena->free(buf->offset, buf->size);
    delete buf;
  }
}

ssize_t RGWCuObjServer::rdma_read_from_client(
    const std::string& key,
    RDMABufEntry* buf,
    uint64_t remote_offset,
    size_t size,
    const std::string& rdma_descr)
{
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    // acquire_buffer() found one, on another thread perhaps
    return -EIO;
  }
  uint64_t client_addr = parse_rdma_descriptor_addr(rdma_descr) + remote_offset;
  size_t total_read = 0;

  ldout(m_cct, 21) << "rgw_cuobj: handlePutObject"
                   << " key=" << key
                   << " size=" << size
                   << " client_addr=" << client_addr
                   << " channel=" << channel
                   << " buf_ptr=" << buf->ptr
                   << " buf_size=" << buf->size
                   << " descr=" << rdma_descr
                   << dendl;

  while (total_read < size) {
    size_t chunk = std::min(size - total_read, MAX_RDMA_OP_SIZE);
    ibv_wc_status wc_status = IBV_WC_SUCCESS;
    ssize_t ret = m_server->handlePutObject(
        key, buf->handle, client_addr + total_read,
        chunk, rdma_descr, channel, buf->offset + total_read, &wc_status);
    if (ret < 0) {
      lderr(m_cct) << "rgw_cuobj: ERROR: handlePutObject failed:"
                   << " ret=" << ret
                   << " wc_status=" << static_cast<int>(wc_status)
                   << " chunk=" << chunk
                   << " local_offset=" << total_read
                   << " remote_addr=" << client_addr + total_read
                   << dendl;
      return ret;
    }
    total_read += ret;
    if (static_cast<size_t>(ret) < chunk) {
      break;
    }
  }

  ldout(m_cct, 20) << "rgw_cuobj: RDMA read " << total_read << " bytes" << " for key=" << key << dendl;
  return static_cast<ssize_t>(total_read);
}

ssize_t RGWCuObjServer::rdma_write_to_client(
    const std::string& key,
    RDMABufEntry* buf,
    uint64_t remote_offset,
    size_t size,
    const std::string& rdma_descr)
{
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    // acquire_buffer() found one, on another thread perhaps
    return -EIO;
  }
  uint64_t client_addr = parse_rdma_descriptor_addr(rdma_descr) + remote_offset;
  size_t total_written = 0;

  ldout(m_cct, 21) << "rgw_cuobj: handleGetObject"
                   << " key=" << key
                   << " size=" << size
                   << " client_addr=" << client_addr
                   << " channel=" << channel
                   << " buf_ptr=" << buf->ptr
                   << " buf_size=" << buf->size
                   << " descr=" << rdma_descr
                   << dendl;
  while (total_written < size) {
    size_t chunk = std::min(size - total_written, MAX_RDMA_OP_SIZE);
    ssize_t ret = m_server->handleGetObject(
        key, buf->handle, client_addr + total_written,
        chunk, rdma_descr, channel, buf->offset + total_written);
    if (ret < 0) {
      lderr(m_cct) << "rgw_cuobj: ERROR: handleGetObject failed:"
                   << " ret=" << ret
                   << " chunk=" << chunk
                   << " local_offset=" << total_written
                   << " remote_addr=" << client_addr + total_written
                   << dendl;
      return ret;
    }
    total_written += ret;
    if (static_cast<size_t>(ret) < chunk) {
      break;
    }
  }

  ldout(m_cct, 20) << "rgw_cuobj: RDMA wrote " << total_written << " bytes" << " for key=" << key << dendl;
  return static_cast<ssize_t>(total_written);
}

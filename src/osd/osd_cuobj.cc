// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd_cuobj.h"

#include <cuobjserver.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <deque>
#include <list>
#include <thread>

#include "blk/BlockDevice.h"
#include "common/buffer_instrumentation.h"
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "common/Clock.h"
#include "common/config.h"
#include "common/crc64nvme.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "common/rdma_token.h"
#include "common/Thread.h"

#define dout_context m_cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_cuobj "

thread_local uint16_t OSDCuObj::tls_channel_id = 0;
thread_local bool OSDCuObj::tls_channel_valid = false;

// per-call limit of the cuObj API
static constexpr size_t MAX_RDMA_OP_SIZE = 1ULL << 30;
// the library caps poll() at 16 events and documents no larger
// per-channel bound, so never have more than that outstanding on one
static constexpr int POLL_BATCH = 16;
// a transfer that has not completed by then is reported failed
static constexpr int PLAN_TIMEOUT = 60;
// a payload in more pieces than this is copied rather than registered
// piecemeal
static constexpr uint64_t IN_PLACE_MAX_SEGMENTS = 64;
// how long shutdown waits for writes already on the wire
static constexpr int SHUTDOWN_GRACE = 5;

// one asynchronous plan on its way through a delivery thread
struct OSDCuObj::inflight_plan {
  struct work_item {
    xfer x;
    inflight_plan* owner;
  };

  plan_request req;
  std::vector<work_item> items;  // fixed once submission starts
  uint64_t window_addr = 0;
  uint64_t total = 0;
  staged_payload staged;
  size_t next = 0;
  size_t outstanding = 0;
  ssize_t err = 0;
  utime_t deadline;
  bool reported = false;  // on_done already called (timed out)
  ceph::rdma::oob_result_t res;
};

/**
 * Owns one channel and drives any number of plans over it: stage,
 * submit up to POLL_BATCH writes, poll, complete. The op workers only
 * queue.
 */
class OSDCuObj::DeliveryThread : public Thread {
public:
  DeliveryThread(OSDCuObj& parent, uint16_t channel)
    : m_parent(parent), m_cct(parent.m_cct), m_channel(channel) {}

  void queue(std::unique_ptr<inflight_plan> p) {
    {
      std::lock_guard l{m_lock};
      m_incoming.push_back(std::move(p));
    }
    m_cond.notify_one();
  }

  void shutdown() {
    {
      std::lock_guard l{m_lock};
      m_stopping = true;
    }
    m_cond.notify_one();
    join();
  }

private:
  void* entry() override;
  int prepare(inflight_plan& p);
  void submit(inflight_plan& p);
  void checksum(inflight_plan& p);
  void finish(inflight_plan& p, ssize_t r);

  OSDCuObj& m_parent;
  CephContext* m_cct;
  const uint16_t m_channel;
  size_t m_outstanding = 0;  // writes in flight on m_channel

  ceph::mutex m_lock = ceph::make_mutex("OSDCuObj::DeliveryThread");
  ceph::condition_variable m_cond;
  std::deque<std::unique_ptr<inflight_plan>> m_incoming;
  bool m_stopping = false;
};

OSDCuObj::OSDCuObj(CephContext *cct, const std::string& rdma_ip,
		   uint16_t rdma_port)
  : m_cct(cct)
{
  if (do_init(rdma_ip, rdma_port) < 0) {
    do_shutdown();
  }
}

OSDCuObj::~OSDCuObj()
{
  do_shutdown();
}

int OSDCuObj::do_init(const std::string& rdma_ip, uint16_t rdma_port)
{
  auto num_dcis = static_cast<int>(
    m_cct->_conf.get_val<uint64_t>("osd_cuobj_num_dcis"));
  auto dc_key = m_cct->_conf.get_val<uint64_t>("osd_cuobj_dc_key");
  auto buf_size = static_cast<size_t>(
    m_cct->_conf.get_val<Option::size_t>("osd_cuobj_buffer_size"));
  auto buf_count = static_cast<size_t>(
    m_cct->_conf.get_val<uint64_t>("osd_cuobj_buffer_count"));

  cuObjRDMATunable params;
  params.setNumDcis(num_dcis);
  params.setDcKey(dc_key);

  dout(1) << "initializing cuObjServer on " << rdma_ip << ":" << rdma_port
	  << " dcis=" << num_dcis
	  << " bufs=" << buf_count << "x" << buf_size << dendl;

  try {
    m_server = std::make_unique<cuObjServer>(
      rdma_ip.c_str(), rdma_port, CUOBJ_PROTO_RDMA_DC_V1, params);
  } catch (const std::exception& e) {
    derr << "ERROR: cuObjServer construction failed: " << e.what() << dendl;
    return -EIO;
  }

  if (!m_server->isConnected()) {
    derr << "ERROR: cuObjServer RDMA session failed to start" << dendl;
    m_server.reset();
    return -ECONNREFUSED;
  }

  m_register_in_place =
    m_cct->_conf.get_val<bool>("osd_cuobj_register_in_place");
  m_buf_size = buf_size;
  m_pool = std::make_unique<BufEntry[]>(buf_count);
  for (size_t i = 0; i < buf_count; i++) {
    auto& entry = m_pool[i];
    entry.ptr = m_server->allocHostBuffer(buf_size);
    if (!entry.ptr) {
      derr << "ERROR: allocHostBuffer failed for buffer " << i << dendl;
      return -ENOMEM;
    }
    entry.size = buf_size;
    entry.handle = m_server->registerBuffer(entry.ptr, buf_size);
    if (!entry.handle) {
      derr << "ERROR: registerBuffer failed for buffer " << i << dendl;
      free(entry.ptr);
      entry.ptr = nullptr;
      return -EIO;
    }
    entry.in_use.store(false, std::memory_order_relaxed);
    m_pool_count = i + 1;
  }

  auto threads = m_cct->_conf.get_val<uint64_t>("osd_cuobj_delivery_threads");
  for (uint64_t i = 0; i < threads; i++) {
    uint16_t channel = m_server->allocateChannelId();
    if (channel == invalid_channel) {
      derr << "ERROR: no cuObject channel for delivery thread " << i
	   << " (raise osd_cuobj_num_dcis?)" << dendl;
      break;
    }
    auto t = std::make_unique<DeliveryThread>(*this, channel);
    t->create("cuobj_deliver");
    m_delivery_threads.push_back(std::move(t));
  }

  dout(1) << "initialized with " << m_pool_count << " RDMA buffers of "
	  << buf_size << " bytes, " << m_delivery_threads.size()
	  << " delivery threads" << dendl;
  return 0;
}

void OSDCuObj::do_shutdown()
{
  // the threads fail whatever is still queued and drain what is in
  // flight, so nothing references the buffers below afterwards
  for (auto& t : m_delivery_threads) {
    t->shutdown();
  }
  m_delivery_threads.clear();
  if (m_server) {
    for (auto& [base, handle] : m_pool_handles) {
      m_server->deRegisterBuffer(handle);
    }
  }
  m_pool_handles.clear();
  for (size_t i = 0; i < m_pool_count; i++) {
    auto& entry = m_pool[i];
    if (entry.handle && m_server) {
      m_server->deRegisterBuffer(entry.handle);
      entry.handle = nullptr;
    }
    if (entry.ptr) {
      free(entry.ptr);
      entry.ptr = nullptr;
    }
  }
  m_pool.reset();
  m_pool_count = 0;
  m_server.reset();
}

bool OSDCuObj::is_available() const
{
  return m_server && m_server->isConnected();
}

uint16_t OSDCuObj::get_channel_id()
{
  if (!tls_channel_valid) {
    uint16_t id = m_server->allocateChannelId();
    if (id == invalid_channel) {
      derr << "ERROR: cuObject channel allocation failed"
	   << " (raise osd_cuobj_num_dcis?)" << dendl;
      return invalid_channel;
    }
    tls_channel_id = id;
    tls_channel_valid = true;
    dout(20) << "allocated channel " << id << " for this thread" << dendl;
  }
  return tls_channel_id;
}

OSDCuObj::BufEntry* OSDCuObj::acquire_buffer(size_t needed, bool* transient)
{
  for (size_t i = 0; i < m_pool_count; i++) {
    auto& entry = m_pool[i];
    if (entry.size >= needed) {
      bool expected = false;
      if (entry.in_use.compare_exchange_strong(expected, true,
					       std::memory_order_acquire)) {
	*transient = false;
	return &entry;
      }
    }
  }
  // pool exhausted (or request larger than any pooled buffer):
  // register a one-shot buffer rather than failing the op, but bound
  // it - registration pins memory and the request size is
  // client-controlled up to osd_max_object_size
  if (needed > m_buf_size * 4) {
    derr << "ERROR: " << needed << " bytes exceeds the transient RDMA "
	 << "registration cap (" << m_buf_size * 4
	 << "); raise osd_cuobj_buffer_size" << dendl;
    return nullptr;
  }
  dout(10) << "buffer pool exhausted, registering transient buffer of "
	   << needed << " bytes" << dendl;
  auto entry = new BufEntry;
  entry->ptr = m_server->allocHostBuffer(needed);
  if (!entry->ptr) {
    delete entry;
    return nullptr;
  }
  entry->size = needed;
  entry->handle = m_server->registerBuffer(entry->ptr, needed);
  if (!entry->handle) {
    free(entry->ptr);
    delete entry;
    return nullptr;
  }
  *transient = true;
  return entry;
}

void OSDCuObj::release_buffer(BufEntry* buf, bool transient)
{
  if (!buf) {
    return;
  }
  if (transient) {
    m_server->deRegisterBuffer(buf->handle);
    free(buf->ptr);
    delete buf;
  } else {
    buf->in_use.store(false, std::memory_order_release);
  }
}

bool OSDCuObj::stage_payload(const ceph::buffer::list& data,
			     staged_payload* st)
{
  const uint64_t nbufs = data.get_num_buffers();
  m_payload_segments += nbufs;
  if (m_register_in_place && nbufs <= IN_PLACE_MAX_SEGMENTS) {
    const auto start = ceph::mono_clock::now();
    using ceph::buffer_instrumentation::instrumented_bptr;
    uint64_t ofs = 0;
    for (const auto& b : data.buffers()) {
      struct rdma_buffer* handle = nullptr;
      uint64_t handle_ofs = 0;
      bool cached = false;
      const auto& ib = static_cast<const instrumented_bptr&>(
	static_cast<const ceph::buffer::ptr&>(b));
      if (ib.is_raw_marked<BlockDevice::hugepaged_raw_marker_t>()) {
	const auto raw = ib.get_raw();
	handle = pool_buffer_handle(raw->get_data(), raw->get_len());
	handle_ofs = b.c_str() - raw->get_data();
	cached = true;
	m_segments_pooled++;
      } else {
	handle = m_server->registerBuffer(
	  const_cast<char*>(b.c_str()), b.length());
      }
      if (!handle) {
	break;
      }
      st->segments.push_back({ofs, b.length(), handle, handle_ofs, cached});
      ofs += b.length();
    }
    m_register_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
      ceph::mono_clock::now() - start).count();
    if (st->segments.size() == nbufs) {
      m_plans_in_place++;
      return true;
    }
    dout(10) << "in-place registration failed at buffer "
	     << st->segments.size() << " of " << nbufs
	     << ", staging a copy" << dendl;
    release_payload(*st);
  }
  st->copy = acquire_buffer(data.length(), &st->transient);
  if (!st->copy) {
    return false;
  }
  auto it = data.begin();
  it.copy(data.length(), static_cast<char*>(st->copy->ptr));
  st->segments.push_back({0, data.length(), st->copy->handle, 0, false});
  m_plans_copied++;
  return true;
}

struct rdma_buffer* OSDCuObj::pool_buffer_handle(char* base, size_t len)
{
  {
    std::shared_lock l{m_pool_handles_lock};
    if (auto it = m_pool_handles.find(base); it != m_pool_handles.end()) {
      return it->second;
    }
  }
  std::unique_lock l{m_pool_handles_lock};
  auto [it, fresh] = m_pool_handles.try_emplace(base, nullptr);
  if (fresh) {
    it->second = m_server->registerBuffer(base, len);
    if (!it->second) {
      m_pool_handles.erase(it);
      return nullptr;
    }
    dout(10) << "registered huge-page read buffer " << (void*)base << "~"
	     << len << " (" << m_pool_handles.size() << " cached)" << dendl;
  }
  return it->second;
}

void OSDCuObj::release_payload(staged_payload& st)
{
  if (st.copy) {
    release_buffer(st.copy, st.transient);
    st.copy = nullptr;
  } else {
    for (auto& seg : st.segments) {
      if (!seg.cached) {
	m_server->deRegisterBuffer(seg.handle);
      }
    }
  }
  st.segments.clear();
}

int OSDCuObj::plan_xfers(const std::string& key,
			 const ceph::osd::oob::placement_plan& plan,
			 uint64_t window_size, uint64_t data_len,
			 const staged_payload& st,
			 std::vector<xfer>* out, uint64_t* total)
{
  for (const auto& t : plan) {
    if (t.len == 0) {
      continue;
    }
    if (t.client_ofs > window_size || t.len > window_size - t.client_ofs ||
	t.local_ofs > data_len || t.len > data_len - t.local_ofs) {
      dout(5) << "placement triple " << t.local_ofs << "/" << t.client_ofs
	      << "~" << t.len << " outside window (" << window_size
	      << ") or data (" << data_len << ") for " << key << dendl;
      return -EINVAL;
    }
    // the segments are ascending, so find the first one the triple
    // touches and walk on from there
    auto seg = std::upper_bound(
      st.segments.begin(), st.segments.end(), t.local_ofs,
      [](uint64_t ofs, const auto& s) { return ofs < s.ofs + s.len; });
    for (uint64_t done = 0; done < t.len; ) {
      ceph_assert(seg != st.segments.end());
      const uint64_t at = t.local_ofs + done;
      const uint64_t n = std::min({t.len - done, seg->ofs + seg->len - at,
				   uint64_t(MAX_RDMA_OP_SIZE)});
      out->push_back({seg->handle, seg->handle_ofs + (at - seg->ofs),
		      t.client_ofs + done, n});
      done += n;
      if (at + n == seg->ofs + seg->len) {
	++seg;
      }
    }
    *total += t.len;
  }
  return 0;
}

ssize_t OSDCuObj::rdma_write(const std::string& key,
			     const ceph::buffer::list& bl,
			     const std::string& token,
			     uint64_t client_offset)
{
  if (!is_available()) {
    return -EOPNOTSUPP;
  }
  auto window = ceph::rdma::parse_rdma_token(token);
  if (!window) {
    dout(5) << "malformed RDMA token for " << key << dendl;
    return -EINVAL;
  }
  const size_t len = bl.length();
  if (len == 0) {
    return 0;
  }
  if (client_offset > window->size || len > window->size - client_offset) {
    dout(5) << "target range " << client_offset << "~" << len
	    << " outside client window of " << window->size
	    << " bytes for " << key << dendl;
    return -EINVAL;
  }
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    return -EIO;
  }
  bool transient = false;
  BufEntry* buf = acquire_buffer(len, &transient);
  if (!buf) {
    derr << "ERROR: no RDMA buffer available for " << len << " bytes" << dendl;
    return -ENOMEM;
  }
  auto it = bl.begin();
  it.copy(len, static_cast<char*>(buf->ptr));

  const uint64_t remote_addr = window->addr + client_offset;
  size_t total = 0;
  ssize_t ret = 0;
  dout(20) << "handleGetObject key=" << key << " len=" << len
	   << " client_offset=" << client_offset
	   << " channel=" << channel << dendl;
  while (total < len) {
    size_t chunk = std::min(len - total, MAX_RDMA_OP_SIZE);
    ibv_wc_status wc_status = IBV_WC_SUCCESS;
    ret = m_server->handleGetObject(key, buf->handle, remote_addr + total,
				    chunk, token, channel, total, &wc_status);
    if (ret < 0) {
      derr << "ERROR: handleGetObject failed for " << key
	   << ": ret=" << ret
	   << " wc_status=" << static_cast<int>(wc_status)
	   << " chunk=" << chunk << " local_offset=" << total << dendl;
      break;
    }
    total += ret;
    if (static_cast<size_t>(ret) < chunk) {
      break;
    }
  }
  release_buffer(buf, transient);
  if (ret < 0) {
    // normalize the library's errno-style returns
    return ret == -EOPNOTSUPP ? -EIO : ret;
  }
  dout(20) << "RDMA wrote " << total << " bytes for " << key << dendl;
  return static_cast<ssize_t>(total);
}

ssize_t OSDCuObj::execute_plan(const std::string& key,
			       const std::string& token,
			       const ceph::buffer::list& data,
			       const ceph::osd::oob::placement_plan& plan)
{
  if (!is_available()) {
    return -EOPNOTSUPP;
  }
  auto window = ceph::rdma::parse_rdma_token(token);
  if (!window) {
    dout(5) << "malformed RDMA token for " << key << dendl;
    return -EINVAL;
  }
  if (std::all_of(plan.begin(), plan.end(),
		  [](const auto& t) { return t.len == 0; })) {
    return 0;
  }
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    return -EIO;
  }
  staged_payload staged;
  if (!stage_payload(data, &staged)) {
    derr << "ERROR: no RDMA buffer available for " << data.length()
	 << " bytes" << dendl;
    return -ENOMEM;
  }
  std::vector<xfer> items;
  uint64_t total = 0;
  if (int r = plan_xfers(key, plan, window->size, data.length(), staged,
			 &items, &total); r < 0) {
    release_payload(staged);
    return r;
  }

  m_plans_started++;
  dout(20) << "executing plan for " << key << ": " << items.size()
	   << " writes, " << total << " bytes, channel " << channel << dendl;

  // batched async submission: at most POLL_BATCH outstanding, polled
  // to completion on the same channel
  const utime_t deadline = ceph_clock_now() + utime_t(PLAN_TIMEOUT, 0);
  size_t next = 0;
  size_t outstanding = 0;
  size_t completed = 0;
  ssize_t err = 0;
  while (completed < items.size()) {
    while (err == 0 && next < items.size() && outstanding < POLL_BATCH) {
      auto& w = items[next];
      ssize_t r = m_server->handleGetObject(
	key, w.handle, window->addr + w.remote_ofs, w.len, token, channel,
	w.local_ofs, nullptr, /*async_handle=*/&items[next]);
      if (r < 0) {
	derr << "ERROR: async handleGetObject submission failed for " << key
	     << ": " << r << dendl;
	err = r;
	break;
      }
      next++;
      outstanding++;
      m_writes_inflight++;
    }
    if (outstanding == 0) {
      break;  // submission failed before anything went out
    }
    cuObjAsyncEvent_t events[POLL_BATCH];
    for (auto& e : events) {
      e.async_handle = nullptr;
    }
    int n = m_server->poll(events, POLL_BATCH, channel);
    if (n == 0) {
      // nothing completed yet; don't hot-spin the op worker
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      if (ceph_clock_now() > deadline) {
	derr << "ERROR: plan for " << key << " timed out with " << outstanding
	     << " writes outstanding; leaking the staging buffer" << dendl;
	m_writes_inflight -= outstanding;
	m_buffers_leaked++;
	m_plans_failed++;
	return -ETIMEDOUT;
      }
      continue;
    }
    if (n < 0) {
      // on -EIO the return is NOT a completion count and the library
      // has reset the QP, flushing the remaining writes; scan for the
      // events that were filled, then abandon the plan
      for (const auto& e : events) {
	if (e.async_handle) {
	  outstanding--;
	  m_writes_inflight--;
	  completed++;
	}
      }
      derr << "ERROR: poll failed for " << key << ": " << n << dendl;
      err = err ? err : -EIO;
      // after a QP reset nothing more will complete; count the rest
      // as flushed
      m_writes_inflight -= outstanding;
      completed += outstanding;
      outstanding = 0;
      break;
    }
    for (int i = 0; i < n; i++) {
      if (!events[i].async_handle) {
	continue;
      }
      outstanding--;
      m_writes_inflight--;
      completed++;
      if (events[i].status != 0 /* IBV_WC_SUCCESS */) {
	derr << "ERROR: RDMA write completion failed for " << key
	     << ": wc_status=" << events[i].status << dendl;
	err = err ? err : -EIO;
      }
    }
    if (ceph_clock_now() > deadline) {
      // wedged transport: we cannot release the staged buffer while
      // writes may still reference it - leak it deliberately
      derr << "ERROR: plan for " << key << " timed out with " << outstanding
	   << " writes outstanding; leaking the staging buffer" << dendl;
      m_writes_inflight -= outstanding;
      m_buffers_leaked++;
      m_plans_failed++;
      return -ETIMEDOUT;
    }
  }
  release_payload(staged);
  if (err < 0) {
    m_plans_failed++;
    return err == -EOPNOTSUPP ? -EIO : err;
  }
  m_plans_completed++;
  m_bytes_pushed += total;
  dout(20) << "plan for " << key << " pushed " << total << " bytes" << dendl;
  return static_cast<ssize_t>(total);
}

void OSDCuObj::execute_plan_async(plan_request&& req)
{
  ceph_assert(has_async_delivery());
  auto p = std::make_unique<inflight_plan>();
  p->req = std::move(req);
  m_plans_queued++;
  const auto n = m_next_delivery_thread++ % m_delivery_threads.size();
  m_delivery_threads[n]->queue(std::move(p));
}

// validate and stage; nothing has touched the fabric if this fails
int OSDCuObj::DeliveryThread::prepare(inflight_plan& p)
{
  const auto& req = p.req;
  if (!m_parent.is_available()) {
    return -EOPNOTSUPP;
  }
  auto window = ceph::rdma::parse_rdma_token(req.token);
  if (!window) {
    dout(5) << "malformed RDMA token for " << req.key << dendl;
    return -EINVAL;
  }
  p.window_addr = window->addr;
  if (std::all_of(req.plan.begin(), req.plan.end(),
		  [](const auto& t) { return t.len == 0; })) {
    return 0;
  }
  const utime_t now = ceph_clock_now();
  if (now > req.initiate_by) {
    // queued past the delivery or read lease: a push now could land in
    // a window the client has already reused
    dout(10) << "plan for " << req.key << " missed its initiation bound"
	     << dendl;
    return -ETIMEDOUT;
  }
  if (!m_parent.stage_payload(req.data, &p.staged)) {
    derr << "ERROR: no RDMA buffer available for " << req.data.length()
	 << " bytes" << dendl;
    return -ENOMEM;
  }
  std::vector<xfer> xfers;
  if (int r = m_parent.plan_xfers(req.key, req.plan, window->size,
				  req.data.length(), p.staged, &xfers,
				  &p.total); r < 0) {
    m_parent.release_payload(p.staged);
    return r;
  }
  p.items.reserve(xfers.size());
  for (const auto& x : xfers) {
    p.items.push_back({x, &p});
  }
  p.deadline = now;
  p.deadline += utime_t(PLAN_TIMEOUT, 0);
  m_parent.m_plans_started++;
  return 0;
}

void OSDCuObj::DeliveryThread::submit(inflight_plan& p)
{
  while (p.err == 0 && p.next < p.items.size() &&
	 m_outstanding < static_cast<size_t>(POLL_BATCH)) {
    auto& w = p.items[p.next];
    ssize_t r = m_parent.m_server->handleGetObject(
      p.req.key, w.x.handle, p.window_addr + w.x.remote_ofs, w.x.len,
      p.req.token, m_channel, w.x.local_ofs, nullptr, /*async_handle=*/&w);
    if (r < 0) {
      derr << "ERROR: async handleGetObject submission failed for "
	   << p.req.key << ": " << r << dendl;
      p.err = r;
      break;
    }
    p.next++;
    p.outstanding++;
    m_outstanding++;
    m_parent.m_writes_inflight++;
  }
}

// checksum each placed range at the storage node. Every triple is one
// contiguous logical
// extent, so a caller holding all of a window's ranges can fold them in
// offset order however they were interleaved across shards; the
// whole-payload value comes from the same pass, since the builders emit
// triples contiguous and ascending in payload order.
void OSDCuObj::DeliveryThread::checksum(inflight_plan& p)
{
  auto& res = p.res;
  res.ranges.reserve(p.req.plan.size());
  uint64_t whole = 0;
  size_t i = 0;
  for (const auto& t : p.req.plan) {
    const auto known = i < p.req.known_crc64.size() ? p.req.known_crc64[i]
						      : std::nullopt;
    i++;
    m_parent.note_crc_source(known.has_value());
    uint64_t crc;
    if (known) {
      crc = *known;
    } else {
      ceph::buffer::list part;
      part.substr_of(p.req.data, t.local_ofs, t.len);
      crc = ceph::crc64nvme(part);
    }
    res.ranges.push_back({t.client_ofs, t.len, crc});
    whole = res.ranges.size() == 1 ? crc
	  : ceph::crc64nvme_combine(whole, crc, t.len);
  }
  res.crc64 = whole;
  res.flags |= ceph::rdma::oob_result_t::FLAG_CRC64NVME |
	       ceph::rdma::oob_result_t::FLAG_CRC64_RANGES;
  if (p.req.plan.size() == 1) {
    // one contiguous logical extent: crc64 itself folds with adjacent
    // stripes, so a caller needs no ranges
    res.flags |= ceph::rdma::oob_result_t::FLAG_CRC64_COMBINABLE;
  }
}

void OSDCuObj::DeliveryThread::finish(inflight_plan& p, ssize_t r)
{
  m_parent.m_plans_queued--;
  if (r < 0) {
    m_parent.m_plans_failed++;
    // normalize the library's errno-style returns
    r = r == -EOPNOTSUPP ? -EIO : r;
  } else {
    m_parent.m_plans_completed++;
    m_parent.m_bytes_pushed += p.total;
    p.res.bytes = p.total;
    r = static_cast<ssize_t>(p.total);
    dout(20) << "plan for " << p.req.key << " pushed " << p.total
	     << " bytes" << dendl;
  }
  auto on_done = std::move(p.req.on_done);
  on_done(r, std::move(p.res));
}

void* OSDCuObj::DeliveryThread::entry()
{
  std::deque<std::unique_ptr<inflight_plan>> waiting;
  std::list<std::unique_ptr<inflight_plan>> active;
  bool stopping = false;
  bool grace_started = false;

  while (true) {
    {
      std::unique_lock l{m_lock};
      if (m_incoming.empty() && waiting.empty() && active.empty()) {
	if (m_stopping) {
	  break;
	}
	m_cond.wait(l);
	continue;
      }
      while (!m_incoming.empty()) {
	waiting.push_back(std::move(m_incoming.front()));
	m_incoming.pop_front();
      }
      stopping = m_stopping;
    }

    // start new plans while the channel has room
    while (!waiting.empty() &&
	   (stopping || m_outstanding < static_cast<size_t>(POLL_BATCH))) {
      auto p = std::move(waiting.front());
      waiting.pop_front();
      if (stopping) {
	finish(*p, -ESHUTDOWN);
	continue;
      }
      if (int r = prepare(*p); r < 0 || p->items.empty()) {
	finish(*p, r);
	continue;
      }
      dout(20) << "executing plan for " << p->req.key << ": "
	       << p->items.size() << " writes, " << p->total
	       << " bytes, channel " << m_channel << dendl;
      submit(*p);
      if (p->req.want_crc64 && p->err == 0) {
	checksum(*p);  // overlaps the transfer
      }
      active.push_back(std::move(p));
    }
    // plans with more writes than fit at once
    for (auto& p : active) {
      submit(*p);
    }

    int n = 0;
    if (m_outstanding > 0) {
      cuObjAsyncEvent_t events[POLL_BATCH];
      for (auto& e : events) {
	e.async_handle = nullptr;
      }
      n = m_parent.m_server->poll(events, POLL_BATCH, m_channel);
      if (n < 0) {
	// on -EIO the return is NOT a completion count and the library
	// has reset the QP, flushing every write outstanding on this
	// channel, whichever plan it belongs to
	derr << "ERROR: poll failed on channel " << m_channel << ": " << n
	     << ", failing " << m_outstanding << " outstanding writes" << dendl;
	for (auto& p : active) {
	  if (p->outstanding) {
	    m_parent.m_writes_inflight -= p->outstanding;
	    p->outstanding = 0;
	    p->err = p->err ? p->err : -EIO;
	  }
	}
	m_outstanding = 0;
      }
      for (int i = 0; i < n; i++) {
	auto w = static_cast<inflight_plan::work_item*>(events[i].async_handle);
	if (!w) {
	  continue;
	}
	auto& p = *w->owner;
	p.outstanding--;
	m_outstanding--;
	m_parent.m_writes_inflight--;
	if (events[i].status != 0 /* IBV_WC_SUCCESS */) {
	  derr << "ERROR: RDMA write completion failed for " << p.req.key
	       << ": wc_status=" << events[i].status << dendl;
	  p.err = p.err ? p.err : -EIO;
	}
      }
    }

    const utime_t now = ceph_clock_now();
    if (stopping && !grace_started) {
      // give what is already on the wire a moment to land
      grace_started = true;
      for (auto& p : active) {
	p->deadline = std::min(p->deadline, now + utime_t(SHUTDOWN_GRACE, 0));
      }
    }
    for (auto it = active.begin(); it != active.end(); ) {
      auto& p = **it;
      if (p.outstanding == 0 && (p.err || p.next == p.items.size())) {
	m_parent.release_payload(p.staged);
	if (!p.reported) {
	  finish(p, p.err);
	}
	it = active.erase(it);
	continue;
      }
      if (!p.reported && now > p.deadline) {
	// wedged transport: report failure now, but the staged buffer
	// stays claimed until the writes that may still reference it
	// drain
	derr << "ERROR: plan for " << p.req.key << " timed out with "
	     << p.outstanding << " writes outstanding" << dendl;
	m_parent.m_buffers_leaked++;
	p.reported = true;
	p.err = p.err ? p.err : -ETIMEDOUT;
	finish(p, -ETIMEDOUT);
      }
      ++it;
    }
    if (stopping && std::all_of(active.begin(), active.end(),
				[](auto& p) { return p->reported; })) {
      // whatever is left never drained; its buffers die with the pool
      active.clear();
      continue;
    }
    if (m_outstanding > 0 && n == 0) {
      // nothing completed yet; don't hot-spin
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }
  }
  return nullptr;
}

void OSDCuObj::dump_stats(ceph::Formatter* f) const
{
  f->dump_bool("available", is_available());
  f->dump_unsigned("plans_started", m_plans_started.load());
  f->dump_unsigned("plans_completed", m_plans_completed.load());
  f->dump_unsigned("plans_failed", m_plans_failed.load());
  f->dump_unsigned("bytes_pushed", m_bytes_pushed.load());
  f->dump_unsigned("writes_inflight", m_writes_inflight.load());
  f->dump_unsigned("buffers_leaked", m_buffers_leaked.load());
  f->dump_unsigned("payload_segments", m_payload_segments.load());
  f->dump_unsigned("crc64_from_metadata", m_crc_from_metadata.load());
  f->dump_unsigned("crc64_computed", m_crc_computed.load());
  f->dump_unsigned("plans_in_place", m_plans_in_place.load());
  f->dump_unsigned("segments_from_huge_pool", m_segments_pooled.load());
  f->dump_unsigned("plans_copied", m_plans_copied.load());
  f->dump_unsigned("register_in_place_ns", m_register_ns.load());
  f->dump_unsigned("delivery_threads", m_delivery_threads.size());
  f->dump_unsigned("plans_queued", m_plans_queued.load());
}

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd_cuobj.h"
#include "osd/osd_dc_target.h"

#include <cuobjserver.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <thread>

#include "common/ceph_context.h"
#include "common/Clock.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "common/rdma_token.h"

#define dout_context m_cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_cuobj "

namespace {
/// how long a posted cuObject write can keep retrying after the OSD
/// stops waiting for it: the RC/DC retry budget at the library's
/// defaults
constexpr std::chrono::milliseconds CUOBJ_RETRY_BUDGET{2000};

/// writes posted at once, and events taken per poll() (the library
/// caps poll() at 16 events and documents no larger per-channel bound)
constexpr int POLL_BATCH = 16;

/// A write's async handle is a number unique in the process, not a
/// pointer into its plan: a write that a plan left posted when it timed
/// out completes during a later plan on the channel, and its handle
/// must still name the plan that posted it, not point at memory that
/// may since be the later plan's own.
static_assert(sizeof(uintptr_t) >= sizeof(uint64_t));

void* as_handle(uint64_t h)
{
  return reinterpret_cast<void*>(static_cast<uintptr_t>(h));
}

uint64_t handle_value(const void* handle)
{
  return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(handle));
}
}

thread_local uint16_t OSDCuObj::tls_channel_id = 0;
thread_local bool OSDCuObj::tls_channel_valid = false;
thread_local ceph::osd::oob::abandoned_plans<OSDCuObj::held_t>
  OSDCuObj::tls_abandoned;

// per-call limit of the cuObj API
static constexpr size_t MAX_RDMA_OP_SIZE = 1ULL << 30;

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
    m_cct->_conf.get_val<Option::size_t>("osd_oob_buffer_size"));
  auto buf_count = static_cast<size_t>(
    m_cct->_conf.get_val<uint64_t>("osd_oob_buffer_count"));

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

  dout(1) << "initialized with " << m_pool_count << " RDMA buffers of "
	  << buf_size << " bytes" << dendl;

  if (m_cct->_conf.get_val<bool>("osd_oob_gather")) {
    // a DC target so peers can push their shard reads into this OSD
    // when it gathers an erasure-coded read
    m_win_size = m_cct->_conf.get_val<Option::size_t>("osd_oob_window_size");
    const auto count = m_cct->_conf.get_val<uint64_t>("osd_oob_window_count");
    auto dct = std::make_unique<OSDDcTarget>();
    if (m_win_size && count &&
	dct->open(m_cct, rdma_ip, m_win_size * count, dc_key) == 0) {
      m_dct = std::move(dct);
      m_win_slots.resize(count);
    } else {
      derr << "WARNING: no DC target for gather windows; peers will reply "
	   << "inline" << dendl;
    }
  }
  return 0;
}

std::optional<OSDOobExecutor::window_t> OSDCuObj::acquire_window(size_t size)
{
  if (!m_dct || size > m_win_size) {
    return std::nullopt;
  }
  const auto now = std::chrono::steady_clock::now();
  std::lock_guard l(m_win_mtx);
  for (size_t i = 0; i < m_win_slots.size(); i++) {
    auto& slot = m_win_slots[i];
    if (slot.in_use || now < slot.quarantined_until) {
      continue;
    }
    slot.in_use = true;
    window_t w;
    w.id = i;
    w.ptr = m_dct->pool() + i * m_win_size;
    w.size = m_win_size;
    w.token = m_dct->token(i * m_win_size, m_win_size);
    return w;
  }
  return std::nullopt;
}

void OSDCuObj::release_window(uint64_t id, uint64_t quarantine_ms)
{
  std::lock_guard l(m_win_mtx);
  if (id >= m_win_slots.size()) {
    return;
  }
  m_win_slots[id].in_use = false;
  if (quarantine_ms) {
    m_win_slots[id].quarantined_until = std::chrono::steady_clock::now() +
      std::chrono::milliseconds(quarantine_ms);
  }
}

void OSDCuObj::do_shutdown()
{
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

bool OSDCuObj::handles(const std::string& token) const
{
  return !ceph::rdma::is_ofi_token(token);
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
	 << "); raise osd_oob_buffer_size" << dendl;
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

void OSDCuObj::credit_abandoned(uint64_t handle)
{
  using credit_t = ceph::osd::oob::abandoned_plans<held_t>::credit_t;
  m_stale_completions++;
  held_t held;
  switch (tls_abandoned.credit(handle, &held)) {
  case credit_t::unknown:
    // its plan's buffer went back when the channel was reset, or the
    // library returned a write that this channel did not post
    dout(5) << "completion of write " << handle << " that no plan on "
	    << "this channel waits for" << dendl;
    break;
  case credit_t::counted:
    break;
  case credit_t::last:
    // the plan's last write is done with its buffer
    dout(10) << "timed-out plan of write " << handle << " completed its "
	     << "last write; staging buffer reclaimed" << dendl;
    release_buffer(held.buf, held.transient);
    m_buffers_reclaimed++;
    break;
  }
}

void OSDCuObj::drop_abandoned()
{
  for (const auto& held : tls_abandoned.drop()) {
    release_buffer(held.buf, held.transient);
    m_buffers_reclaimed++;
  }
}

void OSDCuObj::reap_abandoned(uint16_t channel)
{
  if (tls_abandoned.empty()) {
    return;
  }
  cuObjAsyncEvent_t events[POLL_BATCH];
  for (auto& e : events) {
    e.async_handle = nullptr;
  }
  int n = m_server->poll(events, POLL_BATCH, channel);
  // a failed poll returns no completion count: scan for the events
  // that were filled, as execute_plan does
  for (const auto& e : events) {
    if (e.async_handle) {
      credit_abandoned(handle_value(e.async_handle));
    }
  }
  if (n == -EIO) {
    // a write that a timed-out plan left posted failed, and the library
    // reset the QP: nothing more of those plans completes
    dout(5) << "channel " << channel << " was reset with writes of "
	    << tls_abandoned.size() << " timed-out plans posted; "
	    << "reclaiming their staging buffers" << dendl;
    drop_abandoned();
  } else if (n < 0) {
    dout(5) << "poll of channel " << channel << " failed: " << n << dendl;
  }
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
  // the library does not document whether a synchronous call tells its
  // own completion from one of a write that a timed-out plan left
  // posted on the channel: do not post behind such a write
  reap_abandoned(channel);
  if (!tls_abandoned.empty()) {
    dout(10) << "channel " << channel << " still has writes of timed-out "
	     << "plans posted; not writing " << key << dendl;
    return -EBUSY;
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
			       const ceph::osd::oob::placement_plan& plan,
			       std::chrono::milliseconds budget,
			       bool* started)
{
  if (started) {
    *started = false;
  }
  if (!is_available()) {
    return -EOPNOTSUPP;
  }
  auto window = ceph::rdma::parse_rdma_token(token);
  if (!window) {
    dout(5) << "malformed RDMA token for " << key << dendl;
    return -EINVAL;
  }
  // expand triples into <=1 GiB work items, validating up front
  struct work_item {
    uint64_t local_ofs;   // offset into the staged buffer
    uint64_t remote_ofs;  // offset into the client window
    uint64_t len;
  };
  std::vector<work_item> items;
  uint64_t total = 0;
  for (const auto& t : plan) {
    if (t.len == 0) {
      continue;
    }
    if (t.client_ofs > window->size || t.len > window->size - t.client_ofs ||
	t.local_ofs > data.length() ||
	t.len > data.length() - t.local_ofs) {
      dout(5) << "placement triple " << t.local_ofs << "/" << t.client_ofs
	      << "~" << t.len << " outside window (" << window->size
	      << ") or data (" << data.length() << ") for " << key << dendl;
      return -EINVAL;
    }
    for (uint64_t done = 0; done < t.len; ) {
      const uint64_t chunk = std::min(t.len - done, MAX_RDMA_OP_SIZE);
      items.push_back({t.local_ofs + done, t.client_ofs + done, chunk});
      done += chunk;
    }
    total += t.len;
  }
  if (items.empty()) {
    return 0;
  }
  // cuObject cannot cancel a write it posted: one still in flight when
  // the OSD stops waiting keeps retrying for the DC transport's retry
  // budget. Stop waiting that much before the caller's budget runs out,
  // and do not start at all when the budget does not cover it - decided
  // before anything is staged, so that a plan refused here holds no
  // buffer and is not counted as started.
  if (budget <= CUOBJ_RETRY_BUDGET) {
    dout(10) << "budget of " << budget.count() << " ms for " << key
	     << " does not cover the DC retry budget; nothing staged" << dendl;
    m_budget_refused++;
    return -ETIMEDOUT;
  }
  // counted from here, not from after staging: taking a channel, copying
  // the data and registering a one-time buffer come out of the budget too
  utime_t deadline = ceph_clock_now();
  deadline += std::chrono::duration<double>(budget - CUOBJ_RETRY_BUDGET).count();
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    return -EIO;
  }
  // A write that an earlier plan on this channel left posted when it
  // timed out may still be retrying. One posted behind it waits for it
  // on the DC initiator, and can still be retrying when this plan's
  // budget runs out; this plan would hold the op worker until its
  // deadline meanwhile. Take what completed of those writes, which also
  // frees their buffers for this plan, and deliver this read inline
  // while any is left.
  reap_abandoned(channel);
  if (!tls_abandoned.empty()) {
    dout(10) << "channel " << channel << " still has writes of "
	     << tls_abandoned.size() << " timed-out plans posted; nothing "
	     << "staged for " << key << dendl;
    m_channel_busy++;
    return -EBUSY;
  }
  bool transient = false;
  BufEntry* buf = acquire_buffer(data.length(), &transient);
  if (!buf) {
    derr << "ERROR: no RDMA buffer available for " << data.length()
	 << " bytes" << dendl;
    return -ENOMEM;
  }
  {
    auto it = data.begin();
    it.copy(data.length(), static_cast<char*>(buf->ptr));
  }

  m_plans_started++;
  // no earlier plan's write is left on the channel, but a completion
  // counts toward this plan only when it carries one of the handles
  // that this plan reserves here, write i carrying first + i
  const uint64_t first =
    m_next_handle.fetch_add(items.size(), std::memory_order_relaxed);
  auto mine = [&](const void* handle) {
    return ceph::osd::oob::owns_handle(first, items.size(),
				       handle_value(handle));
  };
  dout(20) << "executing plan " << first << " for " << key << ": "
	   << items.size() << " writes, " << total << " bytes, channel "
	   << channel << dendl;

  // batched async submission: at most POLL_BATCH outstanding, polled
  // to completion on the same channel
  size_t next = 0;
  size_t outstanding = 0;
  size_t completed = 0;
  ssize_t err = 0;
  while (completed < items.size()) {
    while (err == 0 && next < items.size() && outstanding < POLL_BATCH) {
      // a write posted past the deadline could retry past the budget
      if (ceph_clock_now() > deadline) {
	dout(5) << "plan for " << key << " reached its deadline with "
		<< items.size() - next << " writes not posted" << dendl;
	err = -ETIMEDOUT;
	break;
      }
      auto& w = items[next];
      // a submission that fails may still have sent part of its write
      if (started) {
	*started = true;
      }
      ssize_t r = m_server->handleGetObject(
	key, buf->handle, window->addr + w.remote_ofs, w.len, token, channel,
	w.local_ofs, nullptr, /*async_handle=*/as_handle(first + next));
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
      break;  // nothing in flight, and an error or the deadline stops posts
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
	derr << "ERROR: plan " << first << " for " << key << " timed out with "
	     << outstanding << " writes outstanding; keeping the staging "
	     << "buffer until they complete" << dendl;
	m_writes_inflight -= outstanding;
	m_buffers_leaked++;
	m_plans_failed++;
	tls_abandoned.abandon(first, items.size(), outstanding,
			      {buf, transient});
	return -ETIMEDOUT;
      }
      continue;
    }
    if (n < 0) {
      // a failed poll returns no completion count: scan for the events
      // that were filled, then fail the plan
      for (const auto& e : events) {
	if (!e.async_handle) {
	  continue;
	}
	if (!mine(e.async_handle)) {
	  credit_abandoned(handle_value(e.async_handle));
	  continue;
	}
	outstanding--;
	m_writes_inflight--;
	completed++;
      }
      derr << "ERROR: poll failed for " << key << ": " << n << dendl;
      err = err ? err : -EIO;
      if (n != -EIO && outstanding > 0) {
	// only -EIO says that the library reset the QP: the writes still
	// outstanding may go on reading the staging buffer, so keep it
	// until a later read on this thread finds them completed
	derr << "ERROR: plan " << first << " for " << key << " failed with "
	     << outstanding << " writes outstanding; keeping the staging "
	     << "buffer until they complete" << dendl;
	m_writes_inflight -= outstanding;
	m_buffers_leaked++;
	m_plans_failed++;
	tls_abandoned.abandon(first, items.size(), outstanding,
			      {buf, transient});
	return err == -EOPNOTSUPP ? -EIO : err;
      }
      // on -EIO the library has reset the QP, flushing the remaining
      // writes: nothing more of them will complete, count them as
      // flushed
      m_writes_inflight -= outstanding;
      completed += outstanding;
      outstanding = 0;
      break;
    }
    for (int i = 0; i < n; i++) {
      if (!events[i].async_handle) {
	continue;
      }
      if (!mine(events[i].async_handle)) {
	// not this plan's write, nor one of a timed-out plan that is
	// still posted: none of those is left when a plan starts
	dout(5) << "plan " << first << " for " << key << " took the "
		<< "completion of write "
		<< handle_value(events[i].async_handle)
		<< ", which it did not post: wc_status=" << events[i].status
		<< dendl;
	credit_abandoned(handle_value(events[i].async_handle));
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
    if (outstanding > 0 && ceph_clock_now() > deadline) {
      // wedged transport: we cannot release the staged buffer while
      // writes may still reference it - keep it until a later read on
      // this thread finds them completed. With none outstanding
      // nothing reads it: the loop completes the plan or, posting
      // nothing more, fails it, and gives the buffer back.
      derr << "ERROR: plan " << first << " for " << key << " timed out with "
	   << outstanding << " writes outstanding; keeping the staging "
	   << "buffer until they complete" << dendl;
      m_writes_inflight -= outstanding;
      m_buffers_leaked++;
      m_plans_failed++;
      tls_abandoned.abandon(first, items.size(), outstanding,
			    {buf, transient});
      return -ETIMEDOUT;
    }
  }
  release_buffer(buf, transient);
  if (err < 0) {
    m_plans_failed++;
    return err == -EOPNOTSUPP ? -EIO : err;
  }
  m_plans_completed++;
  m_bytes_pushed += total;
  dout(20) << "plan for " << key << " pushed " << total << " bytes" << dendl;
  return static_cast<ssize_t>(total);
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
  f->dump_unsigned("buffers_reclaimed", m_buffers_reclaimed.load());
  f->dump_unsigned("stale_completions", m_stale_completions.load());
  f->dump_unsigned("channel_busy", m_channel_busy.load());
  f->dump_unsigned("budget_refused", m_budget_refused.load());
  f->dump_unsigned("gather_crc_mismatch", get_gather_crc_mismatch());
}

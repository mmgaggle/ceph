/*
 * A userspace client (src/client/Client.cc) holding one cap on one file
 * from one MDS.
 *
 * Modelled: Client::handle_cap_grant, check_caps, send_cap, add_update_cap,
 * handle_cap_flush_ack, mark_caps_flushing, remove_cap (trim), get_caps /
 * put_cap_ref around read and write, the delayed cap release list, the
 * object cacher's dirty set (one buffered write at a time) and its flush
 * callback (_flushed), the wanted_max_size / max_size handshake, getattr
 * (_getattr), setattr of mtime (_do_setattr: local with Fx or Fw, else an
 * MDS request with the request-embedded cap release of
 * encode_inode_release), and the session cap generation: Inode::cap_is_valid
 * with MetaSession::cap_gen and cap_ttl, CEPH_SESSION_STALE, renew_caps,
 * wake_up_session_caps and the I_CAP_DROPPED recovery in get_caps.
 *
 * The file content is one integer version (see Store.p). The read cache is
 * one (valid, version) pair; the dirty buffer is one version waiting to be
 * flushed.
 */

/*
 * recheck_after_renew enables a proposed fix: when a RENEWCAPS ack restores
 * cap_ttl, re-run check_caps on an inode that still has caps under
 * revocation. Without it a revocation handled while cap_ttl was expired is
 * never acknowledged (see README, finding 1).
 */
type tClientConfig = (id: int, mds: machine, store: machine, recheck_after_renew: bool,
                      invalidate_on_fc_grant: bool);
/*
 * invalidate_on_fc_grant enables a second proposed fix, the kernel client's
 * behaviour: when Fc is granted and was not issued before, drop the clean
 * cache, because another client may have written in between (README,
 * finding 2).
 */

enum tOpKind { OP_NONE = 0, OP_OPEN = 1, OP_READ = 2, OP_WRITE_SYNC = 3, OP_WRITE_BUF = 4,
               OP_GETATTR = 5, OP_SETATTR = 6 }

type tFlushTid = (tid: int, caps: tCaps);

/* application -> client */
event eAppOpen  : tMode;
event eAppClose : tMode;
event eAppRead;
event eAppWrite;
event eAppStat;         // stat(): needs Fs or a getattr request
event eAppSetattr;      // utimes(): needs Fx, Fw, or a setattr request
event eAppTrim;         // the inode fell off the LRU: release the cap if it is idle
event eAppFlushTick;    // the object cacher's flusher
event eAppRenewTick;    // Client::tick() -> renew_caps()
event eAppTtlExpire;    // the session's cap_ttl passed without a renew ack
event eDelayedCheck;    // Client::tick() processing the delayed cap list

/* a read or write was dropped with EBADF (file not open) */
event eIoDropped : int;

machine Client {
  var id: int;
  var mds: machine;
  var store: machine;

  // MetaSession
  var sessGen: int;          // MetaSession::cap_gen
  var ttlValid: bool;        // ceph_clock_now() < cap_ttl
  var renewSeq: int;         // MetaSession::cap_renew_seq
  var evicted: bool;         // the session was killed and we are blocklisted
  var recheckAfterRenew: bool;
  var invalidateOnFcGrant: bool;
  var superVer: int;         // unflushed version the current buffered write overwrites

  // the Cap
  var hasCap: bool;
  var capId: int;
  var capGen: int;           // Cap::gen
  var capIssued: tCaps;
  var capImplemented: tCaps;
  var capSeq: int;
  var capIssueSeq: int;
  var capWanted: tCaps;

  // Inode
  var openRd: int;
  var openWr: int;
  var cacheValid: bool;
  var cacheVer: int;
  var dirty: bool;           // oset.dirty_or_tx
  var dirtyVer: int;
  var flushInflight: bool;   // a flush was requested and has not completed
  var flushVer: int;         // version sent to the store for this flush, 0 if not sent yet
  var ticketPending: bool;   // a buffered write is in the cache but has no version yet
  var dirtyCaps: tCaps;
  var flushingCaps: tCaps;
  var flushTids: seq[tFlushTid];
  var lastFlushTid: int;
  var maxSize: int;
  var wantedMaxSize: int;
  var requestedMaxSize: int;
  var delayQueued: bool;
  var capDropped: bool;      // I_CAP_DROPPED

  // the application
  var op: tOpKind;
  var opRefs: tCaps;         // cap refs held by the in-flight operation
  var opCached: bool;        // the in-flight read goes through the cache
  var pendingOpens: seq[tMode];
  var pendingReads: int;
  var pendingWrites: int;
  var pendingStats: int;
  var pendingSetattrs: int;

  start state Running {
    entry (cfg: tClientConfig) {
      id = cfg.id;
      mds = cfg.mds;
      store = cfg.store;
      recheckAfterRenew = cfg.recheck_after_renew;
      invalidateOnFcGrant = cfg.invalidate_on_fc_grant;
      op = OP_NONE;
      ttlValid = true;
    }
    on eOpenReply    do (r: tCapReply) { HandleOpenReply(r); }
    on eGetattrReply do (r: tCapReply) { HandleGetattrReply(r); }
    on eSetattrReply do (r: tCapReply) { HandleSetattrReply(r); }
    on eCapGrant     do (m: tCapGrant) { HandleCapGrant(m); }
    on eFlushAck     do (a: tFlushAck) { HandleFlushAck(a); }
    on eSessionStale do { if (!evicted) { HandleSessionStale(); } }
    on eRenewCapsAck do (rs: int) { if (!evicted) { HandleRenewAck(rs); } }
    on eSessionKilled do { HandleSessionKilled(); }
    on eStoreWriteRejected do (ver: int) { HandleStoreWriteRejected(ver); }
    on eStoreReadRejected do { HandleStoreReadRejected(); }

    on eStoreReadResp   do (ver: int) { CompleteRead(ver); }
    on eStoreTicketResp do (ver: int) { CompleteBufferedWrite(ver); }
    on eStoreWriteAck   do (ver: int) { HandleStoreWriteAck(ver); }

    on eAppOpen    do (mode: tMode) { if (!evicted) { AppOpen(mode); } }
    on eAppClose   do (mode: tMode) { if (!evicted) { AppClose(mode); } }
    on eAppRead    do { pendingReads = pendingReads + 1; TryPendingOps(); }
    on eAppWrite   do { pendingWrites = pendingWrites + 1; TryPendingOps(); }
    on eAppStat    do { pendingStats = pendingStats + 1; TryPendingOps(); }
    on eAppSetattr do { pendingSetattrs = pendingSetattrs + 1; TryPendingOps(); }
    on eAppTrim    do { if (!evicted) { AppTrim(); } }
    on eAppFlushTick do { if (dirty && !evicted) { StartFlush(); } }
    on eAppRenewTick do { if (!evicted) { SendRenew(); } }
    on eAppTtlExpire do { if (!evicted) { TtlExpire(); } }
    on eDelayedCheck do { delayQueued = false; if (!evicted) { CheckCaps(true); } }
  }

  /* ---------------- session ---------------- */

  /* Inode::cap_is_valid(cap) */
  fun CapIsValid() : bool {
    return hasCap && sessGen <= capGen && ttlValid;
  }

  /* Client::renew_caps(session) */
  fun SendRenew() {
    renewSeq = renewSeq + 1;
    send mds, eRenewCaps, (client = id, renew_seq = renewSeq);
  }

  /*
   * cap_ttl passed. The MDS session timeout is modelled as a message the
   * client sends after this point, so the MDS never declares the session
   * stale while the client still trusts its caps.
   */
  fun TtlExpire() {
    ttlValid = false;
    AnnounceImplemented();
    if ($) {
      send mds, eSessionTimeout, id;
    }
    // Client::tick() keeps renewing every session_timeout / 3
    SendRenew();
  }

  /*
   * Client::_closed_mds_session(s, -EBLOCKLISTED) -> remove_session_caps:
   * the cap is gone, dirty metadata and dirty data are lost, every blocked
   * operation fails. The client does not reconnect in this model.
   */
  fun HandleSessionKilled() {
    evicted = true;
    hasCap = false;
    ttlValid = false;
    // objectcacher->purge_set(): the store already reported unlanded data as lost
    dirty = false;
    flushInflight = false;
    cacheValid = false;
    dirtyCaps = CapsNone();
    flushingCaps = CapsNone();
    flushTids = default(seq[tFlushTid]);
    wantedMaxSize = 0;
    requestedMaxSize = 0;
    AnnounceImplemented();
    DropPendingOps();
  }

  fun DropPendingOps() {
    while (pendingReads > 0) { pendingReads = pendingReads - 1; announce eIoDropped, id; }
    while (pendingWrites > 0) { pendingWrites = pendingWrites - 1; announce eIoDropped, id; }
    while (pendingStats > 0) { pendingStats = pendingStats - 1; announce eIoDropped, id; }
    while (pendingSetattrs > 0) { pendingSetattrs = pendingSetattrs - 1; announce eIoDropped, id; }
    pendingOpens = default(seq[tMode]);
    if (op == OP_OPEN || op == OP_GETATTR || op == OP_SETATTR) {
      // kick_requests_closed: the request fails
      if (op != OP_OPEN) { announce eIoDropped, id; }
      op = OP_NONE;
    }
  }

  fun HandleStoreWriteRejected(ver: int) {
    if (op == OP_WRITE_SYNC) {
      announce eIoDropped, id;
      op = OP_NONE;
      opRefs = CapsNone();
      return;
    }
    // a flush of buffered data was rejected: the data is gone
    flushInflight = false;
    dirty = false;
  }

  fun HandleStoreReadRejected() {
    assert op == OP_READ, "read rejection without a read in flight";
    announce eIoDropped, id;
    op = OP_NONE;
    opRefs = CapsNone();
  }

  /* Client::handle_client_session(CEPH_SESSION_STALE) */
  fun HandleSessionStale() {
    sessGen = sessGen + 1;   // invalidate session caps/leases
    ttlValid = false;
    AnnounceImplemented();
    SendRenew();
  }

  /* Client::handle_client_session(CEPH_SESSION_RENEWCAPS) */
  fun HandleRenewAck(rs: int) {
    var wasStale: bool;
    if (rs != renewSeq) { return; }
    wasStale = !ttlValid;
    ttlValid = true;
    if (wasStale) {
      WakeUpSessionCaps();
      if (recheckAfterRenew && hasCap && !CapsEmpty(CapsMinus(capImplemented, capIssued))) {
        CheckCaps(true);   // proposed fix: finish revocations handled while the ttl was expired
      }
    }
    AnnounceImplemented();
    TryPendingOps();
  }

  /* Client::wake_up_session_caps(session, reconnect=false) */
  fun WakeUpSessionCaps() {
    if (hasCap && capGen < sessGen) {
      // mds did not re-issue stale cap
      capIssued = CapsNone();
      capImplemented = CapsNone();
      // make sure mds knows what we want
      if (!CapsSubset(CapsFileWanted(), capWanted)) {
        capDropped = true;
      }
    }
  }

  /* ---------------- Inode cap accounting ---------------- */

  fun AnnounceImplemented() {
    announce eClientImplemented, (client = id, caps = CapsImplemented());
  }

  /* Inode::caps_file_wanted() */
  fun CapsFileWanted() : tCaps {
    var r: tCaps;
    if (openRd > 0) { r = CapsUnion(r, CapsForMode(MODE_RD)); }
    if (openWr > 0) { r = CapsUnion(r, CapsForMode(MODE_WR)); }
    return r;
  }

  /* Inode::cap_refs: the in-flight op plus the Fc|Fb ref of the dirty set */
  fun CapRefs() : tCaps {
    var r: tCaps;
    r = opRefs;
    if (dirty) { r = CapsUnion(r, Caps2(Fc, Fb)); }
    return r;
  }

  /* Client::get_caps_used(): refs, plus Fc while the object cacher holds data */
  fun CapsUsed() : tCaps {
    var r: tCaps;
    r = CapRefs();
    if (cacheValid) { r += (Fc); }
    return r;
  }

  /* Inode::caps_wanted() */
  fun CapsWanted() : tCaps {
    var w: tCaps;
    w = CapsUnion(CapsFileWanted(), CapsUsed());
    if (Fb in w) { w += (Fx); }
    return w;
  }

  /* Inode::caps_issued(): only valid caps count */
  fun CapsIssued() : tCaps {
    if (CapIsValid()) { return capIssued; }
    return CapsNone();
  }

  fun CapsImplemented() : tCaps {
    if (CapIsValid()) { return capImplemented; }
    return CapsNone();
  }

  /* Inode::caps_issued_mask(mask, allow_impl) */
  fun CapsIssuedMask(mask: tCaps, allowImpl: bool) : bool {
    if (CapsSubset(mask, CapsIssued())) { return true; }
    if (allowImpl && CapsSubset(mask, CapsUnion(CapsIssued(), CapsImplemented()))) { return true; }
    return false;
  }

  /* Client::check_cap_issue(in, issued), with the kernel client's invalidation when enabled */
  fun CheckCapIssue(had: tCaps, issued: tCaps) {
    if (invalidateOnFcGrant && (Fc in issued) && !(Fc in had)) {
      ReleaseCache();
    }
  }

  /* Client::_release(in): drop the clean cache unless a cached read holds Fc */
  fun ReleaseCache() : bool {
    if (!(Fc in CapRefs())) {
      cacheValid = false;
      return true;
    }
    return false;
  }

  /* Client::_flush(in, onfinish): true when there is nothing to flush */
  fun StartFlush() : bool {
    if (!dirty) {
      return true;
    }
    if (!flushInflight) {
      flushInflight = true;
      flushVer = 0;
      MaybeSendFlush();
    }
    return false;
  }

  /* the object cacher writes the dirty data back once it knows its version */
  fun MaybeSendFlush() {
    if (flushInflight && flushVer == 0 && !ticketPending) {
      flushVer = dirtyVer;
      send store, eStoreWrite, (from = this, ver = dirtyVer);
    }
  }

  /* static is_max_size_approaching(in) with size == 0 */
  fun IsMaxSizeApproaching() : bool {
    if (Fw in flushingCaps) { return false; }
    return maxSize == 0;   // size >= max_size
  }

  /* Client::put_cap_ref(): refs that reached zero while the cap is gone */
  fun PutCapRefs(before: tCaps) {
    var last: tCaps;
    var drop: tCaps;
    last = CapsMinus(before, CapRefs());
    drop = CapsMinus(last, CapsIssued());
    if (!CapsEmpty(drop)) {
      CheckCaps(false);
    }
  }

  /* ---------------- check_caps / send_cap ---------------- */

  /* Client::mark_caps_flushing() */
  fun MarkCapsFlushing() : tFlushTid {
    var f: tFlushTid;
    lastFlushTid = lastFlushTid + 1;
    f = (tid = lastFlushTid, caps = dirtyCaps);
    flushTids += (sizeof(flushTids), f);
    flushingCaps = CapsUnion(flushingCaps, dirtyCaps);
    dirtyCaps = CapsNone();
    return f;
  }

  /* Client::send_cap() */
  fun SendCap(used: tCaps, want: tCaps, retain: tCaps, flush: tFlushTid) {
    var revoking: tCaps;
    var ms: int;
    revoking = CapsMinus(capImplemented, capIssued);
    retain = CapsMinus(retain, revoking);
    capIssued = CapsInter(capIssued, retain);
    capImplemented = CapsInter(capImplemented, CapsUnion(capIssued, used));
    AnnounceImplemented();
    if (CapsIntersects(want, AnyFileWr())) {
      ms = wantedMaxSize;
      requestedMaxSize = wantedMaxSize;
    } else {
      ms = 0;
      requestedMaxSize = 0;
    }
    capWanted = want;
    send mds, eCapUpdate, (client = id, cap_id = capId, cap_seq = capSeq, issue_seq = capIssueSeq,
                           caps = capImplemented, wanted = want, dirty = flush.caps,
                           tid = flush.tid, max_size = ms);
  }

  /* Client::check_caps(in, flags) */
  fun CheckCaps(nodelay: bool) {
    var wanted: tCaps;
    var used: tCaps;
    var origUsed: tCaps;
    var issued: tCaps;
    var implemented: tCaps;
    var revoking: tCaps;
    var retain: tCaps;
    var ack: bool;
    var flush: tFlushTid;
    if (!hasCap) {
      return;
    }
    wanted = CapsWanted();
    used = CapsUsed();
    origUsed = used;
    issued = CapsIssued();
    implemented = CapsImplemented();
    revoking = CapsMinus(implemented, issued);
    retain = CapsUnion(wanted, used);
    if (!CapsEmpty(wanted)) {
      retain = CapsAll();
    } else {
      retain += (Fs);   // CEPH_CAP_ANY_SHARED
      if (maxSize == 0) {
        retain = CapsUnion(retain, Caps3(Fs, Fr, Fc));   // CEPH_CAP_ANY_RD
      }
    }
    if (!(Fb in origUsed) && CapsIntersects(CapsInter(revoking, used), Caps1(Fc))) {
      if (ReleaseCache()) {
        used -= (Fc);
      }
    }
    // the per-cap loop works on the raw Cap fields
    revoking = CapsMinus(capImplemented, capIssued);
    ack = false;
    if (wantedMaxSize > maxSize && wantedMaxSize > requestedMaxSize) {
      ack = true;
    } else if ((Fw in capIssued) && IsMaxSizeApproaching()) {
      ack = true;
    } else if (!CapsEmpty(revoking) && !CapsIntersects(revoking, used)) {
      ack = true;   // completed revocation
    } else if (!CapsEmpty(CapsMinus(wanted, CapsUnion(capWanted, capIssued)))) {
      ack = true;   // want more caps from mds
    } else {
      if (CapsEmpty(CapsMinus(capIssued, retain)) && CapsEmpty(dirtyCaps)) {
        return;     // nothing we wouldn't like, and nothing dirty
      }
      if (!nodelay) {
        if (!delayQueued) {
          delayQueued = true;
          send this, eDelayedCheck;
        }
        return;
      }
    }
    flush = (tid = 0, caps = CapsNone());
    if (!CapsEmpty(dirtyCaps)) {
      flush = MarkCapsFlushing();
    }
    delayQueued = false;
    SendCap(used, wanted, retain, flush);
  }

  /* ---------------- MDS -> client ---------------- */

  /* Client::add_update_cap() for the auth cap */
  fun AddUpdateCap(r: tCapReply) {
    var issued: tCaps;
    var s: int;
    if (!r.has_cap) {
      return;   // insert_trace: no caps with this reply
    }
    issued = r.caps;
    s = r.cap_seq;
    if (!hasCap) {
      hasCap = true;
      capIssued = CapsNone();
      capImplemented = CapsNone();
      capWanted = CapsNone();
      capSeq = 0;
      capIssueSeq = 0;
      capGen = sessGen;
    } else {
      if (capGen < sessGen) {
        capIssued = CapsNone();
        capImplemented = CapsNone();
      }
      if (s <= capSeq) {
        // a message that was sent before this reply already moved seq forward
        s = capSeq;
        issued = CapsUnion(issued, capIssued);
      }
    }
    CheckCapIssue(capIssued, issued);
    capId = r.cap_id;
    capIssued = issued;
    capImplemented = CapsUnion(capImplemented, issued);
    capWanted = CapsUnion(capWanted, r.wanted);   // mseq unchanged: cap.wanted |= wanted
    capSeq = s;
    capIssueSeq = s;
    capGen = sessGen;
    maxSize = r.max_size;
    AnnounceImplemented();
  }

  /* Client::_open(): the open ref is taken first; the request is skipped when the caps are held */
  fun AppOpen(mode: tMode) {
    // Inode::get_open_ref(cmode): make note of pending open, since it effects wanted caps
    if (mode == MODE_RD || mode == MODE_RDWR) { openRd = openRd + 1; }
    if (mode == MODE_WR || mode == MODE_RDWR) { openWr = openWr + 1; }
    if (CapsIssuedMask(CapsForMode(mode), false)) {
      CheckCaps(true);   // update wanted
      TryPendingOps();
      return;
    }
    pendingOpens += (sizeof(pendingOpens), mode);
    TryPendingOps();
  }

  fun HandleOpenReply(r: tCapReply) {
    if (evicted) { return; }
    assert op == OP_OPEN, "open reply without an open in flight";
    pendingOpens -= (0);
    AddUpdateCap(r);
    op = OP_NONE;
    TryPendingOps();
  }

  fun HandleGetattrReply(r: tCapReply) {
    if (evicted) { return; }
    assert op == OP_GETATTR, "getattr reply without a getattr in flight";
    AddUpdateCap(r);
    announce eStatDone, id;
    op = OP_NONE;
    TryPendingOps();
  }

  fun HandleSetattrReply(r: tCapReply) {
    if (evicted) { return; }
    assert op == OP_SETATTR, "setattr reply without a setattr in flight";
    AddUpdateCap(r);
    announce eSetattrDone, id;
    op = OP_NONE;
    TryPendingOps();
  }

  /* Client::handle_caps() + handle_cap_grant() */
  fun HandleCapGrant(m: tCapGrant) {
    var used: tCaps;
    var wanted: tCaps;
    var newCaps: tCaps;
    var revoked: tCaps;
    var wasStale: bool;
    var check: bool;
    var nodelay: bool;
    if (evicted) { return; }
    if (!hasCap) {
      // "don't have cap, immediately releasing": the MDS may be waiting on it
      send mds, eCapRelease, (client = id, cap_id = m.cap_id, issue_seq = m.cap_seq);
      return;
    }
    used = CapsUsed();
    wanted = CapsWanted();
    newCaps = m.caps;
    wasStale = sessGen > capGen;
    if (wasStale) {
      capIssued = CapsNone();        // CEPH_CAP_PIN
      capImplemented = CapsNone();
    }
    capSeq = m.cap_seq;
    capGen = sessGen;
    CheckCapIssue(capIssued, newCaps);
    // max_size
    if (CapsIntersects(newCaps, AnyFileWr()) && m.max_size != maxSize) {
      maxSize = m.max_size;
      if (maxSize > wantedMaxSize) {
        wantedMaxSize = 0;
        requestedMaxSize = 0;
      }
    }
    check = false;
    nodelay = false;
    if (wasStale && !CapsEmpty(CapsMinus(wanted, CapsUnion(capWanted, newCaps)))) {
      // we may not have told the mds what we want while the session was stale
      check = true;
    }
    revoked = CapsMinus(capIssued, newCaps);
    if (!CapsEmpty(revoked)) {
      capIssued = newCaps;
      capImplemented = CapsUnion(capImplemented, newCaps);
      AnnounceImplemented();
      if (CapsIntersects(CapsInter(used, revoked), Caps1(Fb)) && !StartFlush()) {
        // waiting for the flush; _flushed() answers the MDS
      } else if (CapsIntersects(CapsInter(used, revoked), Caps1(Fc))) {
        if (ReleaseCache()) {
          check = true;
          nodelay = true;
        }
      } else {
        capWanted = CapsNone();   // don't let check_caps skip sending a response
        check = true;
        nodelay = true;
      }
    } else if (capIssued == newCaps) {
      // caps unchanged
      AnnounceImplemented();
    } else {
      capIssued = newCaps;
      capImplemented = CapsUnion(capImplemented, newCaps);
      AnnounceImplemented();
    }
    // just in case the caps was released just before we got the revoke
    if (!check && m.op == OP_REVOKE) {
      capWanted = CapsNone();
      check = true;
      nodelay = true;
    }
    if (check) {
      CheckCaps(nodelay);
    }
    if (!CapsEmpty(newCaps)) {
      TryPendingOps();   // signal_caps_inode
    }
  }

  /* Client::handle_cap_flush_ack() */
  fun HandleFlushAck(a: tFlushAck) {
    var cleaned: tCaps;
    var i: int;
    var stop: bool;
    if (evicted) { return; }
    i = 0;
    stop = false;
    while (i < sizeof(flushTids) && !stop) {
      if (flushTids[i].tid == a.tid) {
        cleaned = flushTids[i].caps;
      }
      if (flushTids[i].tid <= a.tid) {
        flushTids -= (i);
      } else {
        cleaned = CapsMinus(cleaned, flushTids[i].caps);
        if (CapsEmpty(cleaned)) {
          stop = true;
        }
        i = i + 1;
      }
    }
    if (!CapsEmpty(cleaned)) {
      flushingCaps = CapsMinus(flushingCaps, cleaned);
    }
  }

  /* ---------------- the object cacher ---------------- */

  fun HandleStoreWriteAck(ver: int) {
    var before: tCaps;
    if (op == OP_WRITE_SYNC) {
      CompleteSyncWrite(ver);
      return;
    }
    assert flushInflight, "store write ack without a flush in flight";
    if (dirtyVer != flushVer || ticketPending) {
      // more data was buffered while flushing: the set is still dirty
      flushVer = 0;
      MaybeSendFlush();
      return;
    }
    // Client::flush_set_callback -> _flushed -> put_cap_ref(Fc|Fb)
    flushInflight = false;
    before = CapRefs();
    dirty = false;
    PutCapRefs(before);
    TryPendingOps();
  }

  /* ---------------- the application ---------------- */

  fun TryPendingOps() {
    if (evicted) {
      DropPendingOps();
      return;
    }
    if (op != OP_NONE) {
      return;
    }
    if (sizeof(pendingOpens) > 0) {
      op = OP_OPEN;
      send mds, eOpenReq, (client = id, from = this, mode = pendingOpens[0]);
      return;
    }
    while (pendingStats > 0) {
      if (TryStartStat()) { return; }
    }
    while (pendingSetattrs > 0) {
      if (TryStartSetattr()) { return; }
    }
    if (pendingReads > 0) {
      if (TryStartRead()) { return; }
    }
    if (pendingWrites > 0) {
      TryStartWrite();
    }
  }

  /*
   * Client::get_caps(): the I_CAP_DROPPED recovery. With an auth cap
   * _renew_caps() reduces to check_caps(CHECK_CAPS_NODELAY).
   */
  fun HandleCapDropped(need: tCaps) {
    if (!capDropped) { return; }
    if (!CapsSubset(need, capWanted)) {
      CheckCaps(true);
    }
    if (CapsSubset(CapsFileWanted(), capWanted)) {
      capDropped = false;
    }
  }

  /* Client::_getattr(): local when Fs is held, else a GETATTR request */
  fun TryStartStat() : bool {
    pendingStats = pendingStats - 1;
    if (CapsIssuedMask(Caps1(Fs), true)) {
      announce eStatDone, id;
      return false;
    }
    op = OP_GETATTR;
    send mds, eGetattrReq, (client = id, from = this, has_release = false, release = default(tReqRelease));
    return true;
  }

  /* Client::encode_inode_release(in, req, drop, unless=0) */
  fun EncodeInodeRelease(drop: tCaps) : tMdsReq {
    var released: bool;
    var rel: tReqRelease;
    released = false;
    if (hasCap) {
      drop = CapsMinus(drop, CapsUnion(dirtyCaps, CapsUsed()));
      if (CapsIntersects(drop, capIssued)) {
        capIssued = CapsMinus(capIssued, drop);
        capImplemented = CapsMinus(capImplemented, drop);
        AnnounceImplemented();
        released = true;
      }
      if (released) {
        capWanted = CapsWanted();
        if (!CapsIntersects(capWanted, AnyFileWr())) {
          requestedMaxSize = 0;
        }
        rel = (cap_id = capId, cap_seq = capSeq, issue_seq = capIssueSeq,
               caps = capImplemented, wanted = capWanted);
      }
    }
    return (client = id, from = this, has_release = released, release = rel);
  }

  /* Client::_do_setattr(CEPH_SETATTR_MTIME) */
  fun TryStartSetattr() : bool {
    pendingSetattrs = pendingSetattrs - 1;
    if (CapsIssuedMask(Caps1(Fx), false)) {
      dirtyCaps += (Fx);     // mark_caps_dirty(CEPH_CAP_FILE_EXCL)
      announce eSetattrDone, id;
      CheckCaps(false);
      return false;
    }
    if (CapsIssuedMask(Caps1(Fw), false)) {
      dirtyCaps += (Fw);     // mark_caps_dirty(CEPH_CAP_FILE_WR)
      announce eSetattrDone, id;
      CheckCaps(false);
      return false;
    }
    op = OP_SETATTR;
    send mds, eSetattrReq, EncodeInodeRelease(Caps3(Fs, Fr, Fw));
    return true;
  }

  /* Client::get_caps(fh, need=Fr, want=Fc) followed by _read */
  fun TryStartRead() : bool {
    var have: tCaps;
    var revoking: tCaps;
    if (openRd == 0) {
      // every blocked reader gets EBADF
      while (pendingReads > 0) {
        pendingReads = pendingReads - 1;
        announce eIoDropped, id;
      }
      return false;
    }
    have = CapsIssued();
    revoking = CapsMinus(CapsImplemented(), have);
    if (!(Fr in have) || (Fc in revoking)) {
      HandleCapDropped(Caps1(Fr));
      return false;   // waiting for caps
    }
    pendingReads = pendingReads - 1;
    op = OP_READ;
    opRefs = Caps1(Fr);
    opCached = Fc in have;
    if (opCached) {
      opRefs += (Fc);
    }
    announce eReadStart, id;
    if (opCached && cacheValid) {
      CompleteRead(cacheVer);
      return true;
    }
    send store, eStoreRead, this;
    return true;
  }

  fun CompleteRead(ver: int) {
    var before: tCaps;
    assert op == OP_READ, "read completion without a read in flight";
    if (opCached) {
      cacheValid = true;
      cacheVer = ver;
    }
    announce eReadDone, (client = id, ver = ver);
    before = CapRefs();
    op = OP_NONE;
    opRefs = CapsNone();
    PutCapRefs(before);
    TryPendingOps();
  }

  /* Client::get_caps(fh, need=Fw, want=Fb, endoff) followed by _write */
  fun TryStartWrite() : bool {
    var have: tCaps;
    var revoking: tCaps;
    if (openWr == 0) {
      while (pendingWrites > 0) {
        pendingWrites = pendingWrites - 1;
        announce eIoDropped, id;
      }
      return false;
    }
    have = CapsIssued();
    revoking = CapsMinus(CapsImplemented(), have);
    if (Fw in have) {
      // endoff (1) >= max_size || endoff > size << 1 (0): ask for a range
      if (wantedMaxSize < 1) {
        wantedMaxSize = 1;
      }
      if (wantedMaxSize > maxSize && wantedMaxSize > requestedMaxSize) {
        CheckCaps(false);
      }
      if (maxSize < 1) {
        return false;   // waiting on max_size
      }
    }
    if (!(Fw in have) || (Fb in revoking)) {
      HandleCapDropped(Caps1(Fw));
      return false;   // waiting for caps
    }
    pendingWrites = pendingWrites - 1;
    if (Fb in have) {
      // buffered: the data enters the object cacher now, under the client
      // lock, with the Fc|Fb ref of the dirty set; only its version is
      // still to be assigned
      op = OP_WRITE_BUF;
      opRefs = Caps3(Fw, Fb, Fc);
      if (dirty) { superVer = dirtyVer; } else { superVer = 0; }
      dirty = true;
      ticketPending = true;
      send store, eStoreTicket, this;
    } else {
      op = OP_WRITE_SYNC;
      opRefs = Caps2(Fw, Fb);   // get_caps(Fw) + the Fb ref of the sync write
      send store, eStoreWrite, (from = this, ver = 0);
    }
    return true;
  }

  fun CompleteBufferedWrite(ver: int) {
    var before: tCaps;
    assert op == OP_WRITE_BUF, "ticket without a buffered write in flight";
    ticketPending = false;
    if (superVer > 0 && !(flushInflight && flushVer == superVer)) {
      // the previous buffered version was overwritten before it was written back
      announce eWriteSuperseded, (older = superVer, newer = ver);
    }
    superVer = 0;
    dirtyVer = ver;
    cacheValid = true;   // the object cacher now holds the new data
    cacheVer = ver;
    dirtyCaps += (Fw);   // mark_caps_dirty(CEPH_CAP_FILE_WR)
    announce eWriteDone, (client = id, ver = ver);
    if (evicted) {
      // the session was killed while the write was being buffered: the data is lost
      dirty = false;
      op = OP_NONE;
      opRefs = CapsNone();
      return;
    }
    MaybeSendFlush();
    before = CapRefs();
    op = OP_NONE;
    opRefs = CapsNone();
    PutCapRefs(before);
    TryPendingOps();
  }

  fun CompleteSyncWrite(ver: int) {
    var before: tCaps;
    dirtyCaps += (Fw);
    announce eWriteDone, (client = id, ver = ver);
    before = CapRefs();
    op = OP_NONE;
    opRefs = CapsNone();
    PutCapRefs(before);
    TryPendingOps();
  }

  /* Client::_release_fh */
  fun AppClose(mode: tMode) {
    var last: bool;
    last = false;
    if ((mode == MODE_RD || mode == MODE_RDWR) && openRd > 0) {
      openRd = openRd - 1;
      if (openRd == 0) { last = true; }
    }
    if ((mode == MODE_WR || mode == MODE_RDWR) && openWr > 0) {
      openWr = openWr - 1;
      if (openWr == 0) { last = true; }
    }
    if (last) {
      StartFlush();
      CheckCaps(false);
    }
    TryPendingOps();   // blocked readers and writers of this mode fail with EBADF
  }

  /* Client::trim_caps -> remove_cap(cap, queue_release=true) for an idle inode */
  fun AppTrim() {
    if (!hasCap || op != OP_NONE || openRd > 0 || openWr > 0 ||
        sizeof(pendingOpens) > 0 || pendingReads > 0 || pendingWrites > 0 ||
        pendingStats > 0 || pendingSetattrs > 0 ||
        dirty || !CapsEmpty(dirtyCaps) || !CapsEmpty(flushingCaps)) {
      return;
    }
    hasCap = false;
    cacheValid = false;
    delayQueued = false;
    capDropped = false;
    AnnounceImplemented();
    send mds, eCapRelease, (client = id, cap_id = capId, issue_seq = capIssueSeq);
  }
}

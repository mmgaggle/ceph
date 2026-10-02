/*
 * The auth MDS for one regular file.
 *
 * What is modelled (function names refer to src/mds/Locker.cc, Capability.h,
 * CInode.cc, Server.cc and SessionMap.h):
 *   - Capability::issue / issue_norevoke / confirm_receipt / revoke /
 *     clean_revoke_from / revalidate, with the session cap_gen that makes a
 *     cap invalid once the session went stale
 *   - Locker::issue_caps including the stale and re-issue branches,
 *     get_allowed_caps, share_inode_max_size
 *   - Locker::handle_client_caps, adjust_cap_wanted, _do_cap_update,
 *     process_request_cap_release, kick_cap_releases
 *   - Locker::_do_cap_release, remove_client_cap
 *   - Locker::eval, eval_gather, file_eval, simple_sync, simple_lock,
 *     file_excl, scatter_mix, file_xsyn, simple_xlock, rdlock_start,
 *     rdlock_finish, xlock_start, xlock_finish, _finish_xlock, with the
 *     lock waiters that retry a blocked request
 *   - CInode loner selection (calc_ideal_loner, choose_ideal_loner,
 *     try_set_loner, try_drop_loner)
 *   - client_ranges / max_size: check_inode_max_size, the journal wrlock,
 *     and the C_MDL_CheckMaxSize WAIT_STABLE waiter
 *   - Server::handle_client_open, handle_client_getattr (filelock part),
 *     handle_client_setattr for mtime (filelock xlock, early reply),
 *     encode_inodestat on the reply
 *   - Server::find_idle_sessions -> revoke_stale_caps / CEPH_SESSION_STALE,
 *     RENEWCAPS -> resume_stale_caps, revoke_stale_cap
 *
 * Not modelled: other locks (auth, link, xattr), replicas, snapshots,
 * client eviction and blocklisting (a stale session that holds revoking
 * write caps stays stale, as with defer_client_eviction_on_laggy_osds),
 * file recovery (STATE_NEEDSRECOVER), cap export/import, truncation of
 * file data, request batching.
 *
 * max_size is reduced to 0 ("no writeable range") or 1 ("has a range"),
 * because the model never grows the file.
 */

type tRevoke = (before: tCaps, rseq: int, last_issue: int);

type tMdsCap = (cap_id: int, cap_gen: int, pending: tCaps, issued: tCaps, revokes: seq[tRevoke],
                last_sent: int, last_issue: int, wanted: tCaps,
                is_new: bool, suppress: int, client_writeable: bool);

enum tReqKind { REQ_GETATTR = 0, REQ_SETATTR = 1 }

/* an MDRequest on the file, keyed by request id */
type tReqState = (kind: tReqKind, client: int, from: machine, locking: bool, cap_rel_seq: int,
                  waiting: bool, wait_mask: int, has_rdlock: bool, has_xlock: bool,
                  early_replied: bool);

/* C_Locker_FileUpdate_finish or C_MDS_inode_update_finish: a journal entry is durable */
type tJournalDone = (client: int, tid: int, ack: bool, dirty: tCaps,
                     need_issue: bool, share_max: bool, wrlocked: bool, request: int);
event eJournalDone : tJournalDone;
/* C_Locker_RevokeStaleCap queued by issue_caps */
event eRevokeStaleCap : int;

type tCapsSplit = (loner: tCaps, other: tCaps, xlocker: tCaps, all: tCaps);

/*
 * gather_lock_to_mix enables a proposed fix: scatter_mix() from LOCK gathers
 * the Fc and Fb caps that LOCK allows before the lock becomes MIX, instead
 * of granting Fr and Fw to other clients while buffered data is unflushed
 * (README, finding 3).
 */
type tMdsConfig = (store: machine, gather_lock_to_mix: bool);


machine MDS {
  var clients: map[int, machine];
  var caps: map[int, tMdsCap];
  var sessStale: map[int, bool];  // Session::is_stale()
  var sessClosed: map[int, bool]; // the session was killed (client evicted)
  var staleRevokes: seq[int];     // C_Locker_RevokeStaleCap contexts queued at the front
  var store: machine;
  var osdEpoch: int;              // osd epoch barrier after a blocklist (not yet used)
  var gatherLockToMix: bool;
  var sessGen: map[int, int];     // Session::cap_gen
  var reqs: map[int, tReqState];  // active MDRequests by request id
  var nextReqId: int;
  var lockState: tLockState;
  var nrdlock: int;
  var nwrlock: int;               // filelock wrlocks held by journaling cap updates
  var nxlock: int;
  var xlockBy: int;               // SimpleLock::get_xlock_by_client(), -1 when none
  var loner: int;                 // CInode::loner_cap, -1 when none
  var wantLoner: int;             // CInode::want_loner_cap
  var clientRanges: set[int];     // clients with a non-zero client_ranges entry
  var waitStableCheckMax: bool;   // a C_MDL_CheckMaxSize waits for WAIT_STABLE
  var needIssue: bool;            // the `bool *need_issue` out-parameter
  var nextCapId: int;             // Capability ids are unique per incarnation

  start state Serving {
    entry (cfg: tMdsConfig) {
      store = cfg.store;
      gatherLockToMix = cfg.gather_lock_to_mix;
      lockState = LOCK_SYNC;
      loner = -1;
      wantLoner = -1;
      xlockBy = -1;
      nextCapId = 1;
      nextReqId = 1;
    }
    on eOpenReq        do (r: tOpenReq)       { HandleOpen(r); DrainStaleRevokes(); }
    on eGetattrReq     do (m: tMdsReq)        { HandleRequest(REQ_GETATTR, m); DrainStaleRevokes(); }
    on eSetattrReq     do (m: tMdsReq)        { HandleRequest(REQ_SETATTR, m); DrainStaleRevokes(); }
    on eCapUpdate      do (m: tCapUpdate)     { HandleCapUpdate(m); DrainStaleRevokes(); }
    on eCapRelease     do (m: tCapRelease)    { HandleCapRelease(m); DrainStaleRevokes(); }
    on eRenewCaps      do (m: tRenewCaps)     { HandleRenewCaps(m); DrainStaleRevokes(); }
    on eSessionTimeout do (c: int)            { HandleSessionTimeout(c); DrainStaleRevokes(); }
    on eJournalDone    do (j: tJournalDone)   { HandleJournalDone(j); DrainStaleRevokes(); }
  }

  /* mds->queue_waiter_front(): these contexts run before the next message is dispatched */
  fun DrainStaleRevokes() {
    var c: int;
    while (sizeof(staleRevokes) > 0) {
      c = staleRevokes[0];
      staleRevokes -= (0);
      HandleRevokeStaleCap(c);
    }
  }

  /* ---------------- waiters ---------------- */
  /* SimpleLock::WAIT_RD = 1, WAIT_WR = 2, WAIT_STABLE = 4, WAIT_XLOCK = 8 */

  fun BitSet(mask: int, bit: int) : bool {
    return (mask / bit) % 2 == 1;
  }

  fun MaskOverlap(a: int, b: int) : bool {
    return (BitSet(a, 1) && BitSet(b, 1)) || (BitSet(a, 2) && BitSet(b, 2)) ||
           (BitSet(a, 4) && BitSet(b, 4)) || (BitSet(a, 8) && BitSet(b, 8));
  }

  /* SimpleLock::is_waiter_for(mask) */
  fun HasWaiter(mask: int) : bool {
    var rid: int;
    if (BitSet(mask, 4) && waitStableCheckMax) { return true; }
    foreach (rid in keys(reqs)) {
      if (reqs[rid].waiting && MaskOverlap(reqs[rid].wait_mask, mask)) { return true; }
    }
    return false;
  }

  fun AddWaiter(rid: int, mask: int) {
    reqs[rid].waiting = true;
    reqs[rid].wait_mask = mask;
  }

  /*
   * SimpleLock::finish_waiters(mask): MDSCacheObject::finish_waiting runs
   * the contexts inline, and C_MDS_RetryRequest re-dispatches the request
   * inline as well.
   */
  fun RunWaiters(mask: int) {
    var rs: seq[int];
    var rid: int;
    if (BitSet(mask, 4) && waitStableCheckMax) {
      waitStableCheckMax = false;
      CheckInodeMaxSize(false);
    }
    foreach (rid in keys(reqs)) {
      if (reqs[rid].waiting && MaskOverlap(reqs[rid].wait_mask, mask)) { rs += (sizeof(rs), rid); }
    }
    foreach (rid in rs) {
      if ((rid in reqs) && reqs[rid].waiting) {
        reqs[rid].waiting = false;
        reqs[rid].wait_mask = 0;
        DispatchRequest(rid);
      }
    }
  }

  fun SetLock(s: tLockState) {
    announce eLockTransition, (prev = lockState, next = s);
    lockState = s;
  }

  /* ---------------- Capability ---------------- */

  fun NewCap() : tMdsCap {
    var c: tMdsCap;
    c.cap_id = nextCapId;
    nextCapId = nextCapId + 1;
    return c;
  }

  fun AnnounceIssued(c: int) {
    if (c in caps) {
      announce eMdsIssued, (client = c, caps = caps[c].issued);
    } else {
      announce eMdsIssued, (client = c, caps = CapsNone());
    }
  }

  fun EnsureSession(c: int, from: machine) {
    clients[c] = from;
    if (!(c in sessGen)) {
      sessGen[c] = 0;
      sessStale[c] = false;
      sessClosed[c] = false;
    }
  }

  fun SessionClosed(c: int) : bool {
    return (c in sessClosed) && sessClosed[c];
  }

  /* Capability::is_stale(): the session is stale */
  fun CapIsStale(c: int) : bool {
    return sessStale[c];
  }

  /* Capability::is_valid(): the session cap_gen did not move on */
  fun CapIsValid(c: int) : bool {
    return sessGen[c] == caps[c].cap_gen;
  }

  /* Capability::revalidate() */
  fun CapRevalidate(c: int) {
    if (!CapIsValid(c)) {
      caps[c].cap_gen = sessGen[c];
    }
  }

  /* Capability::calc_issued() */
  fun CalcIssued(c: int) : tCaps {
    var r: tCaps;
    var i: int;
    r = caps[c].pending;
    i = 0;
    while (i < sizeof(caps[c].revokes)) {
      r = CapsUnion(r, caps[c].revokes[i].before);
      i = i + 1;
    }
    return r;
  }

  fun CapRevoking(c: int) : tCaps {
    return CapsMinus(caps[c].issued, caps[c].pending);
  }

  /* Capability::issue(c, reval) */
  fun CapIssue(c: int, newc: tCaps, reval: bool) : int {
    var rv: tRevoke;
    var last: int;
    if (reval) { CapRevalidate(c); }
    if (!CapsSubset(caps[c].pending, newc)) {
      // revoking (and maybe adding) bits: note caps prior to this revocation
      rv = (before = caps[c].pending, rseq = caps[c].last_sent, last_issue = caps[c].last_issue);
      caps[c].revokes += (sizeof(caps[c].revokes), rv);
      caps[c].pending = newc;
      caps[c].issued = CapsUnion(caps[c].issued, newc);
    } else if (!CapsSubset(newc, caps[c].pending)) {
      // adding bits only: drop old revokes with no bits we don't have
      caps[c].pending = CapsUnion(caps[c].pending, newc);
      caps[c].issued = CapsUnion(caps[c].issued, newc);
      last = sizeof(caps[c].revokes) - 1;
      while (last >= 0 && CapsEmpty(CapsMinus(caps[c].revokes[last].before, caps[c].pending))) {
        caps[c].revokes -= (last);
        last = last - 1;
      }
    } else {
      assert caps[c].pending == newc, "Capability::issue: no change expected";
    }
    caps[c].last_sent = caps[c].last_sent + 1;
    AnnounceIssued(c);
    return caps[c].last_sent;
  }

  /* Capability::issue_norevoke(c, reval=true) */
  fun CapIssueNoRevoke(c: int, newc: tCaps) : int {
    CapRevalidate(c);
    caps[c].pending = CapsUnion(caps[c].pending, newc);
    caps[c].issued = CapsUnion(caps[c].issued, newc);
    caps[c].is_new = false;
    caps[c].last_sent = caps[c].last_sent + 1;
    AnnounceIssued(c);
    return caps[c].last_sent;
  }

  /* Capability::confirm_receipt(seq, caps); returns the bits that were revoked */
  fun CapConfirmReceipt(c: int, sq: int, rcaps: tCaps) : tCaps {
    var wasRevoking: tCaps;
    var rv: tRevoke;
    wasRevoking = CapRevoking(c);
    if (sq == caps[c].last_sent) {
      caps[c].revokes = default(seq[tRevoke]);
      caps[c].issued = rcaps;
      caps[c].pending = CapsInter(caps[c].pending, rcaps);   // don't add bits
      // if the revoking is not totally finished just add the new revoking caps back
      if (!CapsEmpty(wasRevoking) && !CapsEmpty(CapRevoking(c))) {
        rv = (before = caps[c].pending, rseq = caps[c].last_sent, last_issue = caps[c].last_issue);
        caps[c].revokes += (sizeof(caps[c].revokes), rv);
      }
    } else {
      // can i forget any revocations?
      while (sizeof(caps[c].revokes) > 0 && caps[c].revokes[0].rseq < sq) {
        caps[c].revokes -= (0);
      }
      if (sizeof(caps[c].revokes) > 0) {
        if (caps[c].revokes[0].rseq == sq) {
          caps[c].revokes[0].before = rcaps;
        }
        caps[c].issued = CalcIssued(c);
      } else {
        // seq < last_sent
        caps[c].issued = CapsUnion(rcaps, caps[c].pending);
      }
    }
    AnnounceIssued(c);
    return CapsMinus(wasRevoking, caps[c].issued);
  }

  /* Capability::revoke(): forcibly complete the revocation */
  fun CapRevoke(c: int) : tCaps {
    if (!CapsEmpty(CapRevoking(c))) {
      return CapConfirmReceipt(c, caps[c].last_sent, caps[c].pending);
    }
    return CapsNone();
  }

  /* Capability::clean_revoke_from(li) */
  fun CapCleanRevokeFrom(c: int, li: int) {
    var changed: bool;
    changed = false;
    while (sizeof(caps[c].revokes) > 0 && caps[c].revokes[0].last_issue <= li) {
      caps[c].revokes -= (0);
      changed = true;
    }
    if (changed) {
      caps[c].issued = CalcIssued(c);
      AnnounceIssued(c);
    }
  }

  /* Capability::is_notable(), reconstructed from what sets and clears STATE_NOTABLE */
  fun CapIsNotable(c: int) : bool {
    return !CapsEmpty(CapRevoking(c)) || caps[c].client_writeable ||
           CapsIntersects(caps[c].wanted, Caps4(Fx, Fw, Fb, Fr));
  }

  fun MaxSizeOf(c: int) : int {
    if (c in clientRanges) { return 1; }
    return 0;
  }

  /* ---------------- CInode cap / loner helpers ---------------- */

  fun TargetLoner() : int {
    if (loner == wantLoner) { return loner; }
    return -1;
  }

  /* CInode::get_xlocker_mask(client): the xlocker may hold Fs Fx Fc Fr */
  fun XlockerMask(c: int) : tCaps {
    if (c >= 0 && c == xlockBy) { return Caps4(Fs, Fx, Fc, Fr); }
    return CapsNone();
  }

  fun Gcaps(who: tCapWho) : tCaps {
    return LockGcapsAllowed(who, lockState, xlockBy >= 0);
  }

  fun GcapsNext(who: tCapWho, s: tLockState) : tCaps {
    return LockGcapsAllowed(who, s, xlockBy >= 0);
  }

  /* CInode::get_caps_issued() split into loner, other and xlocker */
  fun GetCapsIssued() : tCapsSplit {
    var r: tCapsSplit;
    var c: int;
    foreach (c in keys(caps)) {
      r.all = CapsUnion(r.all, caps[c].issued);
      if (c == loner) {
        r.loner = CapsUnion(r.loner, caps[c].issued);
      } else {
        r.other = CapsUnion(r.other, caps[c].issued);
      }
      r.xlocker = CapsUnion(r.xlocker, CapsInter(XlockerMask(c), caps[c].issued));
    }
    return r;
  }

  /* CInode::get_caps_wanted(): stale caps do not count */
  fun GetCapsWanted() : tCapsSplit {
    var r: tCapsSplit;
    var c: int;
    foreach (c in keys(caps)) {
      if (CapIsStale(c)) { continue; }
      r.all = CapsUnion(r.all, caps[c].wanted);
      if (c == loner) {
        r.loner = CapsUnion(r.loner, caps[c].wanted);
      } else {
        r.other = CapsUnion(r.other, caps[c].wanted);
      }
    }
    return r;
  }

  /* CInode::calc_ideal_loner() */
  fun CalcIdealLoner() : int {
    var n: int;
    var l: int;
    var c: int;
    n = 0;
    l = -1;
    foreach (c in keys(caps)) {
      if (!CapIsStale(c) && CapsIntersects(caps[c].wanted, Caps4(Fx, Fw, Fb, Fr))) {
        if (n > 0) { return -1; }
        n = n + 1;
        l = c;
      }
    }
    return l;
  }

  /* CInode::try_drop_loner() */
  fun TryDropLoner() : bool {
    var otherAllowed: tCaps;
    if (loner < 0) { return true; }
    otherAllowed = Gcaps(CAP_ANY);
    if (!(loner in caps) || CapsSubset(caps[loner].issued, otherAllowed)) {
      loner = -1;
      return true;
    }
    return false;
  }

  /* CInode::try_set_loner() */
  fun TrySetLoner() : bool {
    assert wantLoner >= 0, "try_set_loner without a wanted loner";
    if (loner >= 0 && loner != wantLoner) { return false; }
    loner = wantLoner;
    return true;
  }

  /* CInode::choose_ideal_loner() */
  fun ChooseIdealLoner() : bool {
    var changed: bool;
    wantLoner = CalcIdealLoner();
    changed = false;
    if (loner >= 0 && loner != wantLoner) {
      if (!TryDropLoner()) { return false; }
      changed = true;
    }
    if (wantLoner >= 0) {
      if (loner < 0) {
        loner = wantLoner;
        changed = true;
      } else {
        assert loner == wantLoner, "choose_ideal_loner: loner mismatch";
      }
    }
    return changed;
  }

  /* Locker::get_allowed_caps() */
  fun GetAllowedCaps(c: int) : tCaps {
    var allowed: tCaps;
    if (c == loner) {
      allowed = Gcaps(CAP_LONER);
    } else {
      allowed = Gcaps(CAP_ANY);
    }
    // add in any xlocker-only caps (for locks this client is the xlocker for)
    return CapsUnion(allowed, CapsInter(Gcaps(CAP_XLOCKER), XlockerMask(c)));
  }

  /* CInode::get_caps_allowed_for_client() (the request reply path) */
  fun GetCapsAllowedForClient(c: int) : tCaps {
    if (c == loner) {
      return CapsUnion(Gcaps(CAP_LONER), CapsInter(Gcaps(CAP_XLOCKER), XlockerMask(c)));
    }
    return Gcaps(CAP_ANY);
  }

  /* CInode::issued_caps_need_gather(lock) */
  fun IssuedCapsNeedGather() : bool {
    var iss: tCapsSplit;
    iss = GetCapsIssued();
    return !CapsSubset(iss.loner, Gcaps(CAP_LONER)) ||
           !CapsSubset(iss.other, Gcaps(CAP_ANY)) ||
           !CapsSubset(iss.xlocker, Gcaps(CAP_XLOCKER));
  }

  fun CanRead(c: int) : bool {
    return LockWhoAllows(LockRow(lockState).r, c, xlockBy);
  }

  fun CanRdlock(c: int) : bool {
    return LockWhoAllows(LockRow(lockState).rd, c, xlockBy);
  }

  /* can_wrlock: XCL means the xlocker or the exclusive client (the loner) */
  fun CanWrlock(c: int) : bool {
    var w: tLockWho;
    w = LockRow(lockState).wr;
    return w == WHO_ANY || w == WHO_AUTH || (w == WHO_XCL && c >= 0 && (xlockBy == c || loner == c));
  }

  fun CanForceWrlock(c: int) : bool {
    var w: tLockWho;
    w = LockRow(lockState).fwr;
    return w == WHO_ANY || w == WHO_AUTH || (w == WHO_XCL && c >= 0 && (xlockBy == c || loner == c));
  }

  fun CanXlock(c: int) : bool {
    return LockWhoAllows(LockRow(lockState).x, c, xlockBy);
  }

  /*
   * The `bool *need_issue` convention of Locker: when the caller passed a
   * pointer (deferred), record the request; otherwise issue right now.
   */
  fun MaybeIssue(deferred: bool) {
    if (deferred) {
      needIssue = true;
    } else {
      IssueCaps(-1);
    }
  }

  fun ResolveIssue(deferred: bool, saved: bool) {
    if (deferred) {
      needIssue = needIssue || saved;
    } else {
      if (needIssue) { IssueCaps(-1); }
      needIssue = saved;
    }
  }

  /* ---------------- issue_caps ---------------- */

  fun SendGrant(c: int, op: tCapOp, sq: int, newCaps: tCaps) {
    send clients[c], eCapGrant, (op = op, cap_id = caps[c].cap_id, cap_seq = sq, caps = newCaps,
                                 wanted = caps[c].wanted, issue_seq = caps[c].last_issue,
                                 max_size = MaxSizeOf(c));
  }

  /* Locker::issue_caps(in, only_cap); only < 0 means all caps */
  fun IssueCaps(only: int) : int {
    var nissued: int;
    var c: int;
    var allowed: tCaps;
    var pending: tCaps;
    var wanted: tCaps;
    var before: tCaps;
    var after: tCaps;
    var sq: int;
    var op: tCapOp;
    nissued = 0;
    foreach (c in keys(caps)) {
      if (only >= 0 && c != only) { continue; }
      allowed = GetAllowedCaps(c);
      pending = caps[c].pending;
      wanted = caps[c].wanted;
      if (CapsSubset(pending, allowed)) {
        // skip if suppress or new, and not revocation
        if (caps[c].is_new || caps[c].suppress > 0 || CapIsStale(c)) { continue; }
      } else {
        assert !caps[c].is_new, "issue_caps: revoking from a new cap";
        if (CapIsStale(c)) {
          // revoke stale cap from client
          assert !CapIsValid(c), "issue_caps: stale cap is still valid";
          CapIssue(c, CapsInter(allowed, pending), false);
          staleRevokes += (sizeof(staleRevokes), c);
          continue;
        }
        if (!CapIsValid(c) && !CapsEmpty(pending)) {
          // After stale->resume circle, client thinks it only has CEPH_CAP_PIN.
          // mds needs to re-issue caps, then do revocation.
          sq = CapIssue(c, pending, true);
          SendGrant(c, OP_GRANT, sq, pending);
        }
      }
      // are there caps that the client wants and can have, but aren't pending?
      // or do we need to revoke? or re-issue after a stale->resume circle?
      if (!CapsSubset(pending, allowed) ||
          !CapsEmpty(CapsMinus(CapsInter(wanted, allowed), pending)) ||
          !CapIsValid(c)) {
        nissued = nissued + 1;
        before = pending;
        // get_caps_liked() is every F bit for a regular file, so
        // (wanted | likes) & allowed == allowed
        if (!CapsSubset(pending, allowed)) {
          sq = CapIssue(c, CapsInter(allowed, pending), true);   // if revoking, don't issue anything new
        } else {
          sq = CapIssue(c, allowed, true);
        }
        after = caps[c].pending;
        if (CapsEmpty(CapsMinus(before, after))) {
          op = OP_GRANT;
        } else {
          op = OP_REVOKE;
        }
        SendGrant(c, op, sq, after);
      }
    }
    return nissued;
  }

  /* Locker::share_inode_max_size(in, only_cap) */
  fun ShareInodeMaxSize(only: int) {
    var c: int;
    foreach (c in keys(caps)) {
      if (only >= 0 && c != only) { continue; }
      if (caps[c].suppress > 0) { continue; }
      if (CapsIntersects(caps[c].pending, Caps2(Fw, Fb))) {
        caps[c].last_sent = caps[c].last_sent + 1;
        SendGrant(c, OP_GRANT, caps[c].last_sent, caps[c].pending);
      }
    }
  }

  /* ---------------- lock state machine ---------------- */

  /* Locker::eval_gather(lock, first, need_issue) */
  fun EvalGather(first: bool, deferred: bool) {
    var saved: bool;
    var next: tLockState;
    var iss: tCapsSplit;
    var row: tLockRow;
    var finish: bool;
    assert !LockIsStable(lockState), "eval_gather on a stable lock";
    saved = needIssue;
    needIssue = false;
    next = LockNext(lockState);
    row = LockRow(next);
    iss = GetCapsIssued();
    if (first && (!CapsSubset(iss.other, GcapsNext(CAP_ANY, next)) ||
                  !CapsSubset(iss.loner, GcapsNext(CAP_LONER, next)) ||
                  !CapsSubset(iss.xlocker, GcapsNext(CAP_XLOCKER, next)))) {
      needIssue = true;
    }
    finish = (LockWhoAllowsAuth(row.rd) || nrdlock == 0) &&
             (LockWhoAllowsAuth(row.wr) || nwrlock == 0) &&
             (LockWhoAllowsAuth(row.x) || nxlock == 0) &&
             CapsSubset(iss.other, GcapsNext(CAP_ANY, next)) &&
             CapsSubset(iss.loner, GcapsNext(CAP_LONER, next)) &&
             CapsSubset(iss.xlocker, GcapsNext(CAP_XLOCKER, next));
    if (finish) {
      SetLock(next);
      // drop loner before doing waiters
      if (wantLoner != loner) {
        if (TryDropLoner()) { needIssue = true; }
      }
      RunWaiters(1 + 2 + 4 + 8);
      needIssue = true;
      if (LockIsStable(lockState)) {
        TryEvalLock(true);
      }
    }
    ResolveIssue(deferred, saved);
  }

  /* Locker::try_eval(lock, need_issue) -> Locker::eval(lock) -> file_eval */
  fun TryEvalLock(deferred: bool) {
    if (LockIsStable(lockState)) {
      FileEval(deferred);
    }
  }

  /* Locker::eval(in, CEPH_CAP_LOCKS); returns whether caps were issued */
  fun Eval() : bool {
    var saved: bool;
    var did: bool;
    var again: bool;
    var ok: bool;
    saved = needIssue;
    needIssue = false;
    if (ChooseIdealLoner()) {
      needIssue = true;
    }
    again = true;
    while (again) {
      again = false;
      // eval_any(&in->filelock)
      if (!LockIsStable(lockState)) {
        EvalGather(false, true);
      } else {
        FileEval(true);
      }
      // drop loner?
      if (wantLoner != loner) {
        if (TryDropLoner()) {
          needIssue = true;
          if (wantLoner >= 0) {
            ok = TrySetLoner();
            assert ok, "eval: try_set_loner failed after drop";
            again = true;
          }
        }
      }
    }
    did = needIssue;
    if (did) { IssueCaps(-1); }
    needIssue = saved;
    return did;
  }

  /* Locker::eval_cap_gather(in) */
  fun EvalCapGather() {
    var saved: bool;
    saved = needIssue;
    needIssue = false;
    if (!LockIsStable(lockState)) {
      EvalGather(false, true);
    }
    if (needIssue) { IssueCaps(-1); }
    needIssue = saved;
  }

  /* Locker::file_eval(lock, need_issue) */
  fun FileEval(deferred: bool) {
    var w: tCapsSplit;
    var iss: tCapsSplit;
    var target: int;
    assert LockIsStable(lockState), "file_eval on an unstable lock";
    w = GetCapsWanted();
    target = TargetLoner();
    if (lockState == LOCK_EXCL) {
      iss = GetCapsIssued();
      if (!CapsIntersects(CapsUnion(w.loner, iss.loner), AnyFileWr()) ||
          CapsIntersects(w.other, Caps3(Fx, Fw, Fr))) {
        // we should lose it: any writer means MIX, RD doesn't matter
        if ((Fw in w.other) || (Fw in w.loner) || HasWaiter(2)) {
          ScatterMix(deferred);
        } else if (nwrlock == 0) {
          SimpleSync(deferred);
        }
        // else: waiting for wrlock to drain
      }
    } else if (nrdlock == 0 && target >= 0 && CapsIntersects(w.all, AnyFileWr())) {
      // * -> excl
      FileExcl(deferred);
    } else if (lockState != LOCK_MIX && nrdlock == 0 && target < 0 && (Fw in w.all)) {
      // * -> mixed
      ScatterMix(deferred);
    } else if (lockState != LOCK_SYNC && nwrlock == 0 && !HasWaiter(2) && !(Fw in w.all)) {
      // * -> sync
      SimpleSync(deferred);
    }
  }

  /* Locker::simple_sync(lock, need_issue) */
  fun SimpleSync(deferred: bool) : bool {
    var gather: int;
    assert LockIsStable(lockState), "simple_sync on an unstable lock";
    if (lockState == LOCK_MIX) { SetLock(LOCK_MIX_SYNC); }
    else if (lockState == LOCK_LOCK) { SetLock(LOCK_LOCK_SYNC); }
    else if (lockState == LOCK_XSYN) { SetLock(LOCK_XSYN_SYNC); }
    else if (lockState == LOCK_EXCL) { SetLock(LOCK_EXCL_SYNC); }
    else { assert false, "simple_sync from an unexpected state"; }
    gather = 0;
    if (nwrlock > 0) { gather = gather + 1; }
    if (IssuedCapsNeedGather()) {
      MaybeIssue(deferred);
      gather = gather + 1;
    }
    if (gather > 0) { return false; }
    SetLock(LOCK_SYNC);
    RunWaiters(1 + 4);
    MaybeIssue(deferred);
    return true;
  }

  /* Locker::simple_lock(lock, need_issue) */
  fun SimpleLock(deferred: bool) {
    var gather: int;
    assert LockIsStable(lockState), "simple_lock on an unstable lock";
    assert lockState != LOCK_LOCK, "simple_lock while already LOCK";
    if (lockState == LOCK_SYNC) { SetLock(LOCK_SYNC_LOCK); }
    else if (lockState == LOCK_XSYN) { SetLock(LOCK_XSYN_LOCK); }
    else if (lockState == LOCK_EXCL) { SetLock(LOCK_EXCL_LOCK); }
    else if (lockState == LOCK_MIX) { SetLock(LOCK_MIX_LOCK2); }
    else { assert false, "simple_lock from an unexpected state"; }
    gather = 0;
    if (nrdlock > 0) { gather = gather + 1; }
    if (IssuedCapsNeedGather()) {
      MaybeIssue(deferred);
      gather = gather + 1;
    }
    if (gather == 0) {
      SetLock(LOCK_LOCK);
      RunWaiters(8 + 2 + 4);
    }
  }

  /* Locker::file_excl(lock, need_issue) */
  fun FileExcl(deferred: bool) {
    var gather: int;
    assert LockIsStable(lockState), "file_excl on an unstable lock";
    assert loner >= 0 || lockState == LOCK_XSYN, "file_excl without a loner";
    if (lockState == LOCK_SYNC) { SetLock(LOCK_SYNC_EXCL); }
    else if (lockState == LOCK_MIX) { SetLock(LOCK_MIX_EXCL); }
    else if (lockState == LOCK_LOCK) { SetLock(LOCK_LOCK_EXCL); }
    else if (lockState == LOCK_XSYN) { SetLock(LOCK_XSYN_EXCL); }
    else { assert false, "file_excl from an unexpected state"; }
    gather = 0;
    if (nrdlock > 0) { gather = gather + 1; }
    if (nwrlock > 0) { gather = gather + 1; }
    if (IssuedCapsNeedGather()) {
      MaybeIssue(deferred);
      gather = gather + 1;
    }
    if (gather == 0) {
      SetLock(LOCK_EXCL);
      MaybeIssue(deferred);
    }
  }

  /* Locker::scatter_mix(lock, need_issue) */
  fun ScatterMix(deferred: bool) {
    var gather: int;
    assert LockIsStable(lockState), "scatter_mix on an unstable lock";
    if (lockState == LOCK_LOCK) {
      if (gatherLockToMix) {
        SetLock(LOCK_LOCK_MIX);
        if (IssuedCapsNeedGather()) {
          MaybeIssue(deferred);
          return;
        }
      }
      SetLock(LOCK_MIX);
      MaybeIssue(deferred);
      return;
    }
    if (lockState == LOCK_SYNC) { SetLock(LOCK_SYNC_MIX); }
    else if (lockState == LOCK_EXCL) { SetLock(LOCK_EXCL_MIX); }
    else if (lockState == LOCK_XSYN) { SetLock(LOCK_XSYN_MIX); }
    else { assert false, "scatter_mix from an unexpected state"; }
    gather = 0;
    if (nrdlock > 0) { gather = gather + 1; }
    if (IssuedCapsNeedGather()) {
      MaybeIssue(deferred);
      gather = gather + 1;
    }
    if (gather == 0) {
      SetLock(LOCK_MIX);
      MaybeIssue(deferred);
    }
  }

  /* Locker::file_xsyn(lock, need_issue): let the loner keep Fcb while the MDS reads */
  fun FileXsyn(deferred: bool) {
    var gather: int;
    assert loner >= 0, "file_xsyn without a loner";
    assert lockState == LOCK_EXCL, "file_xsyn from an unexpected state";
    SetLock(LOCK_EXCL_XSYN);
    gather = 0;
    if (nwrlock > 0) { gather = gather + 1; }
    if (IssuedCapsNeedGather()) {
      MaybeIssue(deferred);
      gather = gather + 1;
    }
    if (gather == 0) {
      SetLock(LOCK_XSYN);
      RunWaiters(1 + 4);
      MaybeIssue(deferred);
    }
  }

  /* Locker::simple_xlock(lock) */
  fun SimpleXlock() {
    var gather: int;
    assert lockState != LOCK_XLOCK, "simple_xlock while XLOCK";
    if (lockState == LOCK_LOCK || lockState == LOCK_XLOCKDONE) { SetLock(LOCK_LOCK_XLOCK); }
    else { assert false, "simple_xlock from an unexpected state"; }
    gather = 0;
    if (nrdlock > 0) { gather = gather + 1; }
    if (nwrlock > 0) { gather = gather + 1; }
    if (IssuedCapsNeedGather()) {
      IssueCaps(-1);
      gather = gather + 1;
    }
    if (gather == 0) {
      SetLock(LOCK_PREXLOCK);
    }
  }

  /* ---------------- rdlock / xlock for requests ---------------- */

  /* Locker::_rdlock_kick(lock, as_anon) */
  fun RdlockKick(asAnon: bool) : bool {
    if (!LockIsStable(lockState)) { return false; }
    if (lockState == LOCK_EXCL && TargetLoner() >= 0 && !asAnon) {
      FileXsyn(false);
    } else {
      SimpleSync(false);
    }
    return true;
  }

  /* Locker::rdlock_start(lock, mdr, as_anon=false): true when the rdlock is held */
  fun RdlockStart(rid: int) : bool {
    var again: bool;
    var c: int;
    c = reqs[rid].client;
    again = true;
    while (again) {
      if (CanRdlock(c)) {
        nrdlock = nrdlock + 1;
        return true;
      }
      again = RdlockKick(false);
    }
    // wait: REQRDLOCK is ignored if lock is unstable, so we need to retry
    if (LockIsStable(lockState)) {
      AddWaiter(rid, 1);
    } else {
      AddWaiter(rid, 4);
    }
    return false;
  }

  /* Locker::rdlock_finish() */
  fun RdlockFinish(deferred: bool) {
    nrdlock = nrdlock - 1;
    if (nrdlock == 0) {
      if (!LockIsStable(lockState)) {
        EvalGather(false, deferred);
      } else {
        TryEvalLock(deferred);
      }
    }
  }

  /* Locker::xlock_start(lock, mdr): true when the xlock is held */
  fun XlockStart(rid: int) : bool {
    var again: bool;
    var c: int;
    c = reqs[rid].client;
    again = true;
    while (again) {
      if (reqs[rid].locking && CanXlock(c) &&
          !(lockState == LOCK_LOCK_XLOCK && IssuedCapsNeedGather())) {
        SetLock(LOCK_XLOCK);
        nxlock = nxlock + 1;     // get_xlock
        xlockBy = c;
        reqs[rid].locking = false; // finish_locking
        return true;
      }
      if (!LockIsStable(lockState) &&
          (lockState != LOCK_XLOCKDONE || xlockBy != c || HasWaiter(4))) {
        again = false;
      } else if (lockState == LOCK_XLOCKDONE) {
        // Avoid unstable XLOCKDONE state reset, see tracker 49132
        again = false;
      } else if (lockState == LOCK_LOCK || lockState == LOCK_XLOCKDONE) {
        reqs[rid].locking = true;  // start_locking
        SimpleXlock();
      } else {
        SimpleLock(false);
      }
    }
    AddWaiter(rid, 2 + 4);
    return false;
  }

  /* Locker::_finish_xlock(lock, xlocker, need_issue) */
  fun FinishXlock(xlocker: int, deferred: bool) {
    var target: int;
    if (nrdlock == 0 && nwrlock == 0) {
      target = TargetLoner();
      if (target >= 0 && (xlocker < 0 || xlocker == target)) {
        SetLock(LOCK_EXCL);
        RunWaiters(4 + 2 + 1);
        MaybeIssue(deferred);
        if (LockIsStable(lockState)) {
          TryEvalLock(deferred);
        }
        return;
      }
    }
    // the xlocker may have CEPH_CAP_GSHARED, need to revoke it if next state is LOCK_LOCK
    EvalGather(true, deferred);
  }

  /* Locker::xlock_finish() on the auth MDS */
  fun XlockFinish() {
    var xlocker: int;
    var saved: bool;
    xlocker = xlockBy;
    nxlock = nxlock - 1;      // put_xlock
    if (nxlock == 0) {
      xlockBy = -1;
    }
    saved = needIssue;
    needIssue = false;
    if (nxlock == 0 && lockState != LOCK_LOCK_XLOCK) {
      FinishXlock(xlocker, true);
    }
    ResolveIssue(false, saved);
  }

  /* Locker::cancel_locking(): the request that drove a pending xlock is gone */
  fun CancelLocking() {
    var saved: bool;
    saved = needIssue;
    needIssue = false;
    if (lockState == LOCK_PREXLOCK) {
      FinishXlock(-1, true);
    } else if (lockState == LOCK_LOCK_XLOCK) {
      SetLock(LOCK_PREXLOCK);
      FinishXlock(-1, true);
    }
    ResolveIssue(false, saved);
  }

  /* Locker::set_xlocks_done() at the early reply */
  fun SetXlocksDone() {
    assert lockState == LOCK_XLOCK, "set_xlock_done outside XLOCK";
    SetLock(LOCK_XLOCKDONE);
  }

  /* ---------------- client_ranges / max_size ---------------- */

  /* which clients should have a writeable range: (issued | wanted) & CEPH_CAP_ANY_FILE_WR */
  fun WantedRanges() : set[int] {
    var r: set[int];
    var c: int;
    foreach (c in keys(caps)) {
      if (CapsIntersects(CapsUnion(caps[c].issued, caps[c].wanted), AnyFileWr())) {
        r += (c);
      }
    }
    return r;
  }

  /* Locker::check_client_ranges(): do the ranges need an update? */
  fun CheckClientRanges() : bool {
    return WantedRanges() != clientRanges;
  }

  /* Locker::calc_new_client_ranges() */
  fun CalcNewClientRanges() {
    var c: int;
    clientRanges = WantedRanges();
    foreach (c in keys(caps)) {
      caps[c].client_writeable = c in clientRanges;
    }
  }

  /* submit a journal entry; a cap update holds a filelock wrlock until it is durable */
  fun Journal(client: int, tid: int, ack: bool, dirty: tCaps, needIss: bool, shareMax: bool,
              wrlock: bool, request: int) {
    if (wrlock) { nwrlock = nwrlock + 1; }
    send this, eJournalDone, (client = client, tid = tid, ack = ack, dirty = dirty,
                              need_issue = needIss, share_max = shareMax, wrlocked = wrlock,
                              request = request);
  }

  /* Locker::check_inode_max_size(in, force_wrlock) */
  fun CheckInodeMaxSize(force: bool) : bool {
    var w: tCapsSplit;
    if (!CheckClientRanges()) {
      return false;
    }
    if (!force && !CanWrlock(loner)) {
      if (LockIsStable(lockState)) {
        w = GetCapsWanted();
        if (TargetLoner() >= 0 && CapsIntersects(w.all, AnyFileWr())) {
          FileExcl(false);
        } else {
          SimpleLock(false);
        }
      }
      if (!CanWrlock(loner)) {
        waitStableCheckMax = true;
        return false;
      }
    }
    CalcNewClientRanges();
    Journal(-1, 0, false, CapsNone(), false, true, true, 0);
    return true;
  }

  /* ---------------- requests ---------------- */

  /* CInode::encode_inodestat(): the cap issued with a request reply */
  fun EncodeInodestat(c: int) : tCapReply {
    var cap: tMdsCap;
    var allowed: tCaps;
    var noCaps: bool;
    var issue: bool;
    noCaps = sessStale[c];   // a stale session gets no caps with its reply
    issue = false;
    if (!noCaps && !(c in caps)) {
      // add a new cap
      cap = NewCap();
      caps[c] = cap;
      ChooseIdealLoner();
    }
    if (!noCaps && (c in caps)) {
      allowed = GetCapsAllowedForClient(c);
      CapIssueNoRevoke(c, allowed);   // (wanted | likes) & allowed == allowed
      issue = true;
    } else if ((c in caps) && caps[c].is_new) {
      // always issue new caps to client, otherwise the caps get lost
      assert CapIsStale(c), "encode_inodestat: new cap without caps on a live session";
      assert CapsEmpty(caps[c].pending), "encode_inodestat: new cap already has pending caps";
      CapIssueNoRevoke(c, CapsNone());   // CEPH_CAP_PIN
      issue = true;
    }
    if (!issue) {
      return (has_cap = false, cap_id = 0, caps = CapsNone(), wanted = CapsNone(), cap_seq = 0, max_size = 0);
    }
    caps[c].last_issue = caps[c].last_sent;
    return (has_cap = true, cap_id = caps[c].cap_id, caps = caps[c].pending, wanted = caps[c].wanted,
            cap_seq = caps[c].last_sent, max_size = MaxSizeOf(c));
  }

  /* Locker::process_request_cap_release() */
  fun ProcessRequestCapRelease(rid: int, rel: tReqRelease) {
    var rcaps: tCaps;
    var c: int;
    c = reqs[rid].client;
    if (!(c in caps)) { return; }
    if (rel.cap_id != caps[c].cap_id) { return; }
    rcaps = CapsInter(rel.caps, caps[c].issued);   // confirming not issued caps
    CapConfirmReceipt(c, rel.cap_seq, rcaps);
    AdjustCapWanted(c, rel.wanted, rel.issue_seq);
    caps[c].suppress = caps[c].suppress + 1;
    Eval();
    caps[c].suppress = caps[c].suppress - 1;
    // take note; we may need to reissue on this cap later
    reqs[rid].cap_rel_seq = caps[c].last_sent;
  }

  /* Locker::kick_cap_releases() -> kick_issue_caps() */
  fun KickCapReleases(rid: int) {
    var c: int;
    c = reqs[rid].client;
    if (reqs[rid].cap_rel_seq >= 0 && (c in caps) && caps[c].last_sent == reqs[rid].cap_rel_seq) {
      IssueCaps(c);
    }
  }

  /* Server::handle_client_request() for GETATTR and SETATTR */
  fun HandleRequest(kind: tReqKind, m: tMdsReq) {
    var r: tReqState;
    var rid: int;
    if (SessionClosed(m.client)) { return; }   // session closed|closing|killing, dropping
    EnsureSession(m.client, m.from);
    rid = nextReqId;
    nextReqId = nextReqId + 1;
    r.kind = kind;
    r.client = m.client;
    r.from = m.from;
    r.cap_rel_seq = -1;
    reqs[rid] = r;
    if (m.has_release) {
      ProcessRequestCapRelease(rid, m.release);
    }
    DispatchRequest(rid);
  }

  /* Server::dispatch_client_request(), also the C_MDS_RetryRequest path */
  fun DispatchRequest(rid: int) {
    if (reqs[rid].kind == REQ_GETATTR) {
      DispatchGetattr(rid);
    } else {
      DispatchSetattr(rid);
    }
  }

  /* Server::handle_client_getattr() with mask CEPH_CAP_FILE_SHARED */
  fun DispatchGetattr(rid: int) {
    var issued: tCaps;
    var c: int;
    c = reqs[rid].client;
    if (c in caps) { issued = caps[c].issued; }
    // if client currently holds the EXCL cap on a field, do not rdlock it
    if (!(Fx in issued) && !reqs[rid].has_rdlock) {
      // Don't wait on unstable filelock if client is allowed to read file size.
      if (LockIsStable(lockState) || nwrlock > 0 || !CanRead(c)) {
        if (!RdlockStart(rid)) {
          return;   // waiting
        }
        reqs[rid].has_rdlock = true;
      }
    }
    RespondToRequest(rid);
  }

  /* Server::handle_client_setattr() for mtime: xlock the filelock and journal */
  fun DispatchSetattr(rid: int) {
    var reply: tCapReply;
    if (!reqs[rid].has_xlock) {
      if (!XlockStart(rid)) {
        return;   // waiting
      }
      reqs[rid].has_xlock = true;
    }
    // journal_and_reply -> early_reply: mark xlocks "done" and send the trace
    SetXlocksDone();
    reply = EncodeInodestat(reqs[rid].client);
    reqs[rid].early_replied = true;
    send reqs[rid].from, eSetattrReply, reply;
    Journal(reqs[rid].client, 0, false, CapsNone(), false, false, false, rid);
  }

  /* Server::respond_to_request -> reply_client_request -> MDCache::request_finish */
  fun RespondToRequest(rid: int) {
    var reply: tCapReply;
    var saved: bool;
    if (!reqs[rid].early_replied) {
      reqs[rid].cap_rel_seq = -1;   // the trace re-issues the cap
    }
    // drop non-rdlocks before replying
    if (reqs[rid].has_xlock) {
      reqs[rid].has_xlock = false;
      XlockFinish();
    }
    if (!reqs[rid].early_replied) {
      reply = EncodeInodestat(reqs[rid].client);
      if (reqs[rid].kind == REQ_GETATTR) {
        send reqs[rid].from, eGetattrReply, reply;
      } else {
        send reqs[rid].from, eSetattrReply, reply;
      }
    }
    // request_cleanup: drop the remaining (rd)locks, then kick cap releases
    if (reqs[rid].has_rdlock) {
      reqs[rid].has_rdlock = false;
      saved = needIssue;
      needIssue = false;
      RdlockFinish(true);
      ResolveIssue(false, saved);
    }
    KickCapReleases(rid);
    reqs -= (rid);
  }

  /* ---------------- message handlers ---------------- */

  /* Server::handle_client_open -> Locker::issue_new_caps -> CInode::encode_inodestat */
  fun HandleOpen(r: tOpenReq) {
    var c: int;
    var want: tCaps;
    var cap: tMdsCap;
    var reply: tCapReply;
    c = r.client;
    if (SessionClosed(c)) { return; }
    EnsureSession(c, r.from);
    want = CapsForMode(r.mode);
    if (!(c in caps)) {
      cap = NewCap();
      cap.wanted = want;
      cap.is_new = true;
      caps[c] = cap;
    } else if (!CapsSubset(want, caps[c].wanted)) {
      caps[c].wanted = CapsUnion(caps[c].wanted, want);
    }
    caps[c].suppress = caps[c].suppress + 1;   // bundle with the request reply
    Eval();
    caps[c].suppress = caps[c].suppress - 1;
    // increase max_size?
    if (r.mode != MODE_RD) {
      CheckInodeMaxSize(false);
    }
    reply = EncodeInodestat(c);
    send r.from, eOpenReply, reply;
  }

  /* Locker::adjust_cap_wanted(cap, wanted, issue_seq) */
  fun AdjustCapWanted(c: int, wanted: tCaps, issueSeq: int) {
    if (issueSeq == caps[c].last_issue) {
      caps[c].wanted = wanted;
    } else if (!CapsSubset(wanted, caps[c].wanted)) {
      // added caps even though we had seq mismatch
      caps[c].wanted = CapsUnion(caps[c].wanted, wanted);
    }
  }

  /*
   * Locker::_do_cap_update(): returns true when an update was journaled.
   * The ack for a flush rides on the journal completion in that case.
   */
  fun DoCapUpdate(c: int, dirty: tCaps, reqMax: int, tid: int) : bool {
    var oldMax: int;
    var newMax: int;
    var changeMax: bool;
    var saved: bool;
    var ok: bool;
    oldMax = MaxSizeOf(c);
    newMax = oldMax;
    changeMax = false;
    if (CapsIntersects(CapsUnion(caps[c].issued, caps[c].wanted), AnyFileWr())) {
      // client has write caps: calc_new_max_size() always yields a non-zero range
      if (reqMax > newMax) {
        changeMax = true;
        newMax = 1;
      } else {
        newMax = 1;
        if (newMax > oldMax) {
          changeMax = true;
        } else {
          newMax = oldMax;
        }
      }
    } else if (oldMax > 0) {
      changeMax = true;
      newMax = 0;
    }
    if (changeMax && !CanWrlock(c) && !CanForceWrlock(c)) {
      // i want to change file_max, but lock won't allow it (yet)
      if (LockIsStable(lockState)) {
        saved = needIssue;
        needIssue = false;
        caps[c].suppress = caps[c].suppress + 1;
        ok = false;
        if (loner >= 0) {
          ok = true;
        } else if (wantLoner >= 0) {
          ok = TrySetLoner();
        }
        if (ok) {
          if (lockState != LOCK_EXCL) { FileExcl(true); }
        } else {
          SimpleLock(true);
        }
        if (needIssue) { IssueCaps(-1); }
        needIssue = saved;
        caps[c].suppress = caps[c].suppress - 1;
      }
      if (!CanWrlock(c) && !CanForceWrlock(c)) {
        waitStableCheckMax = true;
        changeMax = false;
      }
    }
    if (CapsEmpty(dirty) && !changeMax) {
      return false;
    }
    // do the update
    if (changeMax) {
      if (newMax > 0) {
        clientRanges += (c);
        caps[c].client_writeable = true;
      } else {
        clientRanges -= (c);
        caps[c].client_writeable = false;
      }
    }
    // wrlock_force(&in->filelock, mut) for the duration of the journal
    Journal(c, tid, !CapsEmpty(dirty), dirty, true, changeMax,
            changeMax || CapsIntersects(dirty, Caps2(Fx, Fw)), 0);
    return true;
  }

  /* Locker::handle_client_caps() for CEPH_CAP_OP_UPDATE / FLUSH */
  fun HandleCapUpdate(m: tCapUpdate) {
    var c: int;
    var rcaps: tCaps;
    var didIssue: bool;
    c = m.client;
    if (SessionClosed(c)) { return; }
    if (!(c in caps)) {
      return;   // no cap for client: dropped
    }
    if (m.cap_id != caps[c].cap_id) {
      return;   // ignoring client capid != my capid
    }
    rcaps = CapsInter(m.caps, caps[c].issued);   // confirming not issued caps
    CapConfirmReceipt(c, m.cap_seq, rcaps);
    if (m.wanted != caps[c].wanted) {
      AdjustCapWanted(c, m.wanted, m.issue_seq);
    }
    if (DoCapUpdate(c, m.dirty, m.max_size, m.tid)) {
      // updated: the flush ack is sent when the journal entry is durable
      Eval();
    } else {
      // no update, ack now
      if (!CapsEmpty(m.dirty)) {
        send clients[c], eFlushAck, (tid = m.tid, dirty = m.dirty);
      }
      didIssue = Eval();
      if (!didIssue && !CapsEmpty(CapsMinus(caps[c].wanted, caps[c].pending))) {
        IssueCaps(c);
      }
    }
  }

  /*
   * Locker::remove_client_cap(in, cap, kill). With kill the real MDS marks the
   * inode NEEDSRECOVER and file recovery later clears the client range; the
   * model clears the range directly.
   */
  fun RemoveClientCap(c: int) {
    var notable: bool;
    notable = CapIsNotable(c);
    caps -= (c);
    AnnounceIssued(c);
    if (c == loner) {
      loner = -1;
    }
    if (!notable) {
      return;
    }
    // make sure we clear out the client byte range
    if (c in clientRanges) {
      CheckInodeMaxSize(false);
    }
    Eval();   // try_eval(in, CEPH_CAP_LOCKS)
  }

  /*
   * MDSRank::evict_client(blocklist=true): blocklist the client at the OSDs,
   * wait for the map, raise the osd epoch barrier, then kill the session
   * (Server::kill_session -> _session_logged: caps and requests are killed).
   */
  fun EvictClient(c: int) {
    var rid: int;
    var rs: seq[int];
    var saved: bool;
    var r: tReqState;
    if (SessionClosed(c)) { return; }
    send store, eBlocklist, (from = this, client = clients[c]);
    receive {
      case eBlocklistAck: { osdEpoch = osdEpoch + 1; }
    }
    sessClosed[c] = true;
    // kill any lingering capabilities, leases, requests
    if (c in caps) {
      RemoveClientCap(c);
    }
    foreach (rid in keys(reqs)) {
      if (reqs[rid].client == c) { rs += (sizeof(rs), rid); }
    }
    foreach (rid in rs) {
      if (!(rid in reqs)) { continue; }
      // request_kill: the request is dead before its locks are dropped, so a
      // lock waiter cannot re-dispatch it
      r = reqs[rid];
      reqs -= (rid);
      if (r.locking) {
        CancelLocking();
      }
      if (r.has_xlock) {
        XlockFinish();
      }
      if (r.has_rdlock) {
        saved = needIssue;
        needIssue = false;
        RdlockFinish(true);
        ResolveIssue(false, saved);
      }
    }
    send clients[c], eSessionKilled;
  }

  /* Locker::_do_cap_release() */
  fun HandleCapRelease(m: tCapRelease) {
    var c: int;
    c = m.client;
    if (SessionClosed(c)) { return; }
    if (!(c in caps)) {
      return;
    }
    if (m.cap_id != caps[c].cap_id) {
      return;   // capid != my capid, ignore
    }
    if (m.issue_seq < caps[c].last_issue) {
      // the client released before seeing a newer request-reply issue
      CapCleanRevokeFrom(c, m.issue_seq);
      EvalCapGather();
      return;
    }
    RemoveClientCap(c);
  }

  /* Locker::file_update_finish() or C_MDS_inode_update_finish */
  fun HandleJournalDone(j: tJournalDone) {
    var saved: bool;
    var ni: bool;
    if (j.request > 0) {
      if (j.request in reqs) {
        RespondToRequest(j.request);
      }
      return;
    }
    if (j.ack && (j.client in clients)) {
      send clients[j.client], eFlushAck, (tid = j.tid, dirty = j.dirty);
    }
    // drop_locks -> wrlock_finish
    saved = needIssue;
    needIssue = false;
    if (j.wrlocked) {
      nwrlock = nwrlock - 1;
      if (nwrlock == 0) {
        if (!LockIsStable(lockState)) {
          EvalGather(false, true);
        } else {
          TryEvalLock(true);
        }
      }
    }
    ni = needIssue;
    needIssue = saved;
    if (j.need_issue && !ni && (j.client in caps) &&
        !CapsEmpty(CapsMinus(caps[j.client].wanted, caps[j.client].pending))) {
      IssueCaps(j.client);
    }
    if (j.share_max && CapsIntersects(Gcaps(CAP_LONER), Caps2(Fw, Fb))) {
      ShareInodeMaxSize(-1);
    }
    if (ni) {
      IssueCaps(-1);
    }
  }

  /* ---------------- sessions ---------------- */

  /* Locker::revoke_stale_caps(session): true unless the client must be evicted */
  fun RevokeStaleCaps(c: int) : bool {
    sessGen[c] = sessGen[c] + 1;   // invalidate all caps
    if (!(c in caps)) { return true; }
    if (!CapIsNotable(c)) { return true; }
    if (CapsEmpty(CapRevoking(c))) { return true; }
    if (CapsIntersects(CapRevoking(c), AnyFileWr())) {
      return false;   // the client would be evicted
    }
    CapRevoke(c);
    if (!LockIsStable(lockState)) {
      EvalGather(false, false);
    }
    Eval();   // try_eval(in, CEPH_CAP_LOCKS)
    return true;
  }

  /*
   * Server::find_idle_sessions(): the session timed out. A session that is
   * already stale has reached the autoclose timeout and is evicted; so is a
   * session whose caps cannot be revoked by force.
   */
  fun HandleSessionTimeout(c: int) {
    if (!(c in sessStale) || SessionClosed(c)) { return; }
    if (sessStale[c]) {
      EvictClient(c);
      return;
    }
    sessStale[c] = true;
    if (RevokeStaleCaps(c)) {
      send clients[c], eSessionStale;
    } else {
      EvictClient(c);
    }
  }

  /* Locker::revoke_stale_cap(in, client), queued by issue_caps */
  fun HandleRevokeStaleCap(c: int) {
    if (!(c in caps)) { return; }
    if (CapsIntersects(CapRevoking(c), AnyFileWr())) {
      EvictClient(c);
      return;
    }
    CapRevoke(c);
    if (!LockIsStable(lockState)) {
      EvalGather(false, false);
    }
    Eval();
  }

  /* Locker::resume_stale_caps(session) */
  fun ResumeStaleCaps(c: int) {
    if (!(c in caps)) { return; }
    if (!Eval()) {
      IssueCaps(c);
    }
  }

  /* Server::handle_client_session(CEPH_SESSION_REQUEST_RENEWCAPS) */
  fun HandleRenewCaps(m: tRenewCaps) {
    if (!(m.client in sessStale) || SessionClosed(m.client)) { return; }
    if (sessStale[m.client]) {
      sessStale[m.client] = false;
      ResumeStaleCaps(m.client);
    }
    send clients[m.client], eRenewCapsAck, m.renew_seq;
  }
}

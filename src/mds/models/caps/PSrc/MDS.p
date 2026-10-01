/*
 * The auth MDS for one regular file.
 *
 * What is modelled (function names refer to src/mds/Locker.cc, Capability.h
 * and CInode.cc):
 *   - Capability::issue / issue_norevoke / confirm_receipt / clean_revoke_from
 *   - Locker::issue_caps and get_allowed_caps
 *   - Locker::handle_client_caps, adjust_cap_wanted, _do_cap_update
 *   - Locker::_do_cap_release, remove_client_cap
 *   - Locker::eval, eval_gather, file_eval, simple_sync, simple_lock,
 *     file_excl, scatter_mix
 *   - CInode loner selection (calc_ideal_loner, choose_ideal_loner,
 *     try_set_loner, try_drop_loner)
 *   - the client_ranges / max_size path: check_inode_max_size,
 *     share_inode_max_size, the journal wrlock held while an update is
 *     journaled, and the C_MDL_CheckMaxSize WAIT_STABLE waiter
 *   - Server::handle_client_open -> issue_new_caps -> encode_inodestat
 *
 * Not modelled: other locks (auth, link, xattr), xlocks and rdlocks taken by
 * MDS requests, replicas, snapshots, stale sessions, cap export/import,
 * recovery. See README.md for the list of follow-ups.
 *
 * max_size is reduced to 0 ("no writeable range") or 1 ("has a range"),
 * because the model never grows the file.
 */

type tRevoke = (before: tCaps, rseq: int, last_issue: int);

type tMdsCap = (cap_id: int, pending: tCaps, issued: tCaps, revokes: seq[tRevoke],
                last_sent: int, last_issue: int, wanted: tCaps,
                is_new: bool, suppress: int, client_writeable: bool);

/* C_Locker_FileUpdate_finish: the journal entry for a cap update is durable */
type tJournalDone = (client: int, tid: int, ack: bool, dirty: tCaps,
                     need_issue: bool, share_max: bool, wrlocked: bool);
event eJournalDone : tJournalDone;

type tCapsSplit = (loner: tCaps, other: tCaps, all: tCaps);

machine MDS {
  var clients: map[int, machine];
  var caps: map[int, tMdsCap];
  var lockState: tLockState;
  var nwrlock: int;             // filelock wrlocks held by journaling cap updates
  var loner: int;               // CInode::loner_cap, -1 when none
  var wantLoner: int;           // CInode::want_loner_cap
  var clientRanges: set[int];   // clients with a non-zero client_ranges entry
  var waitStableCheckMax: bool; // a C_MDL_CheckMaxSize waits for WAIT_STABLE
  var needIssue: bool;          // the `bool *need_issue` out-parameter
  var nextCapId: int;           // Capability ids are unique per incarnation

  start state Serving {
    entry {
      lockState = LOCK_SYNC;
      loner = -1;
      wantLoner = -1;
      nextCapId = 1;
    }
    on eOpenReq     do (r: tOpenReq)     { HandleOpen(r); }
    on eCapUpdate   do (m: tCapUpdate)   { HandleCapUpdate(m); }
    on eCapRelease  do (m: tCapRelease)  { HandleCapRelease(m); }
    on eJournalDone do (j: tJournalDone) { HandleJournalDone(j); }
  }

  fun SetLock(s: tLockState) {
    announce eLockTransition, (prev = lockState, next = s);
    lockState = s;
  }

  /* ---------------- Capability ---------------- */

  fun NewCap() : tMdsCap {
    var c: tMdsCap;
    return c;
  }

  fun AnnounceIssued(c: int) {
    if (c in caps) {
      announce eMdsIssued, (client = c, caps = caps[c].issued);
    } else {
      announce eMdsIssued, (client = c, caps = CapsNone());
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

  /* Capability::issue(c) */
  fun CapIssue(c: int, newc: tCaps) : int {
    var rv: tRevoke;
    var last: int;
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

  /* Capability::issue_norevoke(c) */
  fun CapIssueNoRevoke(c: int, newc: tCaps) : int {
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

  /* CInode::get_caps_issued() split into loner and other */
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
    }
    return r;
  }

  /* CInode::get_caps_wanted() */
  fun GetCapsWanted() : tCapsSplit {
    var r: tCapsSplit;
    var c: int;
    foreach (c in keys(caps)) {
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
      if (CapsIntersects(caps[c].wanted, Caps4(Fx, Fw, Fb, Fr))) {
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
    otherAllowed = LockGcapsAllowed(CAP_ANY, lockState);
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

  /* Locker::get_allowed_caps() and CInode::get_caps_allowed_for_client() */
  fun GetAllowedCaps(c: int) : tCaps {
    if (c == loner) {
      return LockGcapsAllowed(CAP_LONER, lockState);
    }
    return LockGcapsAllowed(CAP_ANY, lockState);
  }

  /* CInode::issued_caps_need_gather(lock) */
  fun IssuedCapsNeedGather() : bool {
    var iss: tCapsSplit;
    iss = GetCapsIssued();
    return !CapsSubset(iss.loner, LockGcapsAllowed(CAP_LONER, lockState)) ||
           !CapsSubset(iss.other, LockGcapsAllowed(CAP_ANY, lockState));
  }

  /* SimpleLock::can_wrlock(client) for the filelock on the auth MDS */
  fun CanWrlock(c: int) : bool {
    var w: tLockWho;
    w = LockRow(lockState).wr;
    return w == WHO_ANY || w == WHO_AUTH || (w == WHO_XCL && c >= 0 && loner == c);
  }

  fun CanForceWrlock(c: int) : bool {
    var w: tLockWho;
    w = LockRow(lockState).fwr;
    return w == WHO_ANY || w == WHO_AUTH || (w == WHO_XCL && c >= 0 && loner == c);
  }

  /*
   * The `bool *need_issue` convention of Locker: when the caller passed a
   * pointer (defer), record the request; otherwise issue right now.
   */
  fun MaybeIssue(deferred: bool) {
    if (deferred) {
      needIssue = true;
    } else {
      IssueCaps(-1);
    }
  }

  /* ---------------- issue_caps ---------------- */

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
        if (caps[c].is_new || caps[c].suppress > 0) { continue; }
      } else {
        assert !caps[c].is_new, "issue_caps: revoking from a new cap";
      }
      // are there caps that the client wants and can have, but aren't pending?
      // or do we need to revoke?
      if (!CapsSubset(pending, allowed) ||
          !CapsEmpty(CapsMinus(CapsInter(wanted, allowed), pending))) {
        nissued = nissued + 1;
        before = pending;
        // get_caps_liked() is every F bit for a regular file, so
        // (wanted | likes) & allowed == allowed
        if (!CapsSubset(pending, allowed)) {
          sq = CapIssue(c, CapsInter(allowed, pending));   // if revoking, don't issue anything new
        } else {
          sq = CapIssue(c, allowed);
        }
        after = caps[c].pending;
        if (CapsEmpty(CapsMinus(before, after))) {
          op = OP_GRANT;
        } else {
          op = OP_REVOKE;
        }
        send clients[c], eCapGrant, (op = op, cap_id = caps[c].cap_id, cap_seq = sq, caps = after,
                                     wanted = wanted, issue_seq = caps[c].last_issue,
                                     max_size = MaxSizeOf(c));
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
        send clients[c], eCapGrant, (op = OP_GRANT, cap_id = caps[c].cap_id, cap_seq = caps[c].last_sent,
                                     caps = caps[c].pending, wanted = caps[c].wanted,
                                     issue_seq = caps[c].last_issue, max_size = MaxSizeOf(c));
      }
    }
  }

  /* ---------------- lock state machine ---------------- */

  /* SimpleLock::finish_waiters(WAIT_STABLE): run the pending C_MDL_CheckMaxSize */
  fun FinishStableWaiters() {
    if (waitStableCheckMax) {
      waitStableCheckMax = false;
      CheckInodeMaxSize(false);
    }
  }

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
    if (first && (!CapsSubset(iss.other, LockGcapsAllowed(CAP_ANY, next)) ||
                  !CapsSubset(iss.loner, LockGcapsAllowed(CAP_LONER, next)))) {
      needIssue = true;
    }
    finish = (LockWhoAllowsAuth(row.wr) || nwrlock == 0) &&
             CapsSubset(iss.other, LockGcapsAllowed(CAP_ANY, next)) &&
             CapsSubset(iss.loner, LockGcapsAllowed(CAP_LONER, next));
    if (finish) {
      SetLock(next);
      // drop loner before doing waiters
      if (wantLoner != loner) {
        if (TryDropLoner()) { needIssue = true; }
      }
      FinishStableWaiters();
      needIssue = true;
      if (LockIsStable(lockState)) {
        TryEvalLock(true);
      }
    }
    if (deferred) {
      needIssue = needIssue || saved;
    } else {
      if (needIssue) { IssueCaps(-1); }
      needIssue = saved;
    }
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
        if ((Fw in w.other) || (Fw in w.loner)) {
          ScatterMix(deferred);
        } else if (nwrlock == 0) {
          SimpleSync(deferred);
        }
        // else: waiting for wrlock to drain
      }
    } else if (target >= 0 && CapsIntersects(w.all, AnyFileWr())) {
      // * -> excl
      FileExcl(deferred);
    } else if (lockState != LOCK_MIX && target < 0 && (Fw in w.all)) {
      // * -> mixed
      ScatterMix(deferred);
    } else if (lockState != LOCK_SYNC && nwrlock == 0 && !(Fw in w.all)) {
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
    FinishStableWaiters();
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
    if (IssuedCapsNeedGather()) {
      MaybeIssue(deferred);
      gather = gather + 1;
    }
    if (gather == 0) {
      SetLock(LOCK_LOCK);
      FinishStableWaiters();
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
      SetLock(LOCK_MIX);
      MaybeIssue(deferred);
      return;
    }
    if (lockState == LOCK_SYNC) { SetLock(LOCK_SYNC_MIX); }
    else if (lockState == LOCK_EXCL) { SetLock(LOCK_EXCL_MIX); }
    else if (lockState == LOCK_XSYN) { SetLock(LOCK_XSYN_MIX); }
    else { assert false, "scatter_mix from an unexpected state"; }
    gather = 0;
    if (IssuedCapsNeedGather()) {
      MaybeIssue(deferred);
      gather = gather + 1;
    }
    if (gather == 0) {
      SetLock(LOCK_MIX);
      MaybeIssue(deferred);
    }
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

  /* submit an EUpdate/EOpen that holds a filelock wrlock until it is durable */
  fun Journal(client: int, tid: int, ack: bool, dirty: tCaps, needIss: bool, shareMax: bool, wrlock: bool) {
    if (wrlock) { nwrlock = nwrlock + 1; }
    send this, eJournalDone, (client = client, tid = tid, ack = ack, dirty = dirty,
                              need_issue = needIss, share_max = shareMax, wrlocked = wrlock);
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
    Journal(-1, 0, false, CapsNone(), false, true, true);
    return true;
  }

  /* ---------------- message handlers ---------------- */

  /* Server::handle_client_open -> Locker::issue_new_caps -> CInode::encode_inodestat */
  fun HandleOpen(r: tOpenReq) {
    var c: int;
    var want: tCaps;
    var cap: tMdsCap;
    var allowed: tCaps;
    c = r.client;
    clients[c] = r.from;
    want = CapsForMode(r.mode);
    if (!(c in caps)) {
      cap = NewCap();
      cap.cap_id = nextCapId;
      nextCapId = nextCapId + 1;
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
    // reply: CInode::encode_inodestat issues (wanted | likes) & allowed without revoking
    allowed = GetAllowedCaps(c);
    CapIssueNoRevoke(c, allowed);
    caps[c].last_issue = caps[c].last_sent;
    send r.from, eOpenReply, (cap_id = caps[c].cap_id, caps = caps[c].pending, wanted = caps[c].wanted,
                              cap_seq = caps[c].last_sent, max_size = MaxSizeOf(c));
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
            changeMax || CapsIntersects(dirty, Caps2(Fx, Fw)));
    return true;
  }

  /* Locker::handle_client_caps() for CEPH_CAP_OP_UPDATE / FLUSH */
  fun HandleCapUpdate(m: tCapUpdate) {
    var c: int;
    var rcaps: tCaps;
    var revoked: tCaps;
    var didIssue: bool;
    c = m.client;
    if (!(c in caps)) {
      return;   // no cap for client: dropped
    }
    if (m.cap_id != caps[c].cap_id) {
      return;   // ignoring client capid != my capid
    }
    rcaps = CapsInter(m.caps, caps[c].issued);   // confirming not issued caps
    revoked = CapConfirmReceipt(c, m.cap_seq, rcaps);
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

  /* Locker::remove_client_cap() */
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

  /* Locker::_do_cap_release() */
  fun HandleCapRelease(m: tCapRelease) {
    var c: int;
    c = m.client;
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

  /* Locker::file_update_finish() */
  fun HandleJournalDone(j: tJournalDone) {
    var saved: bool;
    var ni: bool;
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
    if (j.share_max && CapsIntersects(LockGcapsAllowed(CAP_LONER, lockState), Caps2(Fw, Fb))) {
      ShareInodeMaxSize(-1);
    }
    if (ni) {
      IssueCaps(-1);
    }
  }
}

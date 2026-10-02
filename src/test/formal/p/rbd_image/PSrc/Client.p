/*
 * One librbd ImageCtx, open on one image, running a script of actions one
 * after another. It keeps:
 *
 * - its watch on the header (Watcher): a handle, re-registered after a
 *   watch error (RewatchRequest), after which the lock is re-acquired
 *   (ImageWatcher::handle_rewatch_complete -> reacquire_lock);
 * - the exclusive lock state machine (ManagedLock, ExclusiveLock) with
 *   its action queue (TRY_LOCK, ACQUIRE_LOCK, RELEASE_LOCK,
 *   REACQUIRE_LOCK; an action already queued is merged into):
 *   - acquire: get_lock_info, lock; on -EBUSY, BreakRequest: list the
 *     watchers (the holder is alive if a watcher has its address and its
 *     cookie as handle), re-read the lock, blocklist the holder
 *     (rbd_blocklist_on_break_lock), break_lock, lock again; a live
 *     holder is asked for the lock with a RequestLock notify, and the
 *     acquire waits for a ReleasedLock or AcquiredLock from a peer, or a
 *     timeout;
 *   - post-acquire: refresh, then AcquiredLock to every watcher, then
 *     writes no longer need the lock (unset_require_lock);
 *   - release: writes need the lock again and in-flight ones drain
 *     (PreReleaseRequest), the ops in flight finish, unlock, ReleasedLock;
 *   - reacquire: set_cookie to the new watch handle; if that fails, or
 *     there is no handle, release then acquire again;
 * - the write path (io::ImageDispatch, exclusive_lock::ImageDispatch):
 *   a write refreshes the header if a HeaderUpdate was seen, waits for
 *   the lock if writes need it, waits while writes are blocked
 *   (block_writes), and carries the client's snap context and cookie;
 * - the header it last read (RefreshRequest): snap context, snapshots,
 *   parent;
 * - operations (Operations::C_InvokeAsyncRequest): refresh; if the op
 *   needs the lock, try to acquire it; as owner run it locally, else send
 *   it to the owner (notify_async_request) and wait for its
 *   AsyncComplete, retrying on a timeout or when the owner changes; as
 *   owner, serve requests from peers (handle_operation_request), with
 *   the pending and completed request ids;
 * - the ops themselves, as the operation/ requests run them: snapshot
 *   create (block writes, allocate a snap id, snapshot_add, update the
 *   snap context, unblock, HeaderUpdate), snapshot remove
 *   (snapshot_trash_add, snapshot_get, release the snap id,
 *   snapshot_remove), protect, unprotect (UNPROTECTING, scan
 *   rbd_children, UNPROTECTED or back to PROTECTED), flatten (copy up
 *   each object missing from the child, detach the child from the
 *   parent, remove a trashed parent snapshot left with no children,
 *   detach the parent); and a clone (image::CloneRequest: create the
 *   child, set its parent, attach it, re-check the protection for v1)
 *   and a read through to the parent (io::util::read_parent).
 */

enum tLState { L_UNLOCKED, L_ACQUIRING, L_WAITING_FOR_REGISTER, L_WAITING_FOR_LOCK, L_POST_ACQUIRING,
               L_LOCKED, L_REACQUIRING, L_PRE_RELEASING, L_RELEASING }

// the lock's action queue
fun A_TRY(): int { return 1; }
fun A_ACQ(): int { return 2; }
fun A_REL(): int { return 3; }
fun A_REACQ(): int { return 4; }

// tags: which step of what an answer belongs to
fun T_WATCH(): int { return 1; }
fun T_REWATCH_UNWATCH(): int { return 2; }
fun T_REWATCH_WATCH(): int { return 3; }
fun T_REFRESH_OPEN(): int { return 4; }
fun T_REFRESH_ACT(): int { return 5; }
fun T_REFRESH_IA(): int { return 6; }
fun T_REFRESH_POST(): int { return 7; }
fun T_REFRESH_CLONE(): int { return 8; }
fun T_ACQ_GET_LOCKER(): int { return 10; }
fun T_ACQ_LOCK(): int { return 11; }
fun T_BRK_WATCHERS(): int { return 12; }
fun T_BRK_GET_LOCKER(): int { return 13; }
fun T_BRK_BLOCKLIST(): int { return 14; }
fun T_BRK_BREAK(): int { return 15; }
fun T_REACQ(): int { return 16; }
fun T_REL_UNLOCK(): int { return 17; }
fun T_WRITE(): int { return 20; }
fun T_SC_ALLOC(): int { return 30; }
fun T_SC_ADD(): int { return 31; }
fun T_SC_RELEASE(): int { return 32; }
fun T_SR_TRASH_ADD(): int { return 33; }
fun T_SR_GET(): int { return 34; }
fun T_SR_RELEASE(): int { return 35; }
fun T_SR_REMOVE(): int { return 36; }
fun T_SP_SET(): int { return 37; }
fun T_SU_START(): int { return 38; }
fun T_SU_SCAN(): int { return 39; }
fun T_SU_FINISH(): int { return 40; }
fun T_SU_ROLLBACK(): int { return 41; }
fun T_FL_READ_CHILD(): int { return 50; }
fun T_FL_READ_PARENT(): int { return 51; }
fun T_FL_COPYUP(): int { return 52; }
fun T_FL_DETACH_CHILD(): int { return 53; }
fun T_FL_SNAP_GET(): int { return 54; }
fun T_FL_TRASH_RELEASE(): int { return 55; }
fun T_FL_TRASH_REMOVE(): int { return 56; }
fun T_FL_DETACH_PARENT(): int { return 57; }
fun T_CL_CREATE(): int { return 60; }
fun T_CL_SET_PARENT(): int { return 61; }
fun T_CL_ATTACH(): int { return 62; }
fun T_CL_RB_DETACH(): int { return 63; }
fun T_CL_RB_REMOVE(): int { return 64; }
fun T_RD_CHILD(): int { return 70; }
fun T_RD_PARENT(): int { return 71; }
fun T_N_REQUEST_LOCK(): int { return 80; }
fun T_N_FIRE(): int { return 81; }
fun T_N_ASYNC_REQUEST(): int { return 82; }
fun T_N_HEADER_UPDATE(): int { return 83; }
fun T_RM_TRASH_RELEASE(): int { return 90; }
fun T_RM_TRASH_REMOVE(): int { return 91; }
fun T_RM_WATCHERS(): int { return 92; }
fun T_RM_DETACH(): int { return 93; }
fun T_RM_SNAP_GET(): int { return 94; }
fun T_RM_PTRASH_RELEASE(): int { return 95; }
fun T_RM_PTRASH_REMOVE(): int { return 96; }
fun T_RM_UNWATCH(): int { return 97; }
fun T_RM_REMOVE(): int { return 98; }

type tQueuedOp = (asyncId: int, kind: tReqKind, name: int, requester: int);

// how many steps (events handled) a client that may die picks its death
// from; a pick past its run means it lives
fun CRASH_STEPS(): int { return 40; }

machine Client {
  var cfg: tCfg;
  var id: int;
  var store: machine;
  var driver: machine;
  var image: int;
  var script: seq[tAction];
  var actIdx: int;
  var act: tAction;
  var actAsyncId: int;
  var actRunning: bool;
  // the watch
  var handle: int;
  var blocklisted: bool;
  var rewatching: bool;
  // the lock
  var lstate: tLState;
  var cookie: int;
  var newCookie: int;
  var locker: tLock;
  var lockQ: seq[int];
  var peerRet: tRc;
  var reqGen: int;          // the lock request's retry timer generation
  var removing: bool;       // rbd rm in progress: StandardPolicy, then close
  var rmWaitsLock: bool;
  var rmWaitsRelease: bool;
  var rmTrash: seq[int];    // the trashed snapshots a remove still has to drop
  var crashBudget: int;
  var drainer: machine;
  var draining: bool;
  var crashAt: int;
  var crashStep: int;
  var ownerId: int;         // ImageWatcher::m_owner_client_id
  var requireLock: bool;    // exclusive_lock::ImageDispatch: writes need the lock
  var opWaitsLock: bool;
  var actWaitsLock: bool;
  // writes
  var writesBlocked: bool;
  var pendingWrites: seq[int];
  var blockedWrites: seq[int];
  var inFlight: int;
  var nWrites: int;
  // the header as last read
  var snapc: tSnapc;
  var snapsByName: map[int, int];
  var snapInfo: map[int, tSnap];
  var parent: tParent;
  var refreshSeq: int;
  var lastRefresh: int;
  var refreshing: int;
  // operations
  var opBusy: bool;
  var opKind: tReqKind;
  var opName: int;
  var opAsyncId: int;
  var opSnapId: int;
  var opObj: int;
  var opDraining: bool;
  var opQ: seq[tQueuedOp];
  var asyncPending: set[int];
  var asyncComplete: map[int, tRc];
  var iaKind: tReqKind;
  var iaName: int;
  var iaInFlight: bool;
  var iaRemoteWaiting: bool;
  var iaCancelled: bool;
  var iaGen: int;
  // a clone
  var clChild: int;
  var clSnap: int;
  var clRc: tRc;
  var clAttached: bool;

  start state Run {
    entry (p: (cfg: tCfg, id: int, store: machine, driver: machine, script: tScript)) {
      cfg = p.cfg;
      id = p.id;
      store = p.store;
      driver = p.driver;
      image = p.script.image;
      script = p.script.actions;
      requireLock = cfg.exclusiveLock;
      refreshSeq = 1;
      crashBudget = cfg.crashes;
      if (crashBudget > 0) {
        crashAt = 1 + choose(CRASH_STEPS());
      }
      Send(Op(OP_WATCH, image, T_WATCH()));
    }

    on eRes do (r: tRes) {
      MayCrash();
      HandleRes(r);
    }

    on eNotified do (n: (tag: int, acks: seq[tAck], rc: tRc)) {
      MayCrash();
      HandleNotified(n.tag, NotifyResult(n.acks, n.rc));
    }

    on eNotify do (n: (nid: int, image: int, n: tNotify)) {
      var ack: tAck;
      MayCrash();
      ack = HandleNotify(n.n);
      send store, eAck, (nid = n.nid, client = id, ack = ack);
    }

    on eDrain do (d: machine) {
      drainer = d;
      draining = true;
      CheckDrained();
    }

    // schedule_request_lock's timer: ask the owner again
    on eLockRetryTimer do (g: int) {
      if (g == reqGen && lstate == L_WAITING_FOR_LOCK && handle != 0) {
        NotifyRequestLock();
      }
    }

    // the request timer: the request is still unanswered, so retry it
    // (async_request_timed_out answers -ERESTART, and C_InvokeAsyncRequest
    // refreshes and sends it again). The timer fires once per request,
    // whenever the scheduler lets it: before or after the completion.
    on eRequestTimer do (g: int) {
      if (!iaRemoteWaiting || g != iaGen) {
        return;
      }
      if (iaInFlight) {
        send this, eRequestTimer, g;   // the notify's acks are still out: later
      } else {
        iaRemoteWaiting = false;
        IaRefresh();
      }
    }

    // Watcher::handle_error: the watch is gone; re-register it
    on eWatchError do (e: (image: int, rc: tRc)) {
      MayCrash();
      handle = 0;
      ownerId = 0;
      if (e.rc == EBLOCKLISTED) {
        blocklisted = true;
      }
      if (!rewatching) {
        rewatching = true;
        Send(WithCookie(Op(OP_UNWATCH, image, T_REWATCH_UNWATCH()), cookie));
      }
    }
  }

  // the client died: nothing it had in flight completes, its watches
  // lapse, and its lock entry stays until a peer breaks it
  state Dead {
    ignore eRes, eNotify, eNotified, eWatchError, eRequestTimer, eLockRetryTimer, eDrain;
  }

  // goto leaves the handler that called this. The step to die at is
  // chosen when the client starts, so that every step is as likely.
  fun MayCrash() {
    var remaining: int;
    if (crashBudget == 0) {
      return;
    }
    crashStep = crashStep + 1;
    if (crashStep != crashAt) {
      return;
    }
    crashBudget = 0;
    remaining = sizeof(script) - actIdx;
    if (actRunning) {
      remaining = remaining + 1;
    }
    announce mCrashed, (client = id, running = actRunning);
    send store, eCrash, id;
    send driver, eCrashed, (client = id, remaining = remaining);
    goto Dead;
  }

  fun Send(op: tOp) {
    send store, eOp, (from = this, client = id, op = op);
  }
  fun WithCookie(op: tOp, c: int): tOp {
    op.cookie = c;
    return op;
  }
  fun WithSnap(op: tOp, s: int): tOp {
    op.snap = s;
    return op;
  }
  fun Notify(kind: tNotifyKind, req: tReqKind, asyncId: int, name: int, result: tRc, tag: int) {
    send store, eNotifyReq, (from = this, client = id, image = image,
                             n = (kind = kind, from = id, req = req, asyncId = asyncId, snapName = name,
                                  result = result),
                             tag = tag);
  }
  fun NotifyFire(kind: tNotifyKind) {
    Notify(kind, R_WRITE, 0, 0, OK, T_N_FIRE());
  }

  /* the script */

  fun StartNextAction() {
    if (actIdx >= sizeof(script)) {
      return;
    }
    act = script[actIdx];
    actAsyncId = id * 100 + actIdx;
    actIdx = actIdx + 1;
    actRunning = true;
    announce mActionStarted, (client = id, kind = act.kind, asyncId = actAsyncId);
    if (RefreshRequired()) {
      Refresh(T_REFRESH_ACT());
    } else {
      DoAction();
    }
  }

  fun DoAction() {
    if (act.kind == R_WRITE) {
      SubmitWrite(act.obj);
    } else if (act.kind == R_READ_CHILD) {
      Send(WithSnap(ObjOp(OP_READ, image, act.obj, 0, T_RD_CHILD()), 0));
    } else if (act.kind == R_CLONE) {
      CloneStart();
    } else if (act.kind == R_ACQUIRE_LOCK) {
      actWaitsLock = true;
      Enqueue(A_ACQ());
      Kick();
    } else if (act.kind == R_RELEASE_LOCK) {
      actWaitsLock = true;
      Enqueue(A_REL());
      Kick();
    } else if (act.kind == R_REMOVE_IMAGE) {
      RemoveStart();
    } else {
      IaStart(act.kind, act.snapName);
    }
  }

  // the open failed: every action of the script is answered its error
  fun FailScript(rc: tRc) {
    while (actIdx < sizeof(script)) {
      act = script[actIdx];
      actAsyncId = id * 100 + actIdx;
      actIdx = actIdx + 1;
      announce mActionStarted, (client = id, kind = act.kind, asyncId = actAsyncId);
      announce mActionDone, (client = id, kind = act.kind, asyncId = actAsyncId, rc = rc);
      send driver, eActionDone, (client = id, rc = rc);
    }
  }

  // nothing in flight: an op for a peer, a write, a lock action, a notify
  fun CheckDrained() {
    if (draining && !opBusy && inFlight == 0 && sizeof(lockQ) == 0 && !iaInFlight &&
        !IsTransition() && !rewatching) {
      draining = false;
      send drainer, eDrained, id;
    }
  }

  fun ActionDone(rc: tRc) {
    if (!actRunning) {
      return;
    }
    actRunning = false;
    announce mActionDone, (client = id, kind = act.kind, asyncId = actAsyncId, rc = rc);
    send driver, eActionDone, (client = id, rc = rc);
    StartNextAction();
  }

  /* the header cache */

  fun RefreshRequired(): bool {
    return lastRefresh != refreshSeq;
  }
  fun Refresh(tag: int) {
    refreshing = refreshSeq;
    Send(Op(OP_READ_HEADER, image, tag));
  }
  fun ApplyRefresh(h: tHeader) {
    var s: int;
    var ids: seq[int];
    var i: int;
    var placed: bool;
    snapc = (sq = h.snapSeq, snaps = default(seq[int]));
    // the snap context lists every snapshot, newest first
    foreach (s in keys(h.snaps)) {
      placed = false;
      i = 0;
      while (!placed && i < sizeof(ids)) {
        if (s > ids[i]) {
          ids += (i, s);
          placed = true;
        }
        i = i + 1;
      }
      if (!placed) {
        ids += (sizeof(ids), s);
      }
    }
    snapc.snaps = ids;
    snapInfo = h.snaps;
    snapsByName = default(map[int, int]);
    foreach (s in keys(h.snaps)) {
      if (!h.snaps[s].trash) {
        snapsByName[h.snaps[s].name] = s;
      }
    }
    parent = h.parent;
    lastRefresh = refreshing;
  }

  /* the write path */

  fun SubmitWrite(obj: int) {
    if (writesBlocked) {
      blockedWrites += (sizeof(blockedWrites), obj);
      return;
    }
    if (cfg.exclusiveLock && requireLock) {
      if (!cfg.autoPolicy) {
        // StandardPolicy: a write without the lock is refused
        ActionDone(UnlockedOpError());
        return;
      }
      pendingWrites += (sizeof(pendingWrites), obj);
      Enqueue(A_ACQ());
      Kick();
      return;
    }
    nWrites = nWrites + 1;
    inFlight = inFlight + 1;
    Send(WithCookie(ObjOp(OP_WRITE, image, obj, id * 100 + nWrites, T_WRITE()), cookie));
  }
  fun ObjOp(kind: tOpKind, img: int, obj: int, value: int, tag: int): tOp {
    var op: tOp;
    op = Op(kind, img, tag);
    op.obj = obj;
    op.value = value;
    op.snapc = snapc;
    return op;
  }
  fun UnlockedOpError(): tRc {
    if (blocklisted) {
      return EBLOCKLISTED;
    }
    return EROFS;
  }
  fun DispatchPending() {
    var obj: int;
    while (sizeof(pendingWrites) > 0) {
      obj = pendingWrites[0];
      pendingWrites -= (0);
      SubmitWrite(obj);
    }
  }
  fun FailPendingWrites(rc: tRc) {
    while (sizeof(pendingWrites) > 0) {
      pendingWrites -= (0);
      ActionDone(rc);
    }
  }
  fun UnblockWrites() {
    var obj: int;
    writesBlocked = false;
    while (sizeof(blockedWrites) > 0) {
      obj = blockedWrites[0];
      blockedWrites -= (0);
      SubmitWrite(obj);
    }
  }
  // the last in-flight write completed
  fun Drained() {
    CheckDrained();
    if (lstate == L_PRE_RELEASING) {
      TryContinueRelease();
    }
    if (opBusy && opDraining) {
      opDraining = false;
      Send(Op(OP_SNAP_ALLOC, image, T_SC_ALLOC()));
    }
  }

  /* the lock: ManagedLock's action queue */

  fun Enqueue(a: int) {
    var q: int;
    foreach (q in lockQ) {
      if (q == a) {
        return;
      }
    }
    lockQ += (sizeof(lockQ), a);
  }
  fun IsTransition(): bool {
    return lstate != L_UNLOCKED && lstate != L_LOCKED;
  }
  fun IsOwner(): bool {
    return lstate == L_LOCKED || lstate == L_REACQUIRING || lstate == L_POST_ACQUIRING ||
           lstate == L_PRE_RELEASING;
  }
  fun Kick() {
    var a: int;
    if (IsTransition() || sizeof(lockQ) == 0) {
      return;
    }
    a = lockQ[0];
    if (a == A_ACQ() || a == A_TRY()) {
      StartAcquire();
    } else if (a == A_REL()) {
      StartRelease();
    } else {
      StartReacquire();
    }
  }
  fun CompleteAction(next: tLState, r: tRc) {
    var a: int;
    a = lockQ[0];
    lockQ -= (0);
    lstate = next;
    CheckDrained();
    if (a == A_ACQ() || a == A_TRY()) {
      if (r != OK) {
        FailPendingWrites(r);
      }
      if (opWaitsLock) {
        opWaitsLock = false;
        IaHandleAcquire(r);
      }
      if (actWaitsLock && act.kind == R_ACQUIRE_LOCK) {
        actWaitsLock = false;
        ActionDone(r);
      }
      if (rmWaitsLock) {
        rmWaitsLock = false;
        RemoveLocked(r);
      }
    } else if (a == A_REL()) {
      if (actWaitsLock && act.kind == R_RELEASE_LOCK) {
        actWaitsLock = false;
        ActionDone(r);
      }
      if (rmWaitsRelease) {
        rmWaitsRelease = false;
        Send(WithCookie(Op(OP_UNWATCH, image, T_RM_UNWATCH()), handle));
      }
    }
    Kick();
  }

  // ManagedLock::send_acquire_lock, ExclusiveLock::pre_acquire_lock_handler
  fun StartAcquire() {
    var r: tRc;
    if (lstate == L_LOCKED) {
      CompleteAction(L_LOCKED, OK);
      return;
    }
    if (handle == 0) {
      if (blocklisted) {
        CompleteAction(L_UNLOCKED, EBLOCKLISTED);
      } else {
        lstate = L_WAITING_FOR_REGISTER;
      }
      return;
    }
    lstate = L_ACQUIRING;
    cookie = handle;
    r = peerRet;
    peerRet = OK;
    if (r == EROFS) {
      HandleAcquireResult(EROFS);
      return;
    }
    locker = default(tLock);
    Send(Op(OP_GET_LOCK_INFO, image, T_ACQ_GET_LOCKER()));
  }

  // ManagedLock::handle_acquire_lock + ExclusiveLock::post_acquire_lock_handler
  fun HandleAcquireResult(r: tRc) {
    if (r == OK) {
      PostAcquire();
      return;
    }
    if (r == EROFS) {
      CompleteAction(L_UNLOCKED, EROFS);
      return;
    }
    if (lockQ[0] == A_ACQ() && (r == EBUSY || r == EAGAIN)) {
      lstate = L_WAITING_FOR_LOCK;
      NotifyRequestLock();
      return;
    }
    if (r == EAGAIN) {
      r = OK;   // TRY_LOCK reports 0 when a live peer owns the lock
    }
    CompleteAction(L_UNLOCKED, r);
  }

  fun NotifyRequestLock() {
    Notify(N_REQUEST_LOCK, R_WRITE, 0, 0, OK, T_N_REQUEST_LOCK());
  }
  // ImageWatcher::handle_request_lock
  fun HandleRequestLockResult(r: tRc) {
    if (lstate != L_WAITING_FOR_LOCK) {
      return;
    }
    if (r == ETIMEDOUT) {
      PeerNotification(OK);   // no owner answered: treat it as dead and retry now
    } else if (r == EROFS) {
      PeerNotification(EROFS);
    } else {
      // schedule_request_lock: ask again after the retry delay
      reqGen = reqGen + 1;
      send this, eLockRetryTimer, reqGen;
    }
  }
  // ExclusiveLock::handle_peer_notification
  fun PeerNotification(r: tRc) {
    if (lstate == L_WAITING_FOR_LOCK) {
      peerRet = r;
      StartAcquire();
    }
  }

  // PostAcquireRequest: refresh, then AcquiredLock and writes without the lock
  fun PostAcquire() {
    lstate = L_POST_ACQUIRING;
    if (RefreshRequired() || cfg.refreshOnAcquire) {
      Refresh(T_REFRESH_POST());
    } else {
      PostAcquired();
    }
  }
  fun PostAcquired() {
    ownerId = id;
    NotifyFire(N_ACQUIRED_LOCK);
    requireLock = false;
    DispatchPending();
    CompleteAction(L_LOCKED, OK);
  }

  // PreReleaseRequest: writes need the lock again, in-flight writes and
  // ops finish; then ReleaseRequest
  fun StartRelease() {
    if (lstate == L_UNLOCKED) {
      CompleteAction(L_UNLOCKED, OK);
      return;
    }
    lstate = L_PRE_RELEASING;
    requireLock = true;
    TryContinueRelease();
  }
  fun TryContinueRelease() {
    if (inFlight > 0 || opBusy) {
      return;
    }
    lstate = L_RELEASING;
    Send(WithCookie(Op(OP_UNLOCK, image, T_REL_UNLOCK()), cookie));
  }

  // ManagedLock::send_reacquire_lock
  fun StartReacquire() {
    var op: tOp;
    if (lstate != L_LOCKED) {
      CompleteAction(lstate, OK);
      return;
    }
    lstate = L_REACQUIRING;
    if (handle == 0) {
      ReleaseAcquire();
      return;
    }
    newCookie = handle;
    if (newCookie == cookie && cfg.blocklistOnBreak) {
      NotifyFire(N_ACQUIRED_LOCK);
      CompleteAction(L_LOCKED, OK);
      return;
    }
    op = WithCookie(Op(OP_SET_COOKIE, image, T_REACQ()), cookie);
    op.cookie2 = newCookie;
    Send(op);
  }
  // ManagedLock::release_acquire_lock: treat the lock as lost
  fun ReleaseAcquire() {
    lstate = L_LOCKED;
    lockQ -= (0);
    Enqueue(A_REL());
    Enqueue(A_ACQ());
    Kick();
  }

  /* notifies */

  fun NotifyResult(acks: seq[tAck], rc: tRc): tRc {
    var a: tAck;
    var n: int;
    var r: tRc;
    if (rc != OK) {
      return rc;
    }
    foreach (a in acks) {
      if (!a.empty) {
        n = n + 1;
        r = a.result;
      }
    }
    if (n == 0) {
      return ETIMEDOUT;
    }
    if (n > 1) {
      return EINVAL;
    }
    return r;
  }

  fun HandleNotified(tag: int, r: tRc) {
    if (tag == T_N_REQUEST_LOCK()) {
      HandleRequestLockResult(r);
    } else if (tag == T_N_ASYNC_REQUEST() + 100 * iaGen) {
      IaRemoteAnswered(r);
    } else if (tag == T_N_HEADER_UPDATE()) {
      OpDone(OK);
    }
  }

  // ImageWatcher::handle_payload
  fun HandleNotify(n: tNotify): tAck {
    var ack: tAck;
    var changed: bool;
    ack = (empty = true, result = OK);
    if (n.kind == N_REQUEST_LOCK) {
      if (n.from != id && IsOwner() && ownerId == id) {
        if (lstate == L_LOCKED) {
          if (cfg.autoPolicy && !removing) {
            ack = (empty = false, result = OK);
            Enqueue(A_REL());
            Kick();
          } else {
            ack = (empty = false, result = EROFS);
          }
        } else {
          ack = (empty = false, result = OK);
        }
      }
    } else if (n.kind == N_ACQUIRED_LOCK) {
      changed = ownerId != n.from;
      ownerId = n.from;
      if (n.from != id) {
        PeerNotification(OK);
        if (changed) {
          CancelAsyncRequests();
        }
      }
    } else if (n.kind == N_RELEASED_LOCK) {
      if (n.from == ownerId) {
        ownerId = 0;
      }
      if (!IsOwner()) {
        reqGen = reqGen + 1;   // cancel(TASK_CODE_REQUEST_LOCK)
        PeerNotification(OK);
        CancelAsyncRequests();
      }
    } else if (n.kind == N_HEADER_UPDATE) {
      refreshSeq = refreshSeq + 1;
    } else if (n.kind == N_ASYNC_REQUEST) {
      ack = HandleAsyncRequest(n);
    } else if (n.kind == N_ASYNC_COMPLETE) {
      if (iaRemoteWaiting && n.asyncId == actAsyncId) {
        iaRemoteWaiting = false;
        refreshSeq = refreshSeq + 1;
        ActionDone(n.result);
      }
    }
    return ack;
  }

  /* operations: Operations::C_InvokeAsyncRequest */

  fun Proxied(kind: tReqKind): bool {
    if (!cfg.exclusiveLock) {
      return false;
    }
    if (kind == R_SNAP_REMOVE) {
      return cfg.proxySnapRemove;
    }
    if (kind == R_SNAP_PROTECT || kind == R_SNAP_UNPROTECT) {
      return cfg.proxyProtect;
    }
    return true;
  }
  fun IaStart(kind: tReqKind, name: int) {
    iaKind = kind;
    iaName = name;
    iaRemoteWaiting = false;
    iaCancelled = false;
    IaRefresh();
  }
  fun IaRefresh() {
    if (RefreshRequired()) {
      Refresh(T_REFRESH_IA());
    } else {
      IaAcquire();
    }
  }
  fun IaAcquire() {
    if (!Proxied(iaKind) || lstate == L_LOCKED) {
      IaLocal();
      return;
    }
    opWaitsLock = true;
    Enqueue(A_TRY());
    Kick();
  }
  fun IaHandleAcquire(r: tRc) {
    if (r != OK) {
      ActionDone(UnlockedOpError());
    } else if (lstate == L_LOCKED) {
      IaLocal();
    } else {
      IaRemote();
    }
  }
  fun IaLocal() {
    if (Proxied(iaKind) && lstate != L_LOCKED) {
      IaRefresh();   // start_op: the lock was lost, -ERESTART
      return;
    }
    QueueOp((asyncId = 0, kind = iaKind, name = iaName, requester = id));
  }
  // notify_async_request: the request is registered (so an AsyncComplete
  // that arrives before the notify's acks counts), then sent
  fun IaRemote() {
    iaInFlight = true;
    iaRemoteWaiting = true;
    iaGen = iaGen + 1;
    send this, eRequestTimer, iaGen;
    Notify(N_ASYNC_REQUEST, iaKind, actAsyncId, iaName, OK, T_N_ASYNC_REQUEST() + 100 * iaGen);
  }
  fun IaRemoteAnswered(r: tRc) {
    iaInFlight = false;
    CheckDrained();
    if (!iaRemoteWaiting) {
      return;   // its AsyncComplete came first, or the request was cancelled
    }
    if (iaCancelled) {
      iaCancelled = false;
      iaRemoteWaiting = false;
      IaRefresh();
    } else if (r == ETIMEDOUT || r == ERESTART) {
      iaRemoteWaiting = false;
      IaRefresh();
    } else if (r != OK) {
      iaRemoteWaiting = false;
      ActionDone(r);
    }
  }
  // ImageWatcher::schedule_cancel_async_requests: the owner changed
  fun CancelAsyncRequests() {
    if (!iaRemoteWaiting) {
      return;
    }
    if (iaInFlight) {
      iaCancelled = true;   // the notify's result is ignored when it comes
    } else {
      iaRemoteWaiting = false;
      IaRefresh();
    }
  }

  // ImageWatcher::handle_operation_request on the owner
  fun HandleAsyncRequest(n: tNotify): tAck {
    var ack: tAck;
    ack = (empty = true, result = OK);
    if (lstate != L_LOCKED) {
      return ack;
    }
    ack.empty = false;
    if (n.from == id) {
      ack.result = ERESTART;
    } else if (n.asyncId in asyncPending) {
      ack.result = OK;
    } else if (n.asyncId in asyncComplete && asyncComplete[n.asyncId] != OK) {
      ack.result = asyncComplete[n.asyncId];
    } else if (n.asyncId in asyncComplete) {
      // The request completed, and its AsyncComplete was sent while the
      // requester was not waiting for it (it cancelled the request when
      // it saw a lock owner announce itself, and retried). The owner
      // answers 0 from m_async_complete, the requester waits for an
      // AsyncComplete that never comes, times out after
      // rbd_request_timed_out_seconds, retries, and so on until the
      // completed entry expires (600 s): then the request is run again.
      // The model runs it again at once: the outcome is the same.
      asyncPending += (n.asyncId);
      asyncComplete -= (n.asyncId);
      ack.result = OK;
      QueueOp((asyncId = n.asyncId, kind = n.req, name = n.snapName, requester = n.from));
    } else {
      asyncPending += (n.asyncId);
      ack.result = OK;
      QueueOp((asyncId = n.asyncId, kind = n.req, name = n.snapName, requester = n.from));
    }
    return ack;
  }

  fun QueueOp(q: tQueuedOp) {
    if (opBusy) {
      opQ += (sizeof(opQ), q);
    } else {
      ExecuteOp(q);
    }
  }
  fun ExecuteOp(q: tQueuedOp) {
    opBusy = true;
    opKind = q.kind;
    opName = q.name;
    opAsyncId = q.asyncId;
    opDraining = false;
    if (q.kind == R_SNAP_CREATE) {
      SnapCreateStart();
    } else if (q.kind == R_SNAP_REMOVE) {
      SnapRemoveStart();
    } else if (q.kind == R_SNAP_PROTECT) {
      ProtectStart();
    } else if (q.kind == R_SNAP_UNPROTECT) {
      UnprotectStart();
    } else {
      FlattenStart();
    }
  }
  // the op's last step: C_NotifyUpdate sends a HeaderUpdate on success
  fun OpSucceeded() {
    refreshSeq = refreshSeq + 1;
    Notify(N_HEADER_UPDATE, R_WRITE, 0, 0, OK, T_N_HEADER_UPDATE());
  }
  fun OpDone(r: tRc) {
    var q: tQueuedOp;
    opBusy = false;
    CheckDrained();
    if (opAsyncId != 0) {
      asyncPending -= (opAsyncId);
      asyncComplete[opAsyncId] = r;
      Notify(N_ASYNC_COMPLETE, opKind, opAsyncId, 0, r, T_N_FIRE());
    } else {
      ActionDone(r);
    }
    if (lstate == L_PRE_RELEASING) {
      TryContinueRelease();
    }
    if (!opBusy && sizeof(opQ) > 0) {
      q = opQ[0];
      opQ -= (0);
      ExecuteOp(q);
    }
  }

  /* SnapshotCreateRequest */

  fun SnapCreateStart() {
    if (opName in snapsByName) {
      OpDone(EEXIST);
      return;
    }
    writesBlocked = true;
    if (inFlight > 0) {
      opDraining = true;
    } else {
      Send(Op(OP_SNAP_ALLOC, image, T_SC_ALLOC()));
    }
  }
  fun SnapCreateFail(r: tRc) {
    UnblockWrites();
    OpDone(r);
  }

  /* SnapshotRemoveRequest */

  fun SnapRemoveStart() {
    if (!(opName in snapsByName)) {
      OpDone(ENOENT);
      return;
    }
    opSnapId = snapsByName[opName];
    if (snapInfo[opSnapId].protection == PROTECTED()) {
      OpDone(EBUSY);
      return;
    }
    Send(WithSnap(Op(OP_SNAP_TRASH_ADD, image, T_SR_TRASH_ADD()), opSnapId));
  }

  /* SnapshotProtectRequest, SnapshotUnprotectRequest */

  fun ProtectStart() {
    if (!(opName in snapsByName)) {
      OpDone(ENOENT);
      return;
    }
    opSnapId = snapsByName[opName];
    if (snapInfo[opSnapId].protection == PROTECTED()) {
      OpDone(EBUSY);
      return;
    }
    Send(SetProtection(opSnapId, PROTECTED(), snapInfo[opSnapId].protection, T_SP_SET()));
  }
  fun UnprotectStart() {
    if (!(opName in snapsByName)) {
      OpDone(ENOENT);
      return;
    }
    opSnapId = snapsByName[opName];
    if (snapInfo[opSnapId].protection == UNPROTECTED()) {
      OpDone(EINVAL);
      return;
    }
    Send(SetProtection(opSnapId, UNPROTECTING(), snapInfo[opSnapId].protection, T_SU_START()));
  }
  fun SetProtection(snap: int, status: int, seen: int, tag: int): tOp {
    var op: tOp;
    op = Op(OP_SET_PROTECTION, image, tag);
    op.snap = snap;
    op.status = status;
    op.value = seen;
    return op;
  }
  fun ChildOp(kind: tOpKind, p: tParent, child: int, tag: int): tOp {
    var op: tOp;
    op = Op(kind, p.image, tag);
    op.snap = p.snap;
    op.parent = p;
    op.child = child;
    return op;
  }

  /* FlattenRequest, on this image as the child */

  fun FlattenStart() {
    if (parent.image == 0) {
      OpDone(EINVAL);
      return;
    }
    opObj = 0;
    FlattenNextObject();
  }
  fun FlattenNextObject() {
    opObj = opObj + 1;
    if (opObj > NOBJ()) {
      FlattenDetachChild();
      return;
    }
    Send(WithSnap(ObjOp(OP_READ, image, opObj, 0, T_FL_READ_CHILD()), 0));
  }
  fun FlattenDetachChild() {
    if (sizeof(snapInfo) > 0 && !cfg.deepFlatten) {
      // the snapshots still reference the parent: keep the child attached
      FlattenDetachParent();
      return;
    }
    if (cfg.cloneV2) {
      Send(ChildOp(OP_CHILD_DETACH, parent, image, T_FL_DETACH_CHILD()));
    } else {
      Send(ChildOp(OP_REMOVE_CHILD, parent, image, T_FL_DETACH_CHILD()));
    }
  }
  fun FlattenDetachParent() {
    var op: tOp;
    op = Op(OP_REMOVE_PARENT, image, T_FL_DETACH_PARENT());
    if (cfg.deepFlatten) {
      op.value = 1;
    }
    Send(op);
  }

  /* image::CloneRequest, on this image as the parent */

  fun CloneStart() {
    clChild = act.child;
    clAttached = false;
    if (!(act.snapName in snapsByName)) {
      ActionDone(ENOENT);
      return;
    }
    clSnap = snapsByName[act.snapName];
    if (!cfg.cloneV2 && snapInfo[clSnap].protection != PROTECTED()) {
      ActionDone(EINVAL);
      return;
    }
    Send(Op(OP_CREATE_IMAGE, clChild, T_CL_CREATE()));
  }
  fun CloneRollback(r: tRc) {
    clRc = r;
    if (clAttached) {
      if (cfg.cloneV2) {
        Send(ChildOp(OP_CHILD_DETACH, (image = image, snap = clSnap), clChild, T_CL_RB_DETACH()));
      } else {
        Send(ChildOp(OP_REMOVE_CHILD, (image = image, snap = clSnap), clChild, T_CL_RB_DETACH()));
      }
    } else {
      Send(Op(OP_REMOVE_IMAGE, clChild, T_CL_RB_REMOVE()));
    }
  }

  /* rbd rm: image::PreRemoveRequest, then image::RemoveRequest */

  fun RemoveStart() {
    removing = true;   // StandardPolicy: the lock is not given up while removing
    if (cfg.exclusiveLock) {
      rmWaitsLock = true;
      Enqueue(A_ACQ());
      Kick();
    } else {
      RemoveLocked(OK);
    }
  }
  fun RemoveLocked(r: tRc) {
    var s: int;
    if (cfg.exclusiveLock && (r != OK || lstate != L_LOCKED)) {
      removing = false;
      ActionDone(EBUSY);   // not forced
      return;
    }
    // check_image_snaps, on the snapshots as last read: a user snapshot
    // refuses the removal; a trashed one is removed first
    rmTrash = default(seq[int]);
    foreach (s in keys(snapInfo)) {
      if (!snapInfo[s].trash) {
        removing = false;
        ActionDone(ENOTEMPTY);
        return;
      }
      rmTrash += (sizeof(rmTrash), s);
    }
    RemoveNextTrash();
  }
  fun RemoveNextTrash() {
    var s: int;
    if (sizeof(rmTrash) == 0) {
      Send(Op(OP_LIST_WATCHERS, image, T_RM_WATCHERS()));
      return;
    }
    s = rmTrash[0];
    rmTrash -= (0);
    opSnapId = s;
    Send(WithSnap(Op(OP_SNAP_RELEASE, image, T_RM_TRASH_RELEASE()), s));
  }
  // RemoveRequest::detach_child: this image as a child of its parent
  fun RemoveDetachChild() {
    if (parent.image == 0) {
      RemoveClose();
    } else if (cfg.cloneV2) {
      Send(ChildOp(OP_CHILD_DETACH, parent, image, T_RM_DETACH()));
    } else {
      Send(ChildOp(OP_REMOVE_CHILD, parent, image, T_RM_DETACH()));
    }
  }
  // the last-chance snapshot check on the cached snapshots, then close
  // the image: the lock is released and the watch unregistered
  fun RemoveClose() {
    var s: int;
    foreach (s in keys(snapInfo)) {
      if (!snapInfo[s].trash) {
        removing = false;
        ActionDone(ENOTEMPTY);
        return;
      }
    }
    if (cfg.exclusiveLock) {
      rmWaitsRelease = true;
      Enqueue(A_REL());
      Kick();
    } else {
      Send(WithCookie(Op(OP_UNWATCH, image, T_RM_UNWATCH()), handle));
    }
  }

  /* answers from the store */

  fun HandleRes(r: tRes) {
    var t: int;
    var op: tOp;
    t = r.tag;
    if (t == T_WATCH()) {
      if (r.rc != OK) {
        FailScript(r.rc);   // the open failed: nothing runs
        return;
      }
      handle = r.n;
      Refresh(T_REFRESH_OPEN());
    } else if (t == T_REWATCH_UNWATCH()) {
      Send(Op(OP_WATCH, image, T_REWATCH_WATCH()));
    } else if (t == T_REWATCH_WATCH()) {
      rewatching = false;
      CheckDrained();
      if (r.rc == OK) {
        handle = r.n;
      } else if (r.rc == EBLOCKLISTED) {
        blocklisted = true;
      }
      // ImageWatcher::handle_rewatch_complete
      refreshSeq = refreshSeq + 1;
      if (lstate == L_WAITING_FOR_REGISTER || lstate == L_WAITING_FOR_LOCK) {
        StartAcquire();
      } else if (lstate == L_LOCKED || lstate == L_ACQUIRING || lstate == L_POST_ACQUIRING) {
        Enqueue(A_REACQ());
        Kick();
      }
    } else if (t == T_REFRESH_OPEN() || t == T_REFRESH_ACT() || t == T_REFRESH_IA() ||
               t == T_REFRESH_POST() || t == T_REFRESH_CLONE()) {
      if (r.rc == OK) {
        ApplyRefresh(r.hdr);
      }
      if (t == T_REFRESH_OPEN()) {
        if (r.rc != OK) {
          FailScript(r.rc);   // the open failed: nothing runs
        } else {
          StartNextAction();
        }
      } else if (r.rc != OK && (t == T_REFRESH_ACT() || t == T_REFRESH_IA())) {
        ActionDone(r.rc);   // the image is gone: the op fails
      } else if (t == T_REFRESH_ACT()) {
        DoAction();
      } else if (t == T_REFRESH_IA()) {
        IaAcquire();
      } else if (t == T_REFRESH_POST()) {
        PostAcquired();
      } else {
        // AttachChildRequest: the parent must still be PROTECTED
        if (r.rc != OK || !(clSnap in r.hdr.snaps) || r.hdr.snaps[clSnap].protection != PROTECTED()) {
          CloneRollback(EINVAL);
        } else {
          announce mCloneDone, clChild;
          ActionDone(OK);
        }
      }
    } else if (t == T_ACQ_GET_LOCKER()) {
      if (r.rc != OK) {
        HandleAcquireResult(r.rc);
        return;
      }
      locker = r.lock;
      Send(WithCookie(Op(OP_LOCK, image, T_ACQ_LOCK()), cookie));
    } else if (t == T_ACQ_LOCK()) {
      if (r.rc == OK) {
        HandleAcquireResult(OK);
      } else if (r.rc == EBUSY && !locker.held) {
        Send(Op(OP_GET_LOCK_INFO, image, T_ACQ_GET_LOCKER()));
      } else if (r.rc == EBUSY) {
        Send(Op(OP_LIST_WATCHERS, image, T_BRK_WATCHERS()));
      } else {
        HandleAcquireResult(r.rc);
      }
    } else if (t == T_BRK_WATCHERS()) {
      if (r.rc != OK) {
        HandleAcquireResult(r.rc);
      } else if (locker.owner in r.watchers && r.watchers[locker.owner] == locker.cookie) {
        HandleAcquireResult(EAGAIN);   // the holder is alive
      } else {
        Send(Op(OP_GET_LOCK_INFO, image, T_BRK_GET_LOCKER()));
      }
    } else if (t == T_BRK_GET_LOCKER()) {
      if (r.rc != OK) {
        HandleAcquireResult(r.rc);
      } else if (!r.lock.held) {
        locker = default(tLock);
        Send(WithCookie(Op(OP_LOCK, image, T_ACQ_LOCK()), cookie));
      } else if (r.lock != locker) {
        HandleAcquireResult(EAGAIN);
      } else if (cfg.blocklistOnBreak) {
        if (locker.owner == id) {
          HandleAcquireResult(EINVAL);
        } else {
          op = Op(OP_BLOCKLIST, image, T_BRK_BLOCKLIST());
          op.owner = locker.owner;
          Send(op);
        }
      } else {
        SendBreak();
      }
    } else if (t == T_BRK_BLOCKLIST()) {
      if (r.rc != OK) {
        HandleAcquireResult(r.rc);
      } else {
        SendBreak();
      }
    } else if (t == T_BRK_BREAK()) {
      if (r.rc != OK && r.rc != ENOENT) {
        HandleAcquireResult(r.rc);
      } else {
        locker = default(tLock);
        Send(WithCookie(Op(OP_LOCK, image, T_ACQ_LOCK()), cookie));
      }
    } else if (t == T_REACQ()) {
      if (r.rc == OK) {
        cookie = newCookie;
        NotifyFire(N_ACQUIRED_LOCK);
        CompleteAction(L_LOCKED, OK);
      } else {
        ReleaseAcquire();
      }
    } else if (t == T_REL_UNLOCK()) {
      // ReleaseRequest reports success whatever unlock answered
      cookie = 0;
      ownerId = 0;
      NotifyFire(N_RELEASED_LOCK);
      CompleteAction(L_UNLOCKED, OK);
    } else if (t == T_WRITE()) {
      inFlight = inFlight - 1;
      ActionDone(r.rc);
      if (inFlight == 0) {
        Drained();
      }
    } else if (t == T_RD_CHILD()) {
      if (r.rc == ENOENT && parent.image != 0) {
        Send(WithSnap(ObjOp(OP_READ, parent.image, act.obj, 0, T_RD_PARENT()), parent.snap));
      } else {
        ActionDone(OK);
      }
    } else if (t == T_RD_PARENT()) {
      announce mParentRead, (child = image, obj = act.obj, rc = r.rc);
      ActionDone(OK);
    } else {
      HandleOpRes(r);
    }
  }

  fun SendBreak() {
    var op: tOp;
    op = Op(OP_BREAK_LOCK, image, T_BRK_BREAK());
    op.owner = locker.owner;
    op.cookie = locker.cookie;
    Send(op);
  }

  // the ops' and the clone's steps
  fun HandleOpRes(r: tRes) {
    var t: int;
    var s: tSnap;
    var i: int;
    var w: int;
    t = r.tag;
    if (t == T_SC_ALLOC()) {
      if (r.rc != OK) {
        SnapCreateFail(r.rc);
        return;
      }
      opSnapId = r.n;
      Send(WithName(WithSnap(Op(OP_SNAP_ADD, image, T_SC_ADD()), opSnapId), opName));
    } else if (t == T_SC_ADD()) {
      if (r.rc == ESTALE) {
        // the id is older than the header's snap_seq: allocate again; the
        // stale id is not released
        Send(Op(OP_SNAP_ALLOC, image, T_SC_ALLOC()));
      } else if (r.rc != OK) {
        Send(WithSnap(Op(OP_SNAP_RELEASE, image, T_SC_RELEASE()), opSnapId));
      } else {
        announce mSnapAddedFor, opAsyncId;
        // update_snap_context, while writes are still blocked
        snapc.sq = opSnapId;
        snapc.snaps += (0, opSnapId);
        s = default(tSnap);
        s.name = opName;
        s.parent = parent;
        snapInfo[opSnapId] = s;
        snapsByName[opName] = opSnapId;
        UnblockWrites();
        OpSucceeded();
      }
    } else if (t == T_SC_RELEASE()) {
      SnapCreateFail(EINVAL);
    } else if (t == T_SR_TRASH_ADD()) {
      if (r.rc != OK && r.rc != EEXIST) {
        OpDone(r.rc);
      } else {
        Send(WithSnap(Op(OP_SNAP_GET, image, T_SR_GET()), opSnapId));
      }
    } else if (t == T_SR_GET()) {
      if (r.rc != OK) {
        OpDone(r.rc);
      } else if (r.snap.childCount > 0) {
        // children are attached: the snapshot stays in the trash
        snapInfo[opSnapId].trash = true;
        snapsByName -= (opName);
        OpSucceeded();
      } else {
        Send(WithSnap(Op(OP_SNAP_RELEASE, image, T_SR_RELEASE()), opSnapId));
      }
    } else if (t == T_SR_RELEASE()) {
      if (r.rc != OK && r.rc != ENOENT) {
        OpDone(r.rc);
      } else {
        Send(WithSnap(Op(OP_SNAP_REMOVE, image, T_SR_REMOVE()), opSnapId));
      }
    } else if (t == T_SR_REMOVE()) {
      if (r.rc != OK) {
        OpDone(r.rc);
      } else {
        snapInfo -= (opSnapId);
        snapsByName -= (opName);
        OpSucceeded();
      }
    } else if (t == T_SP_SET()) {
      if (r.rc != OK) {
        OpDone(r.rc);
      } else {
        snapInfo[opSnapId].protection = PROTECTED();
        OpSucceeded();
      }
    } else if (t == T_SU_START()) {
      if (r.rc != OK) {
        OpDone(r.rc);
      } else if (cfg.unprotectScans) {
        Send(ChildOp(OP_GET_CHILDREN, (image = image, snap = opSnapId), 0, T_SU_SCAN()));
      } else {
        Send(SetProtection(opSnapId, UNPROTECTED(), UNPROTECTING(), T_SU_FINISH()));
      }
    } else if (t == T_SU_SCAN()) {
      if (r.rc == OK && sizeof(r.children) > 0) {
        Send(SetProtection(opSnapId, PROTECTED(), UNPROTECTING(), T_SU_ROLLBACK()));
      } else if (r.rc != OK && r.rc != ENOENT) {
        Send(SetProtection(opSnapId, PROTECTED(), UNPROTECTING(), T_SU_ROLLBACK()));
      } else {
        Send(SetProtection(opSnapId, UNPROTECTED(), UNPROTECTING(), T_SU_FINISH()));
      }
    } else if (t == T_SU_ROLLBACK()) {
      OpDone(EBUSY);
    } else if (t == T_SU_FINISH()) {
      if (r.rc != OK) {
        OpDone(r.rc);
      } else {
        snapInfo[opSnapId].protection = UNPROTECTED();
        OpSucceeded();
      }
    } else if (t == T_FL_READ_CHILD()) {
      if (r.rc == ENOENT) {
        Send(WithSnap(ObjOp(OP_READ, parent.image, opObj, 0, T_FL_READ_PARENT()), parent.snap));
      } else {
        FlattenNextObject();
      }
    } else if (t == T_FL_READ_PARENT()) {
      announce mParentRead, (child = image, obj = opObj, rc = r.rc);
      if (r.rc == OK) {
        Send(WithCookie(ObjOp(OP_COPYUP, image, opObj, r.n, T_FL_COPYUP()), cookie));
      } else {
        FlattenNextObject();   // nothing in the parent: zeros, no object
      }
    } else if (t == T_FL_COPYUP()) {
      FlattenNextObject();
    } else if (t == T_FL_DETACH_CHILD()) {
      if (cfg.cloneV2 && r.rc == OK) {
        Send(WithSnap(Op(OP_SNAP_GET, parent.image, T_FL_SNAP_GET()), parent.snap));
      } else {
        FlattenDetachParent();
      }
    } else if (t == T_FL_SNAP_GET()) {
      if (r.rc == OK && r.snap.trash && r.snap.childCount == 0) {
        // DetachChildRequest removes the trashed snapshot nobody uses
        Send(WithSnap(Op(OP_SNAP_RELEASE, parent.image, T_FL_TRASH_RELEASE()), parent.snap));
      } else {
        FlattenDetachParent();
      }
    } else if (t == T_FL_TRASH_RELEASE()) {
      Send(WithSnap(Op(OP_SNAP_REMOVE, parent.image, T_FL_TRASH_REMOVE()), parent.snap));
    } else if (t == T_FL_TRASH_REMOVE()) {
      FlattenDetachParent();   // an error is logged and swallowed
    } else if (t == T_FL_DETACH_PARENT()) {
      if (r.rc != OK && r.rc != ENOENT) {
        OpDone(r.rc);
      } else {
        parent = default(tParent);
        OpSucceeded();
      }
    } else if (t == T_CL_CREATE()) {
      if (r.rc != OK) {
        ActionDone(r.rc);
      } else {
        Send(WithParent(Op(OP_SET_PARENT, clChild, T_CL_SET_PARENT()), (image = image, snap = clSnap)));
      }
    } else if (t == T_CL_SET_PARENT()) {
      if (r.rc != OK) {
        CloneRollback(r.rc);
      } else if (cfg.cloneV2) {
        Send(ChildOp(OP_CHILD_ATTACH, (image = image, snap = clSnap), clChild, T_CL_ATTACH()));
      } else {
        Send(ChildOp(OP_ADD_CHILD, (image = image, snap = clSnap), clChild, T_CL_ATTACH()));
      }
    } else if (t == T_CL_ATTACH()) {
      if (r.rc != OK) {
        CloneRollback(r.rc);
      } else {
        clAttached = true;
        if (!cfg.cloneV2 && cfg.cloneRechecks) {
          Refresh(T_REFRESH_CLONE());
        } else {
          announce mCloneDone, clChild;
          ActionDone(OK);
        }
      }
    } else if (t == T_CL_RB_DETACH()) {
      Send(Op(OP_REMOVE_IMAGE, clChild, T_CL_RB_REMOVE()));
    } else if (t == T_CL_RB_REMOVE()) {
      ActionDone(clRc);
    } else if (t == T_RM_TRASH_RELEASE()) {
      Send(WithSnap(Op(OP_SNAP_REMOVE, image, T_RM_TRASH_REMOVE()), opSnapId));
    } else if (t == T_RM_TRASH_REMOVE()) {
      if (r.rc == EBUSY) {
        removing = false;
        ActionDone(EBUSY);   // -ECHILD: a clone still uses the trashed snapshot
      } else {
        snapInfo -= (opSnapId);
        RemoveNextTrash();
      }
    } else if (t == T_RM_WATCHERS()) {
      i = 0;
      foreach (w in keys(r.watchers)) {
        if (w != id) {
          i = 1;
        }
      }
      if (r.rc != OK || i == 1) {
        removing = false;
        ActionDone(EBUSY);   // image has watchers
      } else {
        RemoveDetachChild();
      }
    } else if (t == T_RM_DETACH()) {
      if (cfg.cloneV2 && r.rc == OK) {
        Send(WithSnap(Op(OP_SNAP_GET, parent.image, T_RM_SNAP_GET()), parent.snap));
      } else {
        RemoveClose();
      }
    } else if (t == T_RM_SNAP_GET()) {
      if (r.rc == OK && r.snap.trash && r.snap.childCount == 0) {
        Send(WithSnap(Op(OP_SNAP_RELEASE, parent.image, T_RM_PTRASH_RELEASE()), parent.snap));
      } else {
        RemoveClose();
      }
    } else if (t == T_RM_PTRASH_RELEASE()) {
      Send(WithSnap(Op(OP_SNAP_REMOVE, parent.image, T_RM_PTRASH_REMOVE()), parent.snap));
    } else if (t == T_RM_PTRASH_REMOVE()) {
      RemoveClose();
    } else if (t == T_RM_UNWATCH()) {
      handle = 0;
      Send(Op(OP_REMOVE_IMAGE, image, T_RM_REMOVE()));
    } else if (t == T_RM_REMOVE()) {
      removing = false;
      ActionDone(r.rc);
    }
  }

  fun WithName(op: tOp, name: int): tOp {
    op.name = name;
    return op;
  }
  fun WithParent(op: tOp, p: tParent): tOp {
    op.parent = p;
    return op;
  }
}

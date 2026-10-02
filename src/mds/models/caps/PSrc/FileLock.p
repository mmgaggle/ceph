/*
 * The CEPH_LOCK_IFILE lock state machine, transcribed from the `filelock`
 * table in src/mds/locks.c.
 *
 * States reachable in this model: no replicas, no snapshots and no file
 * recovery. LOCK_MIX_LOCK is written as LOCK_MIX_LOCK2 because
 * Locker::simple_lock() moves straight to the second stage when the inode
 * is not replicated, and both stages issue no caps.
 *
 * Each row of the C table is split into accessor functions because P has
 * no constant tables. The `r`, `rd`, `wr`, `fwr` and `x` columns decide
 * whether the MDS may read, rdlock, wrlock, force a wrlock or xlock in that
 * state. The `rp` and `l` columns are not needed by the model.
 */

enum tLockState {
  LOCK_SYNC, LOCK_LOCK, LOCK_MIX, LOCK_EXCL, LOCK_XSYN,
  LOCK_LOCK_SYNC, LOCK_EXCL_SYNC, LOCK_MIX_SYNC, LOCK_XSYN_SYNC,
  LOCK_SYNC_LOCK, LOCK_EXCL_LOCK, LOCK_MIX_LOCK2, LOCK_XSYN_LOCK,
  LOCK_SYNC_MIX, LOCK_EXCL_MIX, LOCK_XSYN_MIX,
  LOCK_SYNC_EXCL, LOCK_MIX_EXCL, LOCK_LOCK_EXCL, LOCK_XSYN_EXCL,
  LOCK_EXCL_XSYN,
  LOCK_PREXLOCK, LOCK_XLOCK, LOCK_XLOCKDONE, LOCK_LOCK_XLOCK,
  LOCK_LOCK_MIX
}

/* the ANY / AUTH / XCL values of locks.h; REQ counts as NONE on the auth MDS */
enum tLockWho { WHO_NONE = 0, WHO_ANY = 1, WHO_AUTH = 2, WHO_XCL = 3 }

/* CAP_ANY / CAP_LONER / CAP_XLOCKER of SimpleLock::gcaps_allowed() */
enum tCapWho { CAP_ANY = 0, CAP_LONER = 1, CAP_XLOCKER = 2 }

type tLockRow = (stable: bool, next: tLockState, loner: bool,
                 r: tLockWho, rd: tLockWho, wr: tLockWho, fwr: tLockWho, x: tLockWho,
                 caps: tCaps, loner_caps: tCaps, xlocker_caps: tCaps);

fun LockRow(s: tLockState) : tLockRow {
  var row: tLockRow;
  var none: tCaps;
  row.stable = false;
  row.next = s;
  row.loner = false;
  row.r = WHO_NONE; row.rd = WHO_NONE; row.wr = WHO_NONE; row.fwr = WHO_NONE; row.x = WHO_NONE;
  row.caps = none; row.loner_caps = none; row.xlocker_caps = none;
  if (s == LOCK_SYNC)      { row.stable = true; row.r = WHO_ANY; row.rd = WHO_ANY; row.caps = Caps3(Fs, Fc, Fr); }
  if (s == LOCK_LOCK_SYNC) { row.next = LOCK_SYNC; row.r = WHO_AUTH; row.caps = Caps1(Fc); }
  if (s == LOCK_EXCL_SYNC) { row.next = LOCK_SYNC; row.loner = true; row.fwr = WHO_XCL; row.loner_caps = Caps3(Fs, Fc, Fr); }
  if (s == LOCK_MIX_SYNC)  { row.next = LOCK_SYNC; row.caps = Caps1(Fr); }
  if (s == LOCK_XSYN_SYNC) { row.next = LOCK_SYNC; row.loner = true; row.r = WHO_AUTH; row.rd = WHO_AUTH; row.loner_caps = Caps1(Fc); }

  if (s == LOCK_LOCK)      { row.stable = true; row.r = WHO_AUTH; row.wr = WHO_AUTH; row.caps = Caps2(Fc, Fb); }
  if (s == LOCK_SYNC_LOCK) { row.next = LOCK_LOCK; row.r = WHO_ANY; row.caps = Caps1(Fc); }
  if (s == LOCK_EXCL_LOCK) { row.next = LOCK_LOCK; row.fwr = WHO_XCL; row.caps = Caps2(Fc, Fb); }
  if (s == LOCK_MIX_LOCK2) { row.next = LOCK_LOCK; }
  if (s == LOCK_XSYN_LOCK) { row.next = LOCK_LOCK; row.loner = true; row.r = WHO_AUTH; row.wr = WHO_XCL; row.loner_caps = Caps2(Fc, Fb); }

  /* Keep Fcb to allow rapid recall of Fw. The client can keep buffered writes / cached reads. */
  if (s == LOCK_PREXLOCK)   { row.next = LOCK_LOCK; row.x = WHO_ANY; row.caps = Caps2(Fc, Fb); }
  if (s == LOCK_XLOCK)      { row.next = LOCK_LOCK; row.caps = Caps2(Fc, Fb); }
  if (s == LOCK_XLOCKDONE)  { row.next = LOCK_LOCK; row.r = WHO_XCL; row.rd = WHO_XCL; row.caps = Caps2(Fc, Fb); row.xlocker_caps = Caps1(Fs); }
  if (s == LOCK_LOCK_XLOCK) { row.next = LOCK_PREXLOCK; row.x = WHO_XCL; row.caps = Caps2(Fc, Fb); }

  if (s == LOCK_MIX)       { row.stable = true; row.wr = WHO_ANY; row.caps = Caps2(Fr, Fw); }
  /* not in locks.c: the proposed gather state for LOCK -> MIX (README, finding 3) */
  if (s == LOCK_LOCK_MIX)  { row.next = LOCK_MIX; }
  if (s == LOCK_SYNC_MIX)  { row.next = LOCK_MIX; row.r = WHO_ANY; row.caps = Caps1(Fr); }
  if (s == LOCK_EXCL_MIX)  { row.next = LOCK_MIX; row.loner = true; row.wr = WHO_XCL; row.loner_caps = Caps2(Fr, Fw); }
  if (s == LOCK_XSYN_MIX)  { row.next = LOCK_MIX; row.loner = true; row.wr = WHO_XCL; }

  if (s == LOCK_EXCL)      { row.stable = true; row.loner = true; row.rd = WHO_XCL; row.wr = WHO_XCL; row.loner_caps = CapsAll(); }
  if (s == LOCK_SYNC_EXCL) { row.next = LOCK_EXCL; row.loner = true; row.r = WHO_ANY; row.loner_caps = Caps3(Fs, Fc, Fr); }
  if (s == LOCK_MIX_EXCL)  { row.next = LOCK_EXCL; row.loner = true; row.wr = WHO_XCL; row.loner_caps = Caps2(Fr, Fw); }
  if (s == LOCK_LOCK_EXCL) { row.next = LOCK_EXCL; row.loner = true; row.r = WHO_AUTH; row.loner_caps = Caps2(Fc, Fb); }
  if (s == LOCK_XSYN_EXCL) { row.next = LOCK_EXCL; row.loner = true; row.r = WHO_AUTH; row.rd = WHO_XCL; row.loner_caps = Caps2(Fc, Fb); }

  if (s == LOCK_XSYN)      { row.stable = true; row.loner = true; row.r = WHO_AUTH; row.rd = WHO_AUTH; row.wr = WHO_XCL; row.loner_caps = Caps2(Fc, Fb); }
  if (s == LOCK_EXCL_XSYN) { row.next = LOCK_XSYN; row.loner = true; row.rd = WHO_XCL; row.loner_caps = Caps2(Fc, Fb); }
  return row;
}

/* the filelock changed state (observed by the LockTransitions spec) */
type tLockTransition = (prev: tLockState, next: tLockState);
event eLockTransition : tLockTransition;

fun LockIsStable(s: tLockState) : bool {
  return LockRow(s).stable;
}

fun LockNext(s: tLockState) : tLockState {
  return LockRow(s).next;
}

/*
 * SimpleLock::gcaps_allowed(who, s) on the auth MDS. `xlocked` is whether
 * get_xlock_by_client() >= 0.
 */
fun LockGcapsAllowed(who: tCapWho, s: tLockState, xlocked: bool) : tCaps {
  var row: tLockRow;
  row = LockRow(s);
  if (xlocked && who == CAP_XLOCKER) {
    return CapsUnion(row.xlocker_caps, row.caps);   // xlocker always gets more
  }
  if (row.loner && who == CAP_ANY) {
    return row.caps;
  }
  return CapsUnion(row.loner_caps, row.caps);        // loner always gets more
}

/* IS_TRUE_AND_LT_AUTH(x, auth=true) of Locker::eval_gather() */
fun LockWhoAllowsAuth(w: tLockWho) : bool {
  return w == WHO_ANY || w == WHO_AUTH;
}

/* SimpleLock::can_*(client): ANY, AUTH on the auth MDS, or XCL for the matching client */
fun LockWhoAllows(w: tLockWho, client: int, xclClient: int) : bool {
  return w == WHO_ANY || w == WHO_AUTH || (w == WHO_XCL && client >= 0 && xclClient == client);
}

/*
 * The edges Locker takes between states. Besides a gather completing to
 * LockNext(), these are the direct transitions the code makes.
 */
fun LockEdgeIsLegal(prev: tLockState, next: tLockState) : bool {
  if (next == LockNext(prev) && prev != next) { return true; }   // eval_gather, LOCK_XLOCK -> PREXLOCK
  if (LockIsStable(prev) && !LockIsStable(next) && LockIsStable(LockNext(next))) { return true; }   // start a transition
  if (prev == LOCK_LOCK && next == LOCK_MIX) { return true; }                         // scatter_mix from LOCK
  if ((prev == LOCK_LOCK || prev == LOCK_XLOCKDONE) && next == LOCK_LOCK_XLOCK) { return true; }   // simple_xlock
  if (prev == LOCK_PREXLOCK && next == LOCK_XLOCK) { return true; }                   // xlock_start
  if (prev == LOCK_XLOCK && next == LOCK_XLOCKDONE) { return true; }                  // set_xlocks_done
  if ((prev == LOCK_XLOCK || prev == LOCK_XLOCKDONE) && next == LOCK_EXCL) { return true; }   // _finish_xlock to the loner
  return false;
}

/*
 * The CEPH_LOCK_IFILE lock state machine, transcribed from the `filelock`
 * table in src/mds/locks.c.
 *
 * Only the states reachable in this model are listed: no xlocks, no
 * replicas, no snapshots and no file recovery. LOCK_MIX_LOCK is written as
 * LOCK_MIX_LOCK2 because Locker::simple_lock() moves straight to the second
 * stage when the inode is not replicated, and both stages issue no caps.
 *
 * Each row of the C table is split into small accessor functions because P
 * has no constant tables. The `rd`, `wr` and `fwr` columns decide whether an
 * MDS rdlock / wrlock / forced wrlock is allowed in that state.
 */

enum tLockState {
  LOCK_SYNC, LOCK_LOCK, LOCK_MIX, LOCK_EXCL, LOCK_XSYN,
  LOCK_LOCK_SYNC, LOCK_EXCL_SYNC, LOCK_MIX_SYNC, LOCK_XSYN_SYNC,
  LOCK_SYNC_LOCK, LOCK_EXCL_LOCK, LOCK_MIX_LOCK2, LOCK_XSYN_LOCK,
  LOCK_SYNC_MIX, LOCK_EXCL_MIX, LOCK_XSYN_MIX,
  LOCK_SYNC_EXCL, LOCK_MIX_EXCL, LOCK_LOCK_EXCL, LOCK_XSYN_EXCL,
  LOCK_EXCL_XSYN
}

/* the ANY / AUTH / XCL values of locks.h; REQ counts as NONE on the auth MDS */
enum tLockWho { WHO_NONE = 0, WHO_ANY = 1, WHO_AUTH = 2, WHO_XCL = 3 }

/* CAP_ANY / CAP_LONER of SimpleLock::gcaps_allowed() */
enum tCapWho { CAP_ANY = 0, CAP_LONER = 1 }

type tLockRow = (stable: bool, next: tLockState, loner: bool,
                 rd: tLockWho, wr: tLockWho, fwr: tLockWho,
                 caps: tCaps, loner_caps: tCaps);

fun LockRow(s: tLockState) : tLockRow {
  var r: tLockRow;
  var none: tCaps;
  r.stable = false;
  r.next = s;
  r.loner = false;
  r.rd = WHO_NONE; r.wr = WHO_NONE; r.fwr = WHO_NONE;
  r.caps = none; r.loner_caps = none;
  //                                  stable  loner  rd        wr        fwr       caps(any)                loner caps
  if (s == LOCK_SYNC)      { r.stable = true;             r.rd = WHO_ANY;                            r.caps = Caps3(Fs, Fc, Fr); }
  if (s == LOCK_LOCK_SYNC) { r.next = LOCK_SYNC;                                                      r.caps = Caps1(Fc); }
  if (s == LOCK_EXCL_SYNC) { r.next = LOCK_SYNC; r.loner = true;                      r.fwr = WHO_XCL;                       r.loner_caps = Caps3(Fs, Fc, Fr); }
  if (s == LOCK_MIX_SYNC)  { r.next = LOCK_SYNC;                                                      r.caps = Caps1(Fr); }
  if (s == LOCK_XSYN_SYNC) { r.next = LOCK_SYNC; r.loner = true; r.rd = WHO_AUTH;                                            r.loner_caps = Caps1(Fc); }

  if (s == LOCK_LOCK)      { r.stable = true;                              r.wr = WHO_AUTH;           r.caps = Caps2(Fc, Fb); }
  if (s == LOCK_SYNC_LOCK) { r.next = LOCK_LOCK;                                                      r.caps = Caps1(Fc); }
  if (s == LOCK_EXCL_LOCK) { r.next = LOCK_LOCK;                                      r.fwr = WHO_XCL; r.caps = Caps2(Fc, Fb); }
  if (s == LOCK_MIX_LOCK2) { r.next = LOCK_LOCK; }
  if (s == LOCK_XSYN_LOCK) { r.next = LOCK_LOCK; r.loner = true;           r.wr = WHO_XCL;                                   r.loner_caps = Caps2(Fc, Fb); }

  if (s == LOCK_MIX)       { r.stable = true;                              r.wr = WHO_ANY;            r.caps = Caps2(Fr, Fw); }
  if (s == LOCK_SYNC_MIX)  { r.next = LOCK_MIX;                                                       r.caps = Caps1(Fr); }
  if (s == LOCK_EXCL_MIX)  { r.next = LOCK_MIX;  r.loner = true;           r.wr = WHO_XCL;                                   r.loner_caps = Caps2(Fr, Fw); }
  if (s == LOCK_XSYN_MIX)  { r.next = LOCK_MIX;  r.loner = true;           r.wr = WHO_XCL; }

  if (s == LOCK_EXCL)      { r.stable = true;    r.loner = true; r.rd = WHO_XCL; r.wr = WHO_XCL;                             r.loner_caps = CapsAll(); }
  if (s == LOCK_SYNC_EXCL) { r.next = LOCK_EXCL; r.loner = true;                                                             r.loner_caps = Caps3(Fs, Fc, Fr); }
  if (s == LOCK_MIX_EXCL)  { r.next = LOCK_EXCL; r.loner = true;           r.wr = WHO_XCL;                                   r.loner_caps = Caps2(Fr, Fw); }
  if (s == LOCK_LOCK_EXCL) { r.next = LOCK_EXCL; r.loner = true;                                                             r.loner_caps = Caps2(Fc, Fb); }
  if (s == LOCK_XSYN_EXCL) { r.next = LOCK_EXCL; r.loner = true; r.rd = WHO_XCL;                                             r.loner_caps = Caps2(Fc, Fb); }

  if (s == LOCK_XSYN)      { r.stable = true;    r.loner = true; r.rd = WHO_AUTH; r.wr = WHO_XCL;                            r.loner_caps = Caps2(Fc, Fb); }
  if (s == LOCK_EXCL_XSYN) { r.next = LOCK_XSYN; r.loner = true; r.rd = WHO_XCL;                                             r.loner_caps = Caps2(Fc, Fb); }
  return r;
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
 * SimpleLock::gcaps_allowed(who, s) on the auth MDS, without the xlocker
 * column (no xlocks in this model).
 */
fun LockGcapsAllowed(who: tCapWho, s: tLockState) : tCaps {
  var row: tLockRow;
  row = LockRow(s);
  if (who == CAP_LONER) {
    return CapsUnion(row.loner_caps, row.caps);   // loner always gets more
  }
  if (row.loner) {
    return row.caps;
  }
  return CapsUnion(row.loner_caps, row.caps);
}

/* IS_TRUE_AND_LT_AUTH(x, auth=true) of Locker::eval_gather() */
fun LockWhoAllowsAuth(w: tLockWho) : bool {
  return w == WHO_ANY || w == WHO_AUTH;
}

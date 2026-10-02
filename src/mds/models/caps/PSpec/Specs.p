/*
 * Properties checked against the model.
 */

/*
 * DataCoherence: a read never returns data older than a write that had
 * completed (to its application) before the read started. This is the
 * end-to-end promise of the cap protocol: Fr/Fc on one client never
 * coexist with Fw/Fb on another, and the cache is dropped before another
 * client may write.
 */
spec DataCoherence observes eReadStart, eReadDone, eWriteDone, eWriteLost, eWriteSuperseded {
  var completed: set[int];           // versions some application has seen complete
  var lost: set[int];                // versions lost when their writer was blocklisted
  var superseded: map[int, int];     // old buffered version -> the version that overwrote it
  var snapshot: map[int, set[int]];  // completed at the time each client's read started

  /* a lost version takes every version that could only reach the store through it */
  fun MarkLost(v: int) {
    var k: int;
    var again: bool;
    lost += (v);
    again = true;
    while (again) {
      again = false;
      foreach (k in keys(superseded)) {
        if ((superseded[k] in lost) && !(k in lost)) {
          lost += (k);
          again = true;
        }
      }
    }
  }

  /* the newest completed version that was not lost since */
  fun Latest(vs: set[int]) : int {
    var v: int;
    var m: int;
    m = 0;
    foreach (v in vs) {
      if (v > m && !(v in lost)) { m = v; }
    }
    return m;
  }

  start state Watching {
    on eWriteDone do (w: tIoDone) {
      completed += (w.ver);
    }
    on eWriteLost do (v: int) {
      MarkLost(v);
    }
    on eWriteSuperseded do (s: tSuperseded) {
      superseded[s.older] = s.newer;
    }
    on eReadStart do (c: int) {
      snapshot[c] = completed;
    }
    on eReadDone do (r: tIoDone) {
      assert r.ver >= Latest(snapshot[r.client]),
        format("stale read: client {0} read version {1} but version {2} had completed before the read started",
               r.client, r.ver, Latest(snapshot[r.client]));
    }
  }
}

/*
 * CapTracking: the MDS never believes a client released a cap the client
 * still holds and trusts (Cap::implemented, as long as the client's
 * session cap_gen and cap_ttl make the cap valid, is always a subset of
 * Capability::issued on the MDS). Every lock transition relies on this:
 * the MDS waits for issued caps to drain before it changes state. A stale
 * session may have its non-write caps revoked by force, which is why the
 * client reports caps it no longer trusts as empty.
 */
spec CapTracking observes eMdsIssued, eClientImplemented {
  var mdsIssued: map[int, tCaps];
  var clientImpl: map[int, tCaps];

  fun Check(c: int) {
    var m: tCaps;
    var i: tCaps;
    if (c in mdsIssued) { m = mdsIssued[c]; }
    if (c in clientImpl) { i = clientImpl[c]; }
    assert CapsSubset(i, m),
      format("cap tracking: client {0} implements {1} but the MDS has issued only {2}", c, i, m);
  }

  start state Watching {
    on eMdsIssued do (a: tCapsAnnounce) {
      mdsIssued[a.client] = a.caps;
      Check(a.client);
    }
    on eClientImplemented do (a: tCapsAnnounce) {
      clientImpl[a.client] = a.caps;
      Check(a.client);
    }
  }
}

/*
 * IoProgress: every read, write, stat or setattr that an application
 * issues completes or is rejected with EBADF. A schedule that ends in the
 * hot state means a client waits forever for caps, for `max_size`, for a
 * flush, or for an MDS request that never gets its lock.
 */
spec IoProgress observes eAppRead, eAppWrite, eAppStat, eAppSetattr,
                         eReadDone, eWriteDone, eStatDone, eSetattrDone, eIoDropped {
  var outstanding: int;

  fun Started() {
    outstanding = outstanding + 1;
  }

  fun Finished() {
    outstanding = outstanding - 1;
    assert outstanding >= 0, "more completions than requests";
  }

  start cold state Idle {
    on eAppRead    do { Started(); goto Pending; }
    on eAppWrite   do { Started(); goto Pending; }
    on eAppStat    do { Started(); goto Pending; }
    on eAppSetattr do { Started(); goto Pending; }
    on eReadDone    do (r: tIoDone) { Finished(); }
    on eWriteDone   do (w: tIoDone) { Finished(); }
    on eStatDone    do (c: int) { Finished(); }
    on eSetattrDone do (c: int) { Finished(); }
    on eIoDropped   do (c: int) { Finished(); }
  }

  hot state Pending {
    on eAppRead    do { Started(); }
    on eAppWrite   do { Started(); }
    on eAppStat    do { Started(); }
    on eAppSetattr do { Started(); }
    on eReadDone    do (r: tIoDone) { Finished(); if (outstanding == 0) { goto Idle; } }
    on eWriteDone   do (w: tIoDone) { Finished(); if (outstanding == 0) { goto Idle; } }
    on eStatDone    do (c: int)     { Finished(); if (outstanding == 0) { goto Idle; } }
    on eSetattrDone do (c: int)     { Finished(); if (outstanding == 0) { goto Idle; } }
    on eIoDropped   do (c: int)     { Finished(); if (outstanding == 0) { goto Idle; } }
  }
}

/*
 * LockTransitions: the filelock only moves along the edges Locker takes
 * (see LockEdgeIsLegal in FileLock.p).
 */
spec LockTransitions observes eLockTransition {
  start state Watching {
    on eLockTransition do (t: tLockTransition) {
      assert LockEdgeIsLegal(t.prev, t.next),
        format("illegal lock transition {0} -> {1}", t.prev, t.next);
    }
  }
}

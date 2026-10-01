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
spec DataCoherence observes eReadStart, eReadDone, eWriteDone {
  var latest: int;              // newest version any application has seen complete
  var snapshot: map[int, int];  // latest at the time each client's read started

  start state Watching {
    on eWriteDone do (w: tIoDone) {
      if (w.ver > latest) {
        latest = w.ver;
      }
    }
    on eReadStart do (c: int) {
      snapshot[c] = latest;
    }
    on eReadDone do (r: tIoDone) {
      assert r.ver >= snapshot[r.client],
        format("stale read: client {0} read version {1} but version {2} had completed before the read started",
               r.client, r.ver, snapshot[r.client]);
    }
  }
}

/*
 * CapTracking: the MDS never believes a client released a cap the client
 * still holds (Cap::implemented on the client is always a subset of
 * Capability::issued on the MDS). Every lock transition relies on this:
 * the MDS waits for issued caps to drain before it changes state.
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
 * IoProgress: every read or write an application issues eventually
 * completes or is rejected. A schedule that ends with this monitor in the
 * hot state is a liveness bug: a client is stuck waiting for caps, for
 * max_size, or for a flush.
 */
spec IoProgress observes eAppRead, eAppWrite, eReadDone, eWriteDone, eIoDropped {
  var outstanding: int;

  fun Started() {
    outstanding = outstanding + 1;
  }

  fun Finished() {
    outstanding = outstanding - 1;
    assert outstanding >= 0, "more completions than requests";
  }

  start cold state Idle {
    on eAppRead  do { Started(); goto Pending; }
    on eAppWrite do { Started(); goto Pending; }
    on eReadDone  do (r: tIoDone) { Finished(); }
    on eWriteDone do (w: tIoDone) { Finished(); }
    on eIoDropped do (c: int) { Finished(); }
  }

  hot state Pending {
    on eAppRead  do { Started(); }
    on eAppWrite do { Started(); }
    on eReadDone  do (r: tIoDone) { Finished(); if (outstanding == 0) { goto Idle; } }
    on eWriteDone do (w: tIoDone) { Finished(); if (outstanding == 0) { goto Idle; } }
    on eIoDropped do (c: int)     { Finished(); if (outstanding == 0) { goto Idle; } }
  }
}

/*
 * LockTransitions: the filelock only moves along the edges of the locks.c
 * table. A stable state may enter an intermediate state whose target is
 * another stable state, and an intermediate state may only complete to its
 * own target (eval_gather) or, for simple_lock's MIX->LOCK staging, move
 * between stages that share a target.
 */
spec LockTransitions observes eLockTransition {
  start state Watching {
    on eLockTransition do (t: tLockTransition) {
      if (LockIsStable(t.prev)) {
        assert t.prev != t.next, format("lock transition {0} -> {1}: no-op from a stable state", t.prev, t.next);
        assert LockIsStable(LockNext(t.next)),
          format("lock transition {0} -> {1}: target state is not reachable from a stable state", t.prev, t.next);
      } else {
        assert t.next == LockNext(t.prev),
          format("lock transition {0} -> {1}: an unstable state may only complete to {2}", t.prev, t.next, LockNext(t.prev));
      }
    }
  }
}

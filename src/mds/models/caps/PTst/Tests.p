/*
 * Test drivers: one AppDriver per client issues a random sequence of
 * application operations and session events, pacing itself through its own
 * queue so the checker interleaves it with the MDS, the clients and the
 * store.
 */

type tDriverConfig = (client: machine, steps: int, writer: bool, sessions: bool);

event eNext;

machine AppDriver {
  var client: machine;
  var steps: int;
  var writer: bool;
  var sessions: bool;

  start state Init {
    entry (cfg: tDriverConfig) {
      client = cfg.client;
      steps = cfg.steps;
      writer = cfg.writer;
      sessions = cfg.sessions;
      // always start by opening the file
      send client, eAppOpen, PickMode();
      send this, eNext;
    }
    on eNext goto Step;
  }

  state Step {
    entry {
      var k: int;
      if (steps == 0) {
        return;
      }
      steps = steps - 1;
      k = choose(12);
      if (k == 0) { send client, eAppOpen, PickMode(); }
      else if (k == 1) { send client, eAppClose, PickMode(); }
      else if (k == 2 || k == 3) { send client, eAppRead; }
      else if (k == 4 || k == 5) { if (writer) { send client, eAppWrite; } else { send client, eAppRead; } }
      else if (k == 6) { send client, eAppFlushTick; }
      else if (k == 7) { send client, eAppTrim; }
      else if (k == 8) { send client, eAppStat; }
      else if (k == 9) { send client, eAppSetattr; }
      else if (k == 10) { if (sessions) { send client, eAppTtlExpire; } else { send client, eAppStat; } }
      else { send client, eAppRenewTick; }
      send this, eNext;
    }
    on eNext goto Step;
  }

  fun PickMode() : tMode {
    var k: int;
    if (!writer) {
      return MODE_RD;
    }
    k = choose(3);
    if (k == 0) { return MODE_RD; }
    if (k == 1) { return MODE_WR; }
    return MODE_RDWR;
  }
}

/* one writer and one reader, sessions never go stale */
machine TestWriterReader {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      store = new Store();
      mds = new MDS((store = store, gather_lock_to_mix = false));
      c1 = new Client((id = 1, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      c2 = new Client((id = 2, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      new AppDriver((client = c1, steps = 10, writer = true, sessions = false));
      new AppDriver((client = c2, steps = 10, writer = false, sessions = false));
    }
  }
}

/* two writers: exercises MIX, EXCL hand-over, loner changes, xlocks */
machine TestTwoWriters {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      store = new Store();
      mds = new MDS((store = store, gather_lock_to_mix = false));
      c1 = new Client((id = 1, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      c2 = new Client((id = 2, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      new AppDriver((client = c1, steps = 10, writer = true, sessions = false));
      new AppDriver((client = c2, steps = 10, writer = true, sessions = false));
    }
  }
}

/* two writers with the proposed MDS fix for LOCK -> MIX */
machine TestTwoWritersFixed {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      store = new Store();
      mds = new MDS((store = store, gather_lock_to_mix = true));
      c1 = new Client((id = 1, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      c2 = new Client((id = 2, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      new AppDriver((client = c1, steps = 10, writer = true, sessions = false));
      new AppDriver((client = c2, steps = 10, writer = true, sessions = false));
    }
  }
}

/* two writers and a reader */
machine TestThreeClients {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      var c3: machine;
      store = new Store();
      mds = new MDS((store = store, gather_lock_to_mix = false));
      c1 = new Client((id = 1, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      c2 = new Client((id = 2, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      c3 = new Client((id = 3, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      new AppDriver((client = c1, steps = 8, writer = true, sessions = false));
      new AppDriver((client = c2, steps = 8, writer = true, sessions = false));
      new AppDriver((client = c3, steps = 8, writer = false, sessions = false));
    }
  }
}

/* a writer and a reader whose sessions time out and renew */
machine TestStaleSessions {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      store = new Store();
      mds = new MDS((store = store, gather_lock_to_mix = false));
      c1 = new Client((id = 1, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      c2 = new Client((id = 2, mds = mds, store = store, recheck_after_renew = false, invalidate_on_fc_grant = false));
      new AppDriver((client = c1, steps = 10, writer = true, sessions = true));
      new AppDriver((client = c2, steps = 10, writer = true, sessions = true));
    }
  }
}

test tcWriterReader [main=TestWriterReader]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestWriterReader, AppDriver, MDS, Client, Store};

test tcTwoWriters [main=TestTwoWriters]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestTwoWriters, AppDriver, MDS, Client, Store};

test tcTwoWritersFixed [main=TestTwoWritersFixed]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestTwoWritersFixed, AppDriver, MDS, Client, Store};

test tcThreeClients [main=TestThreeClients]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestThreeClients, AppDriver, MDS, Client, Store};

/* the same, with the proposed client fix enabled */
machine TestStaleSessionsRecheck {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      store = new Store();
      mds = new MDS((store = store, gather_lock_to_mix = false));
      c1 = new Client((id = 1, mds = mds, store = store, recheck_after_renew = true, invalidate_on_fc_grant = false));
      c2 = new Client((id = 2, mds = mds, store = store, recheck_after_renew = true, invalidate_on_fc_grant = false));
      new AppDriver((client = c1, steps = 10, writer = true, sessions = true));
      new AppDriver((client = c2, steps = 10, writer = true, sessions = true));
    }
  }
}

/* the same, with both proposed client fixes enabled */
machine TestStaleSessionsFixed {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      store = new Store();
      mds = new MDS((store = store, gather_lock_to_mix = true));
      c1 = new Client((id = 1, mds = mds, store = store, recheck_after_renew = true, invalidate_on_fc_grant = true));
      c2 = new Client((id = 2, mds = mds, store = store, recheck_after_renew = true, invalidate_on_fc_grant = true));
      new AppDriver((client = c1, steps = 10, writer = true, sessions = true));
      new AppDriver((client = c2, steps = 10, writer = true, sessions = true));
    }
  }
}

test tcStaleSessionsFixed [main=TestStaleSessionsFixed]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestStaleSessionsFixed, AppDriver, MDS, Client, Store};

test tcStaleSessionsRecheck [main=TestStaleSessionsRecheck]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestStaleSessionsRecheck, AppDriver, MDS, Client, Store};

test tcStaleSessions [main=TestStaleSessions]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestStaleSessions, AppDriver, MDS, Client, Store};

/*
 * Test drivers: one AppDriver per client issues a random sequence of
 * application operations, pacing itself through its own queue so the
 * checker interleaves it with the MDS, the clients and the store.
 */

type tDriverConfig = (client: machine, steps: int, writer: bool);

event eNext;

machine AppDriver {
  var client: machine;
  var steps: int;
  var writer: bool;

  start state Init {
    entry (cfg: tDriverConfig) {
      client = cfg.client;
      steps = cfg.steps;
      writer = cfg.writer;
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
      k = choose(8);
      if (k == 0) { send client, eAppOpen, PickMode(); }
      else if (k == 1) { send client, eAppClose, PickMode(); }
      else if (k == 2 || k == 3) { send client, eAppRead; }
      else if (k == 4 || k == 5) { if (writer) { send client, eAppWrite; } else { send client, eAppRead; } }
      else if (k == 6) { send client, eAppFlushTick; }
      else { send client, eAppTrim; }
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

/* one writer and one reader */
machine TestWriterReader {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      store = new Store();
      mds = new MDS();
      c1 = new Client((id = 1, mds = mds, store = store));
      c2 = new Client((id = 2, mds = mds, store = store));
      new AppDriver((client = c1, steps = 8, writer = true));
      new AppDriver((client = c2, steps = 8, writer = false));
    }
  }
}

/* two writers: exercises MIX, EXCL hand-over and loner changes */
machine TestTwoWriters {
  start state Init {
    entry {
      var store: machine;
      var mds: machine;
      var c1: machine;
      var c2: machine;
      store = new Store();
      mds = new MDS();
      c1 = new Client((id = 1, mds = mds, store = store));
      c2 = new Client((id = 2, mds = mds, store = store));
      new AppDriver((client = c1, steps = 8, writer = true));
      new AppDriver((client = c2, steps = 8, writer = true));
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
      mds = new MDS();
      c1 = new Client((id = 1, mds = mds, store = store));
      c2 = new Client((id = 2, mds = mds, store = store));
      c3 = new Client((id = 3, mds = mds, store = store));
      new AppDriver((client = c1, steps = 6, writer = true));
      new AppDriver((client = c2, steps = 6, writer = true));
      new AppDriver((client = c3, steps = 6, writer = false));
    }
  }
}

test tcWriterReader [main=TestWriterReader]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestWriterReader, AppDriver, MDS, Client, Store};

test tcTwoWriters [main=TestTwoWriters]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestTwoWriters, AppDriver, MDS, Client, Store};

test tcThreeClients [main=TestThreeClients]:
  assert DataCoherence, CapTracking, IoProgress, LockTransitions in {TestThreeClients, AppDriver, MDS, Client, Store};

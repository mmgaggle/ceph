fun Base(): tCfg {
  var c: tCfg;
  c = (nOsds = 3, crush = default(seq[int]), size = 3, minSize = 2, writes = 2, chaos = 4,
       crash = true, falseDown = true, outs = false, lost = false, minSizeChange = false,
       batchMaps = true, script = default(seq[tStep]),
       upThruGate = true, historyLes = true, staleFilter = true, repopFilter = true,
       getInfoKeepsRequest = false);
  return c;
}

fun Crush(n: int): seq[int] {
  var s: seq[int];
  var i: int;
  while (i < n) {
    s += (i, i);
    i = i + 1;
  }
  return s;
}

machine Main {
  start state Init {
    entry (c: tCfg) {
      var mon: machine;
      var osds: map[int, machine];
      var client: machine;
      var o: int;
      var m1: tMap;
      var env: machine;
      if (sizeof(c.crush) == 0) {
        c.crush = Crush(c.nOsds);
      }
      mon = new Mon(c);
      m1 = InitialMap(c);
      while (o < c.nOsds) {
        osds[o] = new Osd((cfg = c, me = o, m = m1));
        o = o + 1;
      }
      client = new Client();
      env = new Env((cfg = c, mon = mon, osds = osds, client = client));
      send mon, eSetup, (mon = mon, osds = osds, client = client, env = env);
      o = 0;
      while (o < c.nOsds) {
        send osds[o], eSetup, (mon = mon, osds = osds, client = client, env = env);
        o = o + 1;
      }
      send client, eSetup, (mon = mon, osds = osds, client = client, env = env);
    }
  }
}

// the map the monitor starts from (Mon's Init builds the same one)
fun InitialMap(c: tCfg): tMap {
  var m: tMap;
  var o: int;
  m = (epoch = 1, crush = c.crush, up = default(map[int, bool]), inn = default(map[int, bool]),
       upFrom = default(map[int, int]), addr = default(map[int, int]), upThru = default(map[int, int]),
       downAt = default(map[int, int]), lostAt = default(map[int, int]),
       pgTemp = default(seq[int]), size = c.size, minSize = c.minSize);
  while (o < c.nOsds) {
    m.up[o] = true;
    m.inn[o] = true;
    m.upFrom[o] = 1;
    m.addr[o] = 1;
    m.upThru[o] = 1;
    m.downAt[o] = 0;
    m.lostAt[o] = 0;
    o = o + 1;
  }
  return m;
}

// crashes, restarts and false mark-downs on a 3-OSD, size 3 / min_size 2 pool
machine TestCrash3 { start state Init { entry { new Main(Base()); } } }
// the same with min_size 1: intervals of one OSD go read-write
machine TestCrash3Min1 {
  start state Init { entry { var c: tCfg; c = Base(); c.minSize = 1; new Main(c); } }
}
// size 2 / min_size 1 on 3 OSDs, OSDs marked out and in
machine TestRemap {
  start state Init {
    entry { var c: tCfg; c = Base(); c.size = 2; c.minSize = 1; c.outs = true; new Main(c); }
  }
}
// size 2 / min_size 1 on 3 OSDs: many false mark-downs, OSDs out and in
machine TestFlap {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.size = 2; c.minSize = 1; c.outs = true; c.crash = false; c.chaos = 7;
      new Main(c);
    }
  }
}
// a crashed OSD may be marked lost and never return
machine TestLost {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.nOsds = 4; c.size = 2; c.minSize = 1; c.lost = true; c.outs = true; c.chaos = 6;
      new Main(c);
    }
  }
}
// Incomplete after `ceph osd lost` (scripted): size 2 / min_size 1 on
// osd.0 and osd.1. osd.0 is marked down and osd.1 goes active alone; osd.0
// comes back and osd.1 dies before the PG goes active with both; osd.1 is
// marked lost. osd.0's copy is complete for all it knows, but it has
// learned history.last_epoch_started from osd.1's interval and nothing
// alive has that last_epoch_started.
fun LostScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), (op = OP_WAIT_ACTIVE, osd = 0));
  s += (sizeof(s), (op = OP_FALSE_DOWN, osd = 0));
  s += (sizeof(s), (op = OP_WAIT_ACTIVE, osd = 0));
  s += (sizeof(s), (op = OP_WRITE, osd = 0));
  s += (sizeof(s), (op = OP_CRASH, osd = 1));
  s += (sizeof(s), (op = OP_DETECT, osd = 1));
  s += (sizeof(s), (op = OP_LOST, osd = 1));
  return s;
}
machine TestIncompleteAfterLost {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.size = 2; c.minSize = 1; c.script = LostScript();
      new Main(c);
    }
  }
}
machine TestIncompleteAfterLostIgnoreLes {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.size = 2; c.minSize = 1; c.script = LostScript(); c.historyLes = false;
      new Main(c);
    }
  }
}

// A notify from a peer's previous incarnation completes GetInfo without the
// peer (scripted): osd.1 alone acks a write, then crashes and is marked
// out; osd.0 restarts as primary and waits in Down for osd.1. osd.0 lags:
// osd.1 restarts (a stray) and notifies it, is marked down and boots again.
// osd.0 catches up in one batch and queries osd.1 and osd.2; the old notify
// erases osd.1 from peer_info_requested and is then discarded
// (has_been_up_since), so osd.2's reply completes GetInfo.
fun StaleNotifyScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), (op = OP_WAIT_ACTIVE, osd = 0));
  s += (sizeof(s), (op = OP_CRASH, osd = 0));
  s += (sizeof(s), (op = OP_DETECT, osd = 0));
  s += (sizeof(s), (op = OP_WAIT_ACTIVE, osd = 0));
  s += (sizeof(s), (op = OP_WRITE, osd = 0));
  s += (sizeof(s), (op = OP_WAIT_ACKED, osd = 0));
  s += (sizeof(s), (op = OP_CRASH, osd = 1));
  s += (sizeof(s), (op = OP_DETECT, osd = 1));
  s += (sizeof(s), (op = OP_OUT, osd = 1));
  s += (sizeof(s), (op = OP_RESTART, osd = 0));
  s += (sizeof(s), (op = OP_WAIT_UP, osd = 0));
  s += (sizeof(s), (op = OP_HOLD_MAPS, osd = 0));
  s += (sizeof(s), (op = OP_RESTART, osd = 1));
  s += (sizeof(s), (op = OP_WAIT_UP, osd = 1));
  s += (sizeof(s), (op = OP_FALSE_DOWN, osd = 1));
  s += (sizeof(s), (op = OP_WAIT_UP, osd = 1));
  s += (sizeof(s), (op = OP_RELEASE_MAPS, osd = 0));
  return s;
}
machine TestStaleNotify {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.size = 2; c.minSize = 1; c.batchMaps = false; c.script = StaleNotifyScript();
      new Main(c);
    }
  }
}
// The same with size 3 / min_size 2 on 4 OSDs: osd.1 and osd.2 take the
// write while osd.0 is down; osd.2 stays down, osd.1 is the one that
// flaps.
fun StaleNotifyMin2Script(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), (op = OP_WAIT_ACTIVE, osd = 0));
  s += (sizeof(s), (op = OP_CRASH, osd = 0));
  s += (sizeof(s), (op = OP_DETECT, osd = 0));
  s += (sizeof(s), (op = OP_WAIT_ACTIVE, osd = 0));
  s += (sizeof(s), (op = OP_WRITE, osd = 0));
  s += (sizeof(s), (op = OP_WAIT_ACKED, osd = 0));
  s += (sizeof(s), (op = OP_CRASH, osd = 2));
  s += (sizeof(s), (op = OP_DETECT, osd = 2));
  s += (sizeof(s), (op = OP_CRASH, osd = 1));
  s += (sizeof(s), (op = OP_DETECT, osd = 1));
  s += (sizeof(s), (op = OP_OUT, osd = 1));
  s += (sizeof(s), (op = OP_RESTART, osd = 0));
  s += (sizeof(s), (op = OP_WAIT_UP, osd = 0));
  s += (sizeof(s), (op = OP_HOLD_MAPS, osd = 0));
  s += (sizeof(s), (op = OP_RESTART, osd = 1));
  s += (sizeof(s), (op = OP_WAIT_UP, osd = 1));
  s += (sizeof(s), (op = OP_FALSE_DOWN, osd = 1));
  s += (sizeof(s), (op = OP_WAIT_UP, osd = 1));
  s += (sizeof(s), (op = OP_RELEASE_MAPS, osd = 0));
  return s;
}
machine TestStaleNotifyMin2 {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.nOsds = 4; c.batchMaps = false; c.script = StaleNotifyMin2Script();
      new Main(c);
    }
  }
}
machine TestStaleNotifyFixed {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.size = 2; c.minSize = 1; c.batchMaps = false; c.script = StaleNotifyScript();
      c.getInfoKeepsRequest = true;
      new Main(c);
    }
  }
}
machine TestFlapFixed {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.size = 2; c.minSize = 1; c.outs = true; c.crash = false; c.chaos = 7;
      c.getInfoKeepsRequest = true;
      new Main(c);
    }
  }
}

// min_size changes
machine TestMinSize {
  start state Init { entry { var c: tCfg; c = Base(); c.minSizeChange = true; new Main(c); } }
}

// a primary goes active without waiting for up_thru
machine TestNoUpThruGate {
  start state Init {
    entry { var c: tCfg; c = Base(); c.minSize = 1; c.upThruGate = false; new Main(c); }
  }
}
// ... with osd_find_best_info_ignore_history_les = true
machine TestLostIgnoreHistoryLes {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.nOsds = 4; c.size = 2; c.minSize = 1; c.lost = true; c.outs = true; c.chaos = 6;
      c.historyLes = false;
      new Main(c);
    }
  }
}
// no old_peering_msg filter
machine TestNoStaleFilter {
  start state Init { entry { var c: tCfg; c = Base(); c.minSize = 1; c.staleFilter = false; new Main(c); } }
}
// replicas apply repops from an old interval
machine TestNoRepopFilter {
  start state Init { entry { var c: tCfg; c = Base(); c.minSize = 1; c.repopFilter = false; new Main(c); } }
}

fun Base(): tCfg {
  var c: tCfg;
  c = (nOsds = 3, crush = default(seq[int]), size = 2, minSize = 1, nObjects = 2, maxLog = 1,
       maxBackfills = 1, maxRecoveryOps = 1, asyncMinCost = 100, writes = 2, chaos = 3,
       crash = true, falseDown = true, outs = false, lost = false, minSizeChange = false,
       batchMaps = true, full = false, preempt = false, commands = false,
       maxPgs = 0, others = 0, pgChurn = false, partitions = false, minReporters = 2, maxMarkdowns = 5,
       keepCuts = false,
       script = default(seq[tStep]),
       upThruGate = true, historyLes = true, staleFilter = true,
       getInfoKeepsRequest = false, advMapFullCheck = false, resetIgnoresCommands = false,
       grantFromCheck = false, deferWhileWaiting = false, pendingKeepsUp = false, twiddleFix = false,
       slotCheck = true);
  return c;
}

// the proposed fixes for the issues found so far: the random configurations
// run with them, so a failure there is something new
fun Fixed(c: tCfg): tCfg {
  c.resetIgnoresCommands = true;
  c.grantFromCheck = true;
  c.deferWhileWaiting = true;
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
      client = new Client(c);
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
       downAt = default(map[int, int]), lostAt = default(map[int, int]), fullOsds = default(set[int]),
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

fun Step(op: tOp, o: int): tStep {
  return (op = op, osd = o, st = S_NONE, peer = -1);
}
// wait until osd.o's PG enters state st
fun WaitState(o: int, st: tS): tStep {
  return (op = OP_WAIT_STATE, osd = o, st = st, peer = -1);
}

// crashes, restarts and false mark-downs: log recovery, and backfill once
// the log is trimmed past a peer
machine TestCrash { start state Init { entry { new Main(Fixed(Base())); } } }
// size 3 / min_size 2
machine TestCrash3 {
  start state Init { entry { var c: tCfg; c = Base(); c.size = 3; c.minSize = 2; new Main(Fixed(c)); } }
}
// OSDs marked out and in: the PG moves, and backfills its new OSDs
machine TestRemap {
  start state Init {
    entry { var c: tCfg; c = Base(); c.outs = true; c.crash = false; c.chaos = 4; new Main(Fixed(c)); }
  }
}
// OSDs go (backfill)full and back
machine TestFull {
  start state Init {
    entry { var c: tCfg; c = Base(); c.outs = true; c.full = true; c.crash = false; c.chaos = 5; new Main(Fixed(c)); }
  }
}
// other PGs' reservations preempt ours
machine TestPreempt {
  start state Init {
    entry { var c: tCfg; c = Base(); c.outs = true; c.preempt = true; c.crash = false; c.chaos = 5; new Main(Fixed(c)); }
  }
}
// async recovery: a peer behind by any write recovers outside the acting set
machine TestAsync {
  start state Init {
    entry { var c: tCfg; c = Base(); c.size = 3; c.minSize = 1; c.asyncMinCost = 0; c.maxLog = 4; new Main(Fixed(c)); }
  }
}
// the mgr's force-recovery and scrub requests
machine TestCommands {
  start state Init {
    entry { var c: tCfg; c = Base(); c.commands = true; c.chaos = 4; new Main(c); }
  }
}
// a crashed OSD may be marked lost and never return
machine TestLost {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.nOsds = 4; c.lost = true; c.outs = true; c.chaos = 5;
      new Main(Fixed(c));
    }
  }
}
// ------------------------------------------------ scripted setups, then chaos

// size 2 / min_size 1 on 3 OSDs: two writes (the log trims to one entry),
// then osd.1 is marked out; empty osd.2 is backfilled while osd.1 stays
// in the acting set by pg_temp
fun BackfillSetup(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_OUT, 1));
  return s;
}
// size 2 / min_size 1 on 3 OSDs: a write, then osd.1 fails and is marked
// out; osd.2 takes its place and recovers the object from the log
fun RecoverySetup(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_CRASH, 1));
  s += (sizeof(s), Step(OP_DETECT, 1));
  s += (sizeof(s), Step(OP_OUT, 1));
  return s;
}
// size 3 / min_size 1 on 3 OSDs: osd.1 and osd.2 are down for two writes
// (the log trims past them), then return as two backfill targets
fun TwoBackfillSetup(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_CRASH, 1));
  s += (sizeof(s), Step(OP_DETECT, 1));
  s += (sizeof(s), Step(OP_CRASH, 2));
  s += (sizeof(s), Step(OP_DETECT, 2));
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_RESTART, 1));
  s += (sizeof(s), Step(OP_RESTART, 2));
  return s;
}

fun Scenario(setup: seq[tStep], what: int): tCfg {
  var c: tCfg;
  c = Fixed(Base());
  c.script = setup;
  c.chaos = 3;
  c.writes = 1;
  c.crash = false;
  c.falseDown = false;
  if (what == 0) {
    c.full = true;
  } else if (what == 1) {
    c.preempt = true;
  } else {
    c.crash = true;
    c.falseDown = true;
    c.chaos = 2;
  }
  return c;
}

machine TestBfFull { start state Init { entry { new Main(Scenario(BackfillSetup(), 0)); } } }
machine TestBfPreempt { start state Init { entry { new Main(Scenario(BackfillSetup(), 1)); } } }
machine TestBfCrash { start state Init { entry { new Main(Scenario(BackfillSetup(), 2)); } } }
machine TestRecFull { start state Init { entry { new Main(Scenario(RecoverySetup(), 0)); } } }
machine TestRecPreempt { start state Init { entry { new Main(Scenario(RecoverySetup(), 1)); } } }
machine TestRecCrash { start state Init { entry { new Main(Scenario(RecoverySetup(), 2)); } } }
machine TestBf2Full {
  start state Init { entry { var c: tCfg; c = Scenario(TwoBackfillSetup(), 0); c.size = 3; new Main(c); } }
}
machine TestBf2Preempt {
  start state Init { entry { var c: tCfg; c = Scenario(TwoBackfillSetup(), 1); c.size = 3; new Main(c); } }
}

// The mgr's force-recovery reaches a primary that has restarted and not yet
// seen a new map: its PGs are still in Reset (scripted)
fun CommandInResetScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_CRASH, 0));
  s += (sizeof(s), Step(OP_HOLD_MAPS, 0));
  s += (sizeof(s), Step(OP_RESTART, 0));
  s += (sizeof(s), Step(OP_FORCE, 0));
  s += (sizeof(s), Step(OP_RELEASE_MAPS, 0));
  return s;
}
machine TestCmdReset {
  start state Init {
    entry { var c: tCfg; c = Base(); c.chaos = 0; c.writes = 0; c.script = CommandInResetScript(); new Main(c); }
  }
}
machine TestFixCmdReset {
  start state Init {
    entry {
      var c: tCfg;
      c = Base(); c.chaos = 0; c.writes = 0; c.script = CommandInResetScript(); c.resetIgnoresCommands = true;
      new Main(c);
    }
  }
}
machine TestFixCommands {
  start state Init {
    entry { var c: tCfg; c = Base(); c.commands = true; c.chaos = 4; c.resetIgnoresCommands = true; new Main(c); }
  }
}

// ------------------------------------------------ reservations and full OSDs

// The primary's local slot is busy with another PG's work when recovery
// starts, so the PG waits in WaitLocalRecoveryReserved; then the new
// recovery target (osd.2) is marked full.
fun RecWaitLocalFullScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_BUSY, 0));
  s += (sizeof(s), Step(OP_CRASH, 1));
  s += (sizeof(s), Step(OP_DETECT, 1));
  s += (sizeof(s), Step(OP_OUT, 1));
  s += (sizeof(s), WaitState(0, S_WaitLocalRecoveryReserved));
  s += (sizeof(s), Step(OP_FULL, 2));
  return s;
}
// ... osd.2's remote slot is busy, so the PG waits in WaitRemoteRecoveryReserved
fun RecWaitRemoteFullScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_BUSY, 2));
  s += (sizeof(s), Step(OP_CRASH, 1));
  s += (sizeof(s), Step(OP_DETECT, 1));
  s += (sizeof(s), Step(OP_OUT, 1));
  s += (sizeof(s), WaitState(0, S_WaitRemoteRecoveryReserved));
  s += (sizeof(s), Step(OP_FULL, 2));
  return s;
}
fun Scripted(script: seq[tStep]): tCfg {
  var c: tCfg;
  c = Base();
  c.script = script;
  c.chaos = 0;
  c.writes = 0;
  return c;
}
machine TestRecWaitLocalFull { start state Init { entry { new Main(Scripted(RecWaitLocalFullScript())); } } }
machine TestRecWaitRemoteFull { start state Init { entry { new Main(Scripted(RecWaitRemoteFullScript())); } } }
machine TestFixRecWaitLocalFull {
  start state Init {
    entry { var c: tCfg; c = Scripted(RecWaitLocalFullScript()); c.advMapFullCheck = true; new Main(c); }
  }
}
machine TestFixRecWaitRemoteFull {
  start state Init {
    entry { var c: tCfg; c = Scripted(RecWaitRemoteFullScript()); c.advMapFullCheck = true; new Main(c); }
  }
}

// Two backfill targets: osd.2's remote slot is busy, so osd.1 grants and
// osd.2's request waits; osd.1's reservation is then preempted (REVOKE), the
// primary retries, and osd.2's slot frees up.
fun Bf2StaleGrantScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_CRASH, 1));
  s += (sizeof(s), Step(OP_DETECT, 1));
  s += (sizeof(s), Step(OP_CRASH, 2));
  s += (sizeof(s), Step(OP_DETECT, 2));
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_RESTART, 1));
  s += (sizeof(s), Step(OP_RESTART, 2));
  s += (sizeof(s), Step(OP_WAIT_UP, 2));
  s += (sizeof(s), Step(OP_BUSY, 2));
  s += (sizeof(s), WaitState(1, S_RepRecovering));
  s += (sizeof(s), Step(OP_PREEMPT, 1));
  s += (sizeof(s), Step(OP_IDLE, 2));
  return s;
}
machine TestBf2StaleGrant {
  start state Init { entry { var c: tCfg; c = Scripted(Bf2StaleGrantScript()); c.size = 3; new Main(c); } }
}

// ------------------------------------------------ preemption while waiting

// osd.2's remote slot is busy, so the primary waits in
// WaitRemoteRecoveryReserved holding its local slot; a higher-priority
// reservation preempts that local slot; osd.2's slot then frees up.
fun RecSlotLostScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_BUSY, 2));
  s += (sizeof(s), Step(OP_CRASH, 1));
  s += (sizeof(s), Step(OP_DETECT, 1));
  s += (sizeof(s), Step(OP_OUT, 1));
  s += (sizeof(s), WaitState(0, S_WaitRemoteRecoveryReserved));
  s += (sizeof(s), Step(OP_PREEMPT, 0));
  s += (sizeof(s), Step(OP_IDLE, 2));
  return s;
}
// the same for backfill: osd.2 is backfilled while osd.1 stays in acting
fun BfSlotLostScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_BUSY, 2));
  s += (sizeof(s), Step(OP_OUT, 1));
  s += (sizeof(s), WaitState(0, S_WaitRemoteBackfillReserved));
  s += (sizeof(s), Step(OP_PREEMPT, 0));
  s += (sizeof(s), Step(OP_IDLE, 2));
  return s;
}
machine TestRecSlotLost { start state Init { entry { new Main(Scripted(RecSlotLostScript())); } } }
machine TestBfSlotLost { start state Init { entry { new Main(Scripted(BfSlotLostScript())); } } }

// the proposed fixes
machine TestFixBf2StaleGrant {
  start state Init {
    entry { var c: tCfg; c = Scripted(Bf2StaleGrantScript()); c.size = 3; c.grantFromCheck = true; new Main(c); }
  }
}
// giving up on a preemption while waiting releases grants still on their way
machine TestHalfFixRecSlotLost {
  start state Init {
    entry { var c: tCfg; c = Scripted(RecSlotLostScript()); c.deferWhileWaiting = true; new Main(c); }
  }
}
machine TestHalfFixBfSlotLost {
  start state Init {
    entry { var c: tCfg; c = Scripted(BfSlotLostScript()); c.deferWhileWaiting = true; new Main(c); }
  }
}
machine TestFixRecSlotLost {
  start state Init {
    entry {
      var c: tCfg;
      c = Scripted(RecSlotLostScript()); c.deferWhileWaiting = true; c.grantFromCheck = true;
      new Main(c);
    }
  }
}
machine TestFixBfSlotLost {
  start state Init {
    entry {
      var c: tCfg;
      c = Scripted(BfSlotLostScript()); c.deferWhileWaiting = true; c.grantFromCheck = true;
      new Main(c);
    }
  }
}
// both fixes, random preemption and full OSDs during recovery
machine TestFixRecChaos {
  start state Init {
    entry {
      var c: tCfg;
      c = Scenario(RecoverySetup(), 1); c.full = true; c.chaos = 4;
      c.grantFromCheck = true; c.deferWhileWaiting = true;
      new Main(c);
    }
  }
}
// both, with random preemption and full OSDs over two backfill targets
machine TestFixBf2Chaos {
  start state Init {
    entry {
      var c: tCfg;
      c = Scenario(TwoBackfillSetup(), 1); c.size = 3; c.full = true; c.chaos = 4;
      c.grantFromCheck = true; c.deferWhileWaiting = true;
      new Main(c);
    }
  }
}

// ------------------------------------------------ deletion

// osd.1 is marked out; once the PG is clean on osd.0 and osd.2 the primary
// purges it. osd.1's local slot is busy, so its PG waits in
// WaitDeleteReserved; the slot frees up (the grant is queued) and osd.1 goes
// full, which changes the delete priority at the next ActMap.
fun DeleteRePriorityScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_BUSY, 1));
  s += (sizeof(s), Step(OP_OUT, 1));
  s += (sizeof(s), WaitState(1, S_WaitDeleteReserved));
  s += (sizeof(s), Step(OP_IDLE, 1));
  s += (sizeof(s), Step(OP_FULL, 1));
  return s;
}
machine TestDeleteRePriority { start state Init { entry { new Main(Scripted(DeleteRePriorityScript())); } } }
machine TestFixDeleteRePriority {
  start state Init {
    entry { var c: tCfg; c = Scripted(DeleteRePriorityScript()); c.grantFromCheck = true; new Main(c); }
  }
}

// ------------------------------------------------ PGs per OSD

// As BackfillSetup, but osd.2 already holds the most PGs it may: when osd.1
// is marked out, the primary activates with osd.2 as its backfill target and
// osd.2 withholds creating the PG. Then osd.2 has room again.
fun MaxPgBackfillScript(mapBetween: bool): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_PG_ADD, 2));
  s += (sizeof(s), Step(OP_PG_ADD, 2));
  s += (sizeof(s), Step(OP_OUT, 1));
  s += (sizeof(s), WaitState(0, S_Activating));
  if (mapBetween) {
    // any new epoch (here osd.1 crossing a full ratio and back)
    s += (sizeof(s), Step(OP_FULL, 1));
    s += (sizeof(s), Step(OP_NOT_FULL, 1));
  }
  s += (sizeof(s), Step(OP_PG_DEL, 2));
  return s;
}
fun MaxPg(c: tCfg): tCfg {
  c.maxPgs = 2;
  return c;
}
machine TestMaxPgResume {
  start state Init { entry { new Main(MaxPg(Scripted(MaxPgBackfillScript(false)))); } }
}
machine TestMaxPgBackfillTarget {
  start state Init { entry { new Main(MaxPg(Scripted(MaxPgBackfillScript(true)))); } }
}
machine TestFixMaxPgBackfillTarget {
  start state Init {
    entry { var c: tCfg; c = MaxPg(Scripted(MaxPgBackfillScript(true))); c.pendingKeepsUp = true; new Main(c); }
  }
}
// ------------------------------------------------ the cluster network

fun CutStep(op: tOp, a: int, b: int): tStep {
  return (op = op, osd = a, st = S_NONE, peer = b);
}
// links fail and heal at random, with crashes
machine TestCuts {
  start state Init {
    entry { var c: tCfg; c = Fixed(Base()); c.partitions = true; c.chaos = 4; new Main(c); }
  }
}
// ... with mon_osd_min_down_reporters 1
machine TestCuts1 {
  start state Init {
    entry {
      var c: tCfg;
      c = Fixed(Base()); c.partitions = true; c.chaos = 4; c.minReporters = 1; c.maxMarkdowns = 1;
      new Main(c);
    }
  }
}

// The cluster link between the two acting OSDs fails and stays down; a
// client writes, and min_size goes to 2, so the PG peers again across the
// dead link. Each OSD's heartbeats to the other fail, and each reports the
// other.
fun CutPeeringScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), CutStep(OP_CUT, 0, 1));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_MIN_SIZE, 2));
  return s;
}
machine TestCutPersist2 {
  start state Init {
    entry { var c: tCfg; c = Fixed(Scripted(CutPeeringScript())); c.keepCuts = true; new Main(c); }
  }
}
machine TestCutPersist1 {
  start state Init {
    entry {
      var c: tCfg;
      c = Fixed(Scripted(CutPeeringScript())); c.keepCuts = true; c.minReporters = 1; c.maxMarkdowns = 2;
      new Main(c);
    }
  }
}

// osd.1 fails and is marked lost: osd.0 alone has the PG, and backfills
// osd.2, which holds as many PGs as it may. osd.2 withholds creating the PG;
// then it has room, before any new map, and twiddles the acting set [0].
fun MaxPgSingleScript(): seq[tStep] {
  var s: seq[tStep];
  s += (sizeof(s), Step(OP_WAIT_ACTIVE, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_WRITE, 0));
  s += (sizeof(s), Step(OP_WAIT_ACKED, 0));
  s += (sizeof(s), Step(OP_PG_ADD, 2));
  s += (sizeof(s), Step(OP_PG_ADD, 2));
  s += (sizeof(s), Step(OP_CRASH, 1));
  s += (sizeof(s), Step(OP_DETECT, 1));
  s += (sizeof(s), Step(OP_LOST, 1));
  s += (sizeof(s), Step(OP_OUT, 1));
  s += (sizeof(s), WaitState(0, S_Activating));
  s += (sizeof(s), Step(OP_PG_DEL, 2));
  return s;
}
machine TestMaxPgSingle {
  start state Init {
    entry { var c: tCfg; c = MaxPg(Scripted(MaxPgSingleScript())); c.pendingKeepsUp = true; new Main(c); }
  }
}
machine TestFixMaxPgSingle {
  start state Init {
    entry {
      var c: tCfg;
      c = MaxPg(Scripted(MaxPgSingleScript())); c.pendingKeepsUp = true; c.twiddleFix = true;
      new Main(c);
    }
  }
}
// both fixes, other PGs coming and going
machine TestFixMaxPgChurn {
  start state Init {
    entry {
      var c: tCfg;
      c = Fixed(Base()); c.maxPgs = 2; c.others = 1; c.pgChurn = true; c.outs = true; c.crash = false;
      c.chaos = 5; c.pendingKeepsUp = true; c.twiddleFix = true;
      new Main(c);
    }
  }
}

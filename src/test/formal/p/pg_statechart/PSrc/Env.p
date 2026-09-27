/*
 * The environment: `chaos` failure events and `writes` client writes in a
 * random order, then the cluster settles. Between steps the protocol
 * runs (each step is its own event).
 *
 * Failure events:
 *   crash      an OSD process dies; its disk survives
 *   detect     the mon marks a crashed OSD down (failure detection is late)
 *   restart    a crashed OSD boots again
 *   falseDown  the mon marks a running OSD down; it notices and boots again
 *   out / in   CRUSH stops or starts mapping the PG to an OSD
 *   lost       an operator marks a crashed, down OSD lost; it never returns
 *   minSize    an operator changes the pool's min_size
 *   full       an OSD crosses the backfillfull ratio, or drops below it;
 *              the mon marks it full in the map a little later
 *   preempt    a higher-priority reservation of another PG takes an OSD's
 *              local and remote slots, then gives them back
 *   command    the mgr sends an OSD force-recovery or scrub for the PG
 *   pgs        another PG is created on or removed from an OSD
 *   link       a cluster-network link between two OSDs fails (both ways
 *              or one way) or heals; an OSD shut down for being marked
 *              down too often counts as crashed
 *
 * Settling: every crashed OSD not marked lost is marked down if it still
 * shows up, and restarts; every lost OSD is marked out; no OSD is full;
 * every link heals; every OSD has room for the PG.
 * From then on the PG must become active (and, without lost OSDs, clean)
 * and every write must be acked.
 *
 * A scripted run (tCfg.script) takes its steps in order first; a wait
 * step lasts until what it waits for (a primary active, or the PG clean,
 * in an interval newer than any seen before; an OSD up; every scripted
 * write acked). Then any `chaos` and `writes` follow in random order.
 */
machine Env {
  var cfg: tCfg;
  var mon: machine;
  var osds: map[int, machine];
  var client: machine;
  var crashed: set[int];
  var detected: set[int];      // crashed and marked down
  var lost: set[int];
  var outs: set[int];
  var chaosLeft: int;
  var writesLeft: int;
  var nextWid: int;
  var step: int;
  var seenSis: int;
  var waiting: tOp;
  var waitOsd: int;
  var isWaiting: bool;
  var ups: seq[int];           // OSDs the mon marked up, in order (scripted runs)
  var unacked: set[int];
  var fulls: set[int];
  var seenClean: int;
  var others: map[int, int];   // other PGs on each OSD
  var cuts: set[(a: int, b: int)];   // cluster links down, a -> b
  var settling: bool;
  var waitState: tS;

  start state Init {
    entry (p: (cfg: tCfg, mon: machine, osds: map[int, machine], client: machine)) {
      var o: int;
      cfg = p.cfg;
      mon = p.mon;
      osds = p.osds;
      client = p.client;
      chaosLeft = cfg.chaos;
      o = 0;
      while (o < cfg.nOsds) {
        others[o] = cfg.others;
        o = o + 1;
      }
      writesLeft = cfg.writes;
      nextWid = 1;
      send this, eStep;
      goto Run;
    }
  }

  state Run {
    on eNoteActive do (sis: int) {
      if (sis > seenSis) {
        seenSis = sis;
        if (isWaiting && waiting == OP_WAIT_ACTIVE) {
          isWaiting = false;
          send this, eStep;
        }
      }
    }
    on eNoteClean do (sis: int) {
      if (sis > seenClean) {
        seenClean = sis;
        if (isWaiting && waiting == OP_WAIT_CLEAN) {
          isWaiting = false;
          send this, eStep;
        }
      }
    }
    on eNoteState do (n: (osd: int, st: tS)) {
      if (isWaiting && waiting == OP_WAIT_STATE && n.osd == waitOsd && n.st == waitState) {
        isWaiting = false;
        send this, eStep;
      }
    }
    on eNoteExit do (o: int) {
      // shut down for good: the operator restarts it when settling (or,
      // once settling has begun, now), unless the network stays broken
      if (settling) {
        if (!cfg.keepCuts) {
          send osds[o], eRestart;
        } else {
          // it stays down: mon_osd_down_out_interval marks it out
          send mon, eEnvOut, (osd = o, out = true);
        }
      } else {
        crashed += (o);
        detected += (o);
      }
    }
    on eNoteUp do (o: int) {
      ups += (sizeof(ups), o);
      if (isWaiting && waiting == OP_WAIT_UP && o == waitOsd) {
        isWaiting = false;
        send this, eStep;
      }
    }
    on eNoteAcked do (w: int) {
      unacked -= (w);
      if (isWaiting && waiting == OP_WAIT_ACKED && sizeof(unacked) == 0) {
        isWaiting = false;
        send this, eStep;
      }
    }
    on eStep do {
      var total: int;
      if (step < sizeof(cfg.script)) {
        Scripted();
        return;
      }
      total = chaosLeft + writesLeft;
      if (total == 0) {
        Settle();
        goto Done;
      }
      if (choose(total) < writesLeft) {
        writesLeft = writesLeft - 1;
        announce mIssued, nextWid;
        send client, eIssueWrite, nextWid;
        nextWid = nextWid + 1;
      } else {
        chaosLeft = chaosLeft - 1;
        Chaos();
      }
      send this, eStep;
    }
  }

  state Done {
    ignore eStep, eNoteActive, eNoteUp, eNoteAcked, eNoteClean, eNoteState;
    on eNoteExit do (o: int) {
      if (!cfg.keepCuts) {
        send osds[o], eRestart;
      } else {
        send mon, eEnvOut, (osd = o, out = true);
      }
    }
  }

  // one step of the script
  fun Scripted() {
    var s: tStep;
    var o: int;
    s = cfg.script[step];
    step = step + 1;
    if (s.op == OP_WAIT_ACTIVE || s.op == OP_WAIT_UP || s.op == OP_WAIT_ACKED || s.op == OP_WAIT_CLEAN ||
        s.op == OP_WAIT_STATE) {
      if (s.op == OP_WAIT_ACKED && sizeof(unacked) == 0) {
        send this, eStep;
        return;
      }
      waiting = s.op;
      waitOsd = s.osd;
      waitState = s.st;
      isWaiting = true;
      return;
    }
    if (s.op == OP_HOLD_MAPS || s.op == OP_RELEASE_MAPS) {
      send mon, eHoldMaps, (osd = s.osd, hold = s.op == OP_HOLD_MAPS);
      send this, eStep;
      return;
    }
    if (s.op == OP_CRASH) {
      crashed += (s.osd);
      send osds[s.osd], eCrash;
    } else if (s.op == OP_DETECT) {
      detected += (s.osd);
      send mon, eEnvDown, DownSet(s.osd);
    } else if (s.op == OP_RESTART) {
      crashed -= (s.osd);
      detected -= (s.osd);
      send osds[s.osd], eRestart;
    } else if (s.op == OP_FALSE_DOWN) {
      send mon, eEnvDown, DownSet(s.osd);
    } else if (s.op == OP_OUT) {
      outs += (s.osd);
      send mon, eEnvOut, (osd = s.osd, out = true);
    } else if (s.op == OP_IN) {
      outs -= (s.osd);
      send mon, eEnvOut, (osd = s.osd, out = false);
    } else if (s.op == OP_LOST) {
      crashed -= (s.osd);
      detected -= (s.osd);
      lost += (s.osd);
      send mon, eEnvLost, s.osd;
    } else if (s.op == OP_MIN_SIZE) {
      send mon, eEnvMinSize, s.osd;
    } else if (s.op == OP_FULL || s.op == OP_NOT_FULL) {
      SetFull(s.osd, s.op == OP_FULL);
    } else if (s.op == OP_PREEMPT) {
      send osds[s.osd], ePreempt;
    } else if (s.op == OP_FORCE) {
      send osds[s.osd], eCommand, E_SetForceRecovery;
    } else if (s.op == OP_SCRUB) {
      send osds[s.osd], eCommand, E_RequestScrub;
    } else if (s.op == OP_BUSY || s.op == OP_IDLE) {
      send osds[s.osd], eBusy, s.op == OP_BUSY;
    } else if (s.op == OP_PG_ADD || s.op == OP_PG_DEL) {
      Others(s.osd, s.op == OP_PG_ADD);
    } else if (s.op == OP_CUT) {
      Cut(s.osd, s.peer, true);
      Cut(s.peer, s.osd, true);
    } else if (s.op == OP_CUT1) {
      Cut(s.osd, s.peer, true);
    } else if (s.op == OP_HEAL) {
      Cut(s.osd, s.peer, false);
      Cut(s.peer, s.osd, false);
    } else if (s.op == OP_ISOLATE) {
      o = 0;
      while (o < cfg.nOsds) {
        if (o != s.osd) {
          Cut(s.osd, o, true);
          Cut(o, s.osd, true);
        }
        o = o + 1;
      }
    } else {
      announce mIssued, nextWid;
      unacked += (nextWid);
      send client, eIssueWrite, nextWid;
      nextWid = nextWid + 1;
    }
    send this, eStep;
  }

  fun Running(): set[int] {
    var r: set[int];
    var o: int;
    while (o < cfg.nOsds) {
      if (!(o in crashed) && !(o in lost)) {
        r += (o);
      }
      o = o + 1;
    }
    return r;
  }

  fun Chaos() {
    var acts: seq[int];
    var r: set[int];
    var o: int;
    var p: int;
    var a: int;
    r = Running();
    // keep a majority of the PG's copies' worth of OSDs available to settle on
    if (cfg.crash && sizeof(r) > 1) { acts += (sizeof(acts), 0); }
    if (sizeof(crashed) > sizeof(detected)) { acts += (sizeof(acts), 1); }
    if (sizeof(crashed) > 0) { acts += (sizeof(acts), 2); }
    if (cfg.falseDown && sizeof(r) > 0) { acts += (sizeof(acts), 3); }
    if (cfg.outs && cfg.nOsds - sizeof(outs) - sizeof(lost) > cfg.size) { acts += (sizeof(acts), 4); }
    if (cfg.outs && sizeof(outs) > 0) { acts += (sizeof(acts), 5); }
    if (cfg.lost && sizeof(detected) > 0 && cfg.nOsds - sizeof(outs) - sizeof(lost) > cfg.size) { acts += (sizeof(acts), 6); }
    if (cfg.minSizeChange) { acts += (sizeof(acts), 7); }
    if (cfg.full) { acts += (sizeof(acts), 8); }
    if (cfg.preempt && sizeof(r) > 0) { acts += (sizeof(acts), 9); }
    if (cfg.commands && sizeof(r) > 0) { acts += (sizeof(acts), 10); }
    if (cfg.pgChurn) { acts += (sizeof(acts), 11); }
    if (cfg.partitions && cfg.nOsds > 1) { acts += (sizeof(acts), 12); }
    if (sizeof(acts) == 0) {
      return;
    }
    a = acts[choose(sizeof(acts))];
    if (a == 0) {
      o = choose(r);
      crashed += (o);
      send osds[o], eCrash;
    } else if (a == 1) {
      o = choose(crashed);
      while (o in detected) {
        o = choose(crashed);
      }
      detected += (o);
      send mon, eEnvDown, DownSet(o);
    } else if (a == 2) {
      o = choose(crashed);
      crashed -= (o);
      detected -= (o);
      send osds[o], eRestart;
    } else if (a == 3) {
      send mon, eEnvDown, DownSet(choose(r));
    } else if (a == 4) {
      o = choose(cfg.nOsds);
      while (o in outs || o in lost) {
        o = choose(cfg.nOsds);
      }
      outs += (o);
      send mon, eEnvOut, (osd = o, out = true);
    } else if (a == 5) {
      o = choose(outs);
      outs -= (o);
      send mon, eEnvOut, (osd = o, out = false);
    } else if (a == 6) {
      o = choose(detected);
      crashed -= (o);
      detected -= (o);
      lost += (o);
      send mon, eEnvLost, o;
    } else if (a == 7) {
      send mon, eEnvMinSize, 1 + choose(cfg.size);
    } else if (a == 8) {
      o = choose(cfg.nOsds);
      SetFull(o, !(o in fulls));
    } else if (a == 9) {
      send osds[choose(r)], ePreempt;
    } else if (a == 11) {
      o = choose(cfg.nOsds);
      Others(o, $);
    } else if (a == 12) {
      o = choose(cfg.nOsds);
      p = choose(cfg.nOsds);
      while (p == o) {
        p = choose(cfg.nOsds);
      }
      if ((a = o, b = p) in cuts || (a = p, b = o) in cuts) {
        Cut(o, p, false);
        Cut(p, o, false);
      } else {
        Cut(o, p, true);
        if ($) {
          Cut(p, o, true);
        }
      }
    } else {
      if ($) {
        send osds[choose(r)], eCommand, E_SetForceRecovery;
      } else {
        send osds[choose(r)], eCommand, E_RequestScrub;
      }
    }
  }

  fun Others(o: int, add: bool) {
    if (add) {
      others[o] = others[o] + 1;
      send osds[o], eOthers, 1;
    } else if (others[o] > 0) {
      others[o] = others[o] - 1;
      send osds[o], eOthers, -1;
    }
  }

  // the link a -> b fails or heals; both ends learn what they can reach
  fun Cut(a: int, b: int, down: bool) {
    if (down) {
      cuts += ((a = a, b = b));
    } else {
      cuts -= ((a = a, b = b));
    }
    send osds[a], eLink, (peer = b, outCut = (a = a, b = b) in cuts, inCut = (a = b, b = a) in cuts);
    send osds[b], eLink, (peer = a, outCut = (a = b, b = a) in cuts, inCut = (a = a, b = b) in cuts);
  }

  // the OSD knows at once (statfs); the mon marks it in a later map
  fun SetFull(o: int, f: bool) {
    if (f) {
      fulls += (o);
    } else {
      fulls -= (o);
    }
    send osds[o], eFull, f;
    send mon, eEnvFull, (osd = o, full = f);
  }

  // the mon may learn of several failures in one epoch
  fun DownSet(o: int): set[int] {
    var c: int;
    var s: set[int];
    s += (o);
    foreach (c in crashed) {
      if (!(c in detected) && $) {
        detected += (c);
        s += (c);
      }
    }
    return s;
  }

  fun Settle() {
    var o: int;
    var fs: set[int];
    var cs: set[(a: int, b: int)];
    var c: (a: int, b: int);
    var undetected: set[int];
    settling = true;
    foreach (o in crashed) {
      if (!(o in detected)) {
        undetected += (o);
      }
    }
    if (sizeof(undetected) > 0) {
      send mon, eEnvDown, undetected;
    }
    foreach (o in crashed) {
      send osds[o], eRestart;
    }
    foreach (o in lost) {
      if (!(o in outs)) {
        send mon, eEnvOut, (osd = o, out = true);
      }
    }
    fs = fulls;
    foreach (o in fs) {
      SetFull(o, false);
    }
    cs = cuts;
    foreach (c in cs) {
      if (!cfg.keepCuts) {
        Cut(c.a, c.b, false);
      }
    }
    o = 0;
    while (cfg.maxPgs > 0 && o < cfg.nOsds) {
      while (others[o] >= cfg.maxPgs) {
        Others(o, false);
      }
      o = o + 1;
    }
    o = 0;
    while (o < cfg.nOsds) {
      send osds[o], eBusy, false;
      o = o + 1;
    }
    crashed = default(set[int]);
    detected = default(set[int]);
    announce mSettled;
  }
}

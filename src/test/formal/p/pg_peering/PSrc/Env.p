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
 *
 * Settling: every crashed OSD not marked lost is marked down if it still
 * shows up, and restarts; every lost OSD is marked out. From then on the
 * PG must become active and every write must be acked.
 *
 * A scripted run (tCfg.script) takes its steps in order instead; a
 * wait step lasts until a primary goes active in an interval newer than
 * any seen before.
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

  start state Init {
    entry (p: (cfg: tCfg, mon: machine, osds: map[int, machine], client: machine)) {
      cfg = p.cfg;
      mon = p.mon;
      osds = p.osds;
      client = p.client;
      chaosLeft = cfg.chaos;
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
      if (sizeof(cfg.script) > 0) {
        if (Scripted()) {
          goto Done;
        }
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
    ignore eStep, eNoteActive, eNoteUp, eNoteAcked;
  }

  // one step of the script; true once it has run out and the cluster settled
  fun Scripted(): bool {
    var s: tStep;
    if (step == sizeof(cfg.script)) {
      Settle();
      return true;
    }
    s = cfg.script[step];
    step = step + 1;
    if (s.op == OP_WAIT_ACTIVE || s.op == OP_WAIT_UP || s.op == OP_WAIT_ACKED) {
      if (s.op == OP_WAIT_ACKED && sizeof(unacked) == 0) {
        send this, eStep;
        return false;
      }
      waiting = s.op;
      waitOsd = s.osd;
      isWaiting = true;
      return false;
    }
    if (s.op == OP_HOLD_MAPS || s.op == OP_RELEASE_MAPS) {
      send mon, eHoldMaps, (osd = s.osd, hold = s.op == OP_HOLD_MAPS);
      send this, eStep;
      return false;
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
    } else {
      announce mIssued, nextWid;
      unacked += (nextWid);
      send client, eIssueWrite, nextWid;
      nextWid = nextWid + 1;
    }
    send this, eStep;
    return false;
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
    } else {
      send mon, eEnvMinSize, 1 + choose(cfg.size);
    }
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
    var undetected: set[int];
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
    crashed = default(set[int]);
    detected = default(set[int]);
    announce mSettled;
  }
}

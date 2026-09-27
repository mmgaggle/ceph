/*
 * An OSD's services (OSD, OSDService): maps, boot, heartbeats and failure
 * reports, the cluster messenger's links, the messenger's address
 * filter, the map-epoch gate on incoming PG messages, the send rules, up_thru
 * and pg_temp requests, and the local and remote reservers. The PG itself
 * runs in the Pg machine, which this one feeds, as the OSD's shard threads
 * run PGs beside its dispatch thread.
 *
 * - A message addressed to an earlier instance of this OSD is lost; one that
 *   needs a newer map than the OSD has waits for it.
 * - _committed_osd_maps: the OSD's own state follows the newest map of a
 *   batch (booting -> active only if it shows us up at our address; active
 *   and shown down -> boot again at a new address), then the PG takes the
 *   maps (consume_map).
 * - dispatch_context sends a PeeringCtx's messages (queries, notifies,
 *   infos) only while the OSD is up and active; send_message_osd_cluster and
 *   get_con_osd_cluster send nothing to a peer down in the newest map or
 *   restarted since the PG's epoch, and address the newest map's address.
 */
machine Osd {
  var cfg: tCfg;
  var me: int;
  var mon: machine;
  var osds: map[int, machine];
  var client: machine;
  var env: machine;
  var pg: machine;

  // on disk
  var maps: map[int, tMap];
  var newest: int;
  var nonce: int;

  // in memory
  var running: bool;
  var addr: int;
  var booting: bool;
  var upThruWanted: int;
  var pgTempWant: bool;
  var pgTempWanted: seq[int];
  var pgTempHasPending: bool;
  var pgTempPending: seq[int];
  var held: seq[tMsg];
  var heldWrites: seq[(wid: int, oid: int, epoch: int)];
  var full: bool;                       // backfill/recovery toofull
  var crashes: int;                     // (model) the process incarnation: the PG's messages carry theirs
  var pgTempForced: bool;
  var others: int;                      // other PGs held here (num_pgs is these and ours)

  // the cluster network: links that are down, and the lossless messenger's
  // queue for each peer it cannot reach
  var outCut: set[int];
  var inCut: set[int];
  var outq: map[int, seq[tMsg]];
  // heartbeats
  var hbGen: map[int, int];
  var failurePending: set[int];         // peers reported failed and not yet cancelled
  var waitingHealthy: bool;             // STATE_WAITING_FOR_HEALTHY
  var markdowns: int;                   // osd_markdown_log

  // AsyncReserver: local (recovery, backfill and deletion of our own PGs) and
  // remote (replica side of other OSDs' recovery and backfill)
  var local: tReserver;
  var remote: tReserver;

  start state Init {
    entry (p: (cfg: tCfg, me: int, m: tMap)) {
      cfg = p.cfg;
      me = p.me;
      maps[1] = p.m;
      newest = 1;
      local = NewReserver(cfg.maxBackfills);
      remote = NewReserver(cfg.maxBackfills);
      others = cfg.others;
      pg = new Pg((cfg = cfg, me = me, osd = this, m = p.m));
    }
    on eSetup do (s: (mon: machine, osds: map[int, machine], client: machine, env: machine)) {
      mon = s.mon;
      osds = s.osds;
      client = s.client;
      env = s.env;
      running = true;
      nonce = 1;
      addr = 1;
      send pg, eSetup, s;
      send pg, ePgOthers, others;
      goto Run;
    }
    defer ePeer, eMaps, eWrite, eCrash, eRestart, eOut, eUpThru, ePgTempReq, eResv, eFull, ePreempt, eCommand, eBusy,
          eOthers, eLink, eHbGrace;
  }

  state Run {
    on eMaps do (ms: seq[tMap]) {
      if (running) {
        HandleMaps(ms);
      }
    }
    on ePeer do (m: tMsg) {
      if (!running || m.toAddr != addr) {
        return;
      }
      if (MapNeeded(m) > newest) {
        held += (sizeof(held), m);
        return;
      }
      send pg, ePgMsg, m;
    }
    on eWrite do (w: (wid: int, oid: int, epoch: int)) {
      if (!running) {
        return;
      }
      if (w.epoch > newest) {
        heldWrites += (sizeof(heldWrites), w);
        return;
      }
      send pg, ePgWrite, w;
    }
    on eOut do (m: tMsg) {
      if (running && m.pinc == crashes) {
        Out(m);
      }
    }
    on eUpThru do (u: (want: int, inc: int)) {
      if (running && u.inc == crashes) {
        QueueWantUpThru(u.want);
      }
    }
    on ePgTempReq do (r: (want: seq[int], clear: bool, inc: int, forced: bool)) {
      if (!running || r.inc != crashes) {
        return;
      }
      if (r.clear) {
        pgTempWant = false;
        pgTempHasPending = false;
      } else if (r.forced || !pgTempHasPending || pgTempPending != r.want) {
        pgTempWanted = r.want;
        pgTempWant = true;
        pgTempForced = r.forced;
      }
      SendPgTemp();
    }
    on eOthers do (d: int) {
      others = others + d;
      if (others < 0) {
        others = 0;
      }
      send pg, ePgOthers, others;
    }
    on eLink do (l: (peer: int, outCut: bool, inCut: bool)) {
      var q: seq[tMsg];
      var i: int;
      if (l.outCut) {
        outCut += (l.peer);
      } else {
        outCut -= (l.peer);
      }
      if (l.inCut) {
        inCut += (l.peer);
      } else {
        inCut -= (l.peer);
      }
      if (!running) {
        return;
      }
      // the messenger reconnects and resends what it queued
      if (!l.outCut && l.peer in outq) {
        q = outq[l.peer];
        outq -= (l.peer);
        while (i < sizeof(q)) {
          send osds[l.peer], ePeer, q[i];
          i = i + 1;
        }
      }
      HeartbeatChanged(l.peer);
      TryBoot();
    }
    on eHbGrace do (g: (peer: int, gen: int)) {
      if (running && g.peer in hbGen && hbGen[g.peer] == g.gen) {
        ReportFailure(g.peer);
      }
    }
    on eResv do (r: tResvReq) {
      if (running && (r.inc == -1 || r.inc == crashes)) {
        HandleResv(r);
      }
    }
    on eFull do (f: bool) {
      if (running) {
        full = f;
        send pg, ePgFull, f;
      }
    }
    on ePreempt do {
      if (running) {
        PreemptOne();
      }
    }
    on eBusy do (b: bool) {
      var q: tResvReq;
      if (!running) {
        return;
      }
      q = (op = RQ_REQUEST, item = OTHER_ITEM() + 1, local = true, prio = 200, grant = Evt(E_NullEvt),
           preempt = Evt(E_NullEvt), hasPreempt = false, epoch = newest, rseq = 0, inc = -1);
      if (!b) {
        q.op = RQ_CANCEL;
      }
      if (b == ResvHas(local, q.item)) {
        return;
      }
      local = ResvApply(local, q, true);
      remote = ResvApply(remote, q, false);
    }
    // OSD::handle_fast_force_recovery (the OSD's epoch) and handle_fast_scrub
    // (the sender's): enqueue_peering_evt, whatever state the PG is in
    on eCommand do (e: tE) {
      if (running) {
        send pg, eQueued, (evt = Evt(e), es = newest, er = newest, inc = -1);
      }
    }
    on eCrash do {
      running = false;
      crashes = crashes + 1;
      send mon, eDied, (osd = me, addr = addr);
      held = default(seq[tMsg]);
      heldWrites = default(seq[(wid: int, oid: int, epoch: int)]);
      full = false;
      Died();
    }
    on eRestart do {
      if (running) {
        return;                                 // the environment's view was stale: already restarted
      }
      running = true;
      nonce = nonce + 1;
      addr = nonce;
      booting = true;
      upThruWanted = 0;
      pgTempWant = false;
      pgTempHasPending = false;
      waitingHealthy = false;
      markdowns = 0;
      send mon, eBoot, (osd = me, have = newest, addr = addr);
      send pg, ePgRestart;
      send pg, ePgOthers, others;
    }
  }

  // the process is gone (crashed, or shut itself down): what it held in
  // memory goes with it
  fun Died() {
    local = NewReserver(cfg.maxBackfills);
    remote = NewReserver(cfg.maxBackfills);
    Announce(local, true);
    Announce(remote, false);
    held = default(seq[tMsg]);
    heldWrites = default(seq[(wid: int, oid: int, epoch: int)]);
    outq = default(map[int, seq[tMsg]]);
    failurePending = default(set[int]);
    waitingHealthy = false;
    send pg, ePgCrash;
  }

  // ------------------------------------------------------------ heartbeats

  fun Unhealthy(p: int): bool {
    return p in outCut || p in inCut;
  }

  fun Active(): bool {
    return running && !booting && maps[newest].up[me] && maps[newest].addr[me] == addr;
  }

  // a heartbeat peer's pings start or stop failing
  fun HeartbeatChanged(p: int) {
    var gen: int;
    if (p in hbGen) {
      gen = hbGen[p];
    }
    hbGen[p] = gen + 1;
    if (Unhealthy(p)) {
      // heartbeat_check after osd_heartbeat_grace
      send this, eHbGrace, (peer = p, gen = gen + 1);
    } else if (p in failurePending) {
      // handle_osd_ping: a reply from a peer we reported
      failurePending -= (p);
      send mon, eFailure, (reporter = me, raddr = addr, target = p, taddr = maps[newest].addr[p],
                           epoch = newest, alive = true);
    }
  }

  // heartbeat_check -> send_failures: only an active OSD reports, and only
  // peers up in its map
  fun ReportFailure(p: int) {
    var n: tMap;
    n = maps[newest];
    if (!Unhealthy(p) || !Active() || p == me || !n.up[p] || p in failurePending) {
      return;
    }
    failurePending += (p);
    send mon, eFailure, (reporter = me, raddr = addr, target = p, taddr = n.addr[p], epoch = newest, alive = false);
  }

  // _is_healthy while waiting for healthy: enough heartbeat peers answer
  // (osd_heartbeat_min_healthy_ratio 0.33), unless not marked down lately
  fun Healthy(): bool {
    var o: int;
    var num: int;
    var ok: int;
    var n: tMap;
    if (markdowns == 0) {
      return true;
    }
    n = maps[newest];
    while (o < cfg.nOsds) {
      if (o != me && n.up[o]) {
        num = num + 1;
        if (!Unhealthy(o)) {
          ok = ok + 1;
        }
      }
      o = o + 1;
    }
    return 100 * ok >= 33 * num;
  }

  // start_boot: not until healthy
  fun TryBoot() {
    if (running && waitingHealthy && Healthy()) {
      waitingHealthy = false;
      nonce = nonce + 1;
      addr = nonce;
      send mon, eBoot, (osd = me, have = newest, addr = addr);
    }
  }

  // the epoch the OSD must have before the PG sees the message: a peering
  // message's epoch_sent; a data message's min_epoch (the primary's interval
  // start or last peering reset)
  fun MapNeeded(m: tMsg): int {
    if (m.kind == K_REPOP || m.kind == K_REPOP_REPLY || m.kind == K_PUSH || m.kind == K_PUSH_REPLY ||
        m.kind == K_PULL || m.kind == K_SCAN || m.kind == K_SCAN_DIGEST || m.kind == K_BACKFILL_PROGRESS ||
        m.kind == K_BACKFILL_FINISH || m.kind == K_BACKFILL_FINISH_ACK || m.kind == K_BACKFILL_REMOVE) {
      return m.req;
    }
    return m.epSent;
  }

  fun HandleMaps(ms: seq[tMap]) {
    var i: int;
    var old: int;
    var cur: tMap;
    var batch: seq[tMap];
    var pending: seq[tMsg];
    var rebooting: bool;
    var wasBooting: bool;
    var o: int;
    var fp: set[int];
    var pw: seq[(wid: int, oid: int, epoch: int)];
    old = newest;
    while (i < sizeof(ms)) {
      if (ms[i].epoch == newest + 1) {
        maps[ms[i].epoch] = ms[i];
        newest = ms[i].epoch;
        batch += (sizeof(batch), ms[i]);
      }
      i = i + 1;
    }
    if (newest == old) {
      return;
    }
    cur = maps[newest];
    if (booting) {
      if (cur.up[me] && cur.addr[me] == addr) {
        booting = false;
        wasBooting = true;
      }
    } else if (!cur.up[me] || cur.addr[me] != addr) {
      // "wrongly marked me down": wait until healthy, then boot again; or,
      // marked down too often, shut down
      booting = true;
      rebooting = true;
    }
    // note_down_osd: the messenger drops what it queued for a peer now down
    foreach (o in keys(outq)) {
      if (!cur.up[o]) {
        outq -= (o);
      }
    }
    fp = failurePending;
    foreach (o in fp) {
      if (!cur.up[o]) {
        failurePending -= (o);
      }
    }
    // booting -> active: cancel_pending_failures
    if (wasBooting) {
      foreach (o in fp) {
        if (o in failurePending) {
          failurePending -= (o);
          send mon, eFailure, (reporter = me, raddr = addr, target = o, taddr = cur.addr[o], epoch = newest,
                               alive = true);
        }
      }
    }
    send pg, ePgMaps, (maps = batch, active = !booting && cur.up[me]);
    if (rebooting) {
      markdowns = markdowns + 1;
      if (markdowns > cfg.maxMarkdowns) {
        // shut down: as a crash, but the environment is told
        running = false;
        crashes = crashes + 1;
        send mon, eDied, (osd = me, addr = addr);
        full = false;
        send env, eNoteExit, me;
        Died();
        return;
      }
      waitingHealthy = true;
      TryBoot();
    }
    // heartbeats to peers that are unreachable report once we are active
    // (and again for a peer that came back up)
    foreach (o in outCut) {
      if (wasBooting || (!maps[old].up[o] && cur.up[o])) {
        HeartbeatChanged(o);
      }
    }
    foreach (o in inCut) {
      if (!(o in outCut) && (wasBooting || (!maps[old].up[o] && cur.up[o]))) {
        HeartbeatChanged(o);
      }
    }
    TryBoot();
    pending = held;
    held = default(seq[tMsg]);
    i = 0;
    while (i < sizeof(pending)) {
      if (pending[i].toAddr == addr && MapNeeded(pending[i]) <= newest) {
        send pg, ePgMsg, pending[i];
      } else if (pending[i].toAddr == addr) {
        held += (sizeof(held), pending[i]);
      }
      i = i + 1;
    }
    pw = heldWrites;
    heldWrites = default(seq[(wid: int, oid: int, epoch: int)]);
    i = 0;
    while (i < sizeof(pw)) {
      if (pw[i].epoch <= newest) {
        send pg, ePgWrite, pw[i];
      } else {
        heldWrites += (sizeof(heldWrites), pw[i]);
      }
      i = i + 1;
    }
  }

  // OSD::queue_want_up_thru, OSD::send_alive
  fun QueueWantUpThru(want: int) {
    if (want > upThruWanted) {
      upThruWanted = want;
      if (upThruWanted > maps[newest].upThru[me]) {
        send mon, eAlive, (osd = me, addr = addr, want = upThruWanted, version = newest);
      }
    }
  }

  // OSDService::send_pg_temp
  fun SendPgTemp() {
    if (pgTempWant) {
      send mon, ePgTemp, (osd = me, addr = addr, want = pgTempWanted, epoch = newest, forced = pgTempForced);
      pgTempForced = false;
      pgTempPending = pgTempWanted;
      pgTempHasPending = true;
      pgTempWant = false;
    }
  }

  // the send rules. m.epSent is the PG's epoch (from_epoch); m.req == -1
  // marks a reply to a PG-less query, sent against our newest map
  fun Out(m: tMsg) {
    var from: int;
    var n: tMap;
    var q: seq[tMsg];
    if (m.kind == K_QUERY_INFO || m.kind == K_QUERY_LOG || m.kind == K_NOTIFY || m.kind == K_INFO) {
      // OSD::dispatch_context
      if (booting || !maps[newest].up[me]) {
        return;
      }
    }
    from = m.epSent;
    n = maps[newest];
    if (!n.up[m.dst] || n.upFrom[m.dst] > from) {
      return;
    }
    m.toAddr = n.addr[m.dst];
    if (m.dst in outCut) {
      // lossless: queued until the link heals or the peer is marked down
      if (m.dst in outq) {
        q = outq[m.dst];
      }
      q += (sizeof(q), m);
      outq[m.dst] = q;
      return;
    }
    send osds[m.dst], ePeer, m;
  }

  // ======================================================= AsyncReserver

  fun NewReserver(max: int): tReserver {
    var r: tReserver;
    r.max = max;
    return r;
  }

  fun HandleResv(q: tResvReq) {
    if (q.local) {
      local = ResvApply(local, q, true);
    } else {
      remote = ResvApply(remote, q, false);
    }
  }

  fun ResvApply(r: tReserver, q: tResvReq, isLocal: bool): tReserver {
    var i: int;
    var it: tResvItem;
    if (q.op == RQ_REQUEST) {
      // AsyncReserver::request_reservation
      assert !ResvQueued(r, q.item) && !(q.item in r.inProgress),
        format("osd.{0} {1} reserver: duplicate reservation request for {2}", me, ResvName(isLocal), q.item);
      r.queue += (sizeof(r.queue), (item = q.item, prio = q.prio, grant = q.grant, preempt = q.preempt,
                                    hasPreempt = q.hasPreempt, epoch = q.epoch, rseq = q.rseq));
    } else if (q.op == RQ_CANCEL) {
      // cancel_reservation: queued callbacks never fire; a granted slot is freed
      if (ResvQueued(r, q.item)) {
        r.queue = ResvUnqueue(r.queue, q.item);
      } else if (q.item in r.inProgress) {
        r.inProgress -= (q.item);
      }
    } else {
      // update_priority
      if (ResvQueued(r, q.item)) {
        i = 0;
        while (i < sizeof(r.queue)) {
          if (r.queue[i].item == q.item) {
            it = r.queue[i];
          }
          i = i + 1;
        }
        if (it.prio != q.prio) {
          r.queue = ResvUnqueue(r.queue, q.item);
          it.prio = q.prio;
          r.queue += (sizeof(r.queue), it);
        }
      } else if (q.item in r.inProgress) {
        it = r.inProgress[q.item];
        it.prio = q.prio;
        r.inProgress[q.item] = it;
      }
    }
    r = DoQueues(r, isLocal);
    Announce(r, isLocal);
    return r;
  }

  fun ResvName(isLocal: bool): string {
    if (isLocal) {
      return "local";
    }
    return "remote";
  }

  fun ResvHas(r: tReserver, item: int): bool {
    return item in r.inProgress || ResvQueued(r, item);
  }

  fun ResvQueued(r: tReserver, item: int): bool {
    var i: int;
    while (i < sizeof(r.queue)) {
      if (r.queue[i].item == item) {
        return true;
      }
      i = i + 1;
    }
    return false;
  }

  fun ResvUnqueue(qs: seq[tResvItem], item: int): seq[tResvItem] {
    var out: seq[tResvItem];
    var i: int;
    while (i < sizeof(qs)) {
      if (qs[i].item != item) {
        out += (sizeof(out), qs[i]);
      }
      i = i + 1;
    }
    return out;
  }

  // AsyncReserver::do_queues
  fun DoQueues(r: tReserver, isLocal: bool): tReserver {
    var head: int;
    var i: int;
    var low: int;
    var it: tResvItem;
    while (sizeof(r.inProgress) > r.max && LowestPreemptible(r) != -1) {
      r = PreemptOneOf(r, isLocal);
    }
    while (sizeof(r.queue) > 0) {
      // the highest priority, first come first served within it
      head = 0;
      i = 1;
      while (i < sizeof(r.queue)) {
        if (r.queue[i].prio > r.queue[head].prio) {
          head = i;
        }
        i = i + 1;
      }
      if (sizeof(r.inProgress) >= r.max) {
        low = LowestPreemptible(r);
        if (low != -1 && r.inProgress[low].prio < r.queue[head].prio) {
          r = PreemptOneOf(r, isLocal);
        }
      }
      if (sizeof(r.inProgress) >= r.max) {
        return r;
      }
      it = r.queue[head];
      r.queue -= (head);
      r.inProgress[it.item] = it;
      Callback(it, it.grant);
    }
    return r;
  }

  fun LowestPreemptible(r: tReserver): int {
    var best: int;
    var k: int;
    best = -1;
    foreach (k in keys(r.inProgress)) {
      if (r.inProgress[k].hasPreempt) {
        if (best == -1 || r.inProgress[k].prio < r.inProgress[best].prio ||
            (r.inProgress[k].prio == r.inProgress[best].prio && k < best)) {
          best = k;
        }
      }
    }
    return best;
  }

  // AsyncReserver::preempt_one: the victim is told only through its callback
  fun PreemptOneOf(r: tReserver, isLocal: bool): tReserver {
    var v: int;
    var it: tResvItem;
    v = LowestPreemptible(r);
    it = r.inProgress[v];
    r.inProgress -= (v);
    Callback(it, it.preempt);
    return r;
  }

  // the finisher: callbacks are queued, never run inline. Other PGs' items
  // (item != PG_ITEM) have no PG here.
  fun Callback(it: tResvItem, ev: tEvt) {
    ev.fromResv = true;
    ev.rseq = it.rseq;
    if (it.item == PG_ITEM()) {
      send pg, eQueued, (evt = ev, es = it.epoch, er = it.epoch, inc = -1);
    }
  }

  fun Announce(r: tReserver, isLocal: bool) {
    announce mReserve, (osd = me, pgOsd = me, local = isLocal, held = PG_ITEM() in r.inProgress || ResvQueued(r, PG_ITEM()));
  }

  // another PG's higher-priority request takes a slot, then gives it back
  fun PreemptOne() {
    var q: tResvReq;
    q = (op = RQ_REQUEST, item = OTHER_ITEM(), local = true, prio = 255, grant = Evt(E_NullEvt),
         preempt = Evt(E_NullEvt), hasPreempt = false, epoch = newest, rseq = 0, inc = -1);
    local = ResvApply(local, q, true);
    q.op = RQ_CANCEL;
    local = ResvApply(local, q, true);
    q.op = RQ_REQUEST;
    remote = ResvApply(remote, q, false);
    q.op = RQ_CANCEL;
    remote = ResvApply(remote, q, false);
  }
}

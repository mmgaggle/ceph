/*
 * An OSD and its instance of the PG: PeeringState's state machine, the
 * OSD's map handling and its requests to the monitor.
 *
 * The statechart is flattened into `st`:
 *   Reset
 *   Stray, ReplicaActive                           (Started, not primary)
 *   GetInfo, GetLog, GetMissing, WaitUpThru,       (Primary/Peering)
 *   Down, Incomplete
 *   WaitActingChange                               (Primary)
 *   Active                                         (Primary; Activating,
 *                                                   Recovered and Clean folded in)
 *
 * Maps: every map is applied in order (AdvMap per epoch, then one ActMap for
 * the batch), as OSD::advance_pg does. A peering message waits until the
 * OSD has the map it was sent in, then is dropped if the PG has reset since
 * it was sent or requested (PG::old_peering_msg). A message is addressed to
 * the incarnation (up_from) the sender's map shows; one addressed to an
 * earlier incarnation is lost, as a message to an old address is.
 *
 * Persistence: info, log, past intervals, the maps and the PG's epoch are
 * on disk and survive a crash; each handler's changes commit when it ends.
 */
enum tSt {
  S_NONE, S_RESET, S_STRAY, S_REPLICA_ACTIVE,
  S_GET_INFO, S_GET_LOG, S_GET_MISSING, S_WAIT_UP_THRU, S_DOWN, S_INCOMPLETE,
  S_WAIT_ACTING_CHANGE, S_ACTIVE
}

event eRecovered: int;   // PrimaryLogPG::on_activate_complete's AllReplicasRecovered, queued at an epoch

machine Osd {
  var cfg: tCfg;
  var me: int;
  var mon: machine;
  var osds: map[int, machine];
  var client: machine;
  var env: machine;

  // on disk
  var maps: map[int, tMap];
  var newest: int;
  var hasPg: bool;
  var info: tInfo;
  var log: seq[tEntry];
  var pi: tPI;
  var pgEpoch: int;

  // the OSD, in memory
  var running: bool;
  var nonce: int;                  // on disk: boots so far
  var addr: int;                   // our address: a nonce per boot
  var booting: bool;               // sent a boot, not yet up in the map with this address
  var upThruWanted: int;
  var pgTempWant: bool;
  var pgTempWanted: seq[int];
  var pgTempHasPending: bool;
  var pgTempPending: seq[int];
  var held: seq[tMsg];
  var heldWrites: seq[(wid: int, epoch: int)];

  // the PG, in memory
  var st: tSt;
  var up: seq[int];
  var acting: seq[int];
  var lpr: int;                    // last_peering_reset
  var sendNotify: bool;
  var peerInfo: map[int, tInfo];
  var infoRequested: set[int];     // GetInfo::peer_info_requested
  var missingRequested: set[int];  // GetMissing::peer_missing_requested
  var prior: tPrior;
  var needUpThru: bool;
  var wantActing: seq[int];
  var authLogShard: int;
  var arb: set[int];               // acting_recovery_backfill
  var peerActivated: set[int];
  var active: bool;                // PG_STATE_ACTIVE
  var queuedWrites: seq[int];      // waiting_for_active
  var inFlight: map[int, (wid: int, waiting: set[int])];

  start state Init {
    entry (p: (cfg: tCfg, me: int, m: tMap)) {
      cfg = p.cfg;
      me = p.me;
      maps[1] = p.m;
      newest = 1;
      pgEpoch = 1;
      // the PG was created in epoch 1 on its acting set, empty
      if (Contains(ActingSet(p.m), me)) {
        hasPg = true;
        info = EmptyInfo();
        info.h.created = 1;
        info.h.sis = 1;
      }
      pi = EmptyPI();
    }
    on eSetup do (s: (mon: machine, osds: map[int, machine], client: machine, env: machine)) {
      mon = s.mon;
      osds = s.osds;
      client = s.client;
      env = s.env;
      running = true;
      if (hasPg) {
        announce mHolds, me;
      }
      nonce = 1;
      addr = 1;
      LoadPg();
      if (hasPg) {
        ActMap();
      }
      AfterEvent();
      goto Run;
    }
    defer ePeer, eMaps, eWrite, eCrash, eRestart, eRecovered;
  }

  state Run {
    on eMaps do (ms: seq[tMap]) {
      if (!running) {
        return;
      }
      HandleMaps(ms);
      AfterEvent();
    }
    on ePeer do (m: tMsg) {
      if (!running || m.toAddr != addr) {
        return;
      }
      if (MapNeeded(m) > newest) {
        held += (sizeof(held), m);
        return;
      }
      Dispatch(m);
      AfterEvent();
    }
    on eRecovered do (e: int) {
      if (!running || !hasPg) {
        return;
      }
      if (lpr > e) {
        return;
      }
      if (st == S_ACTIVE) {
        Recovered();
      }
      AfterEvent();
    }
    on eWrite do (w: (wid: int, epoch: int)) {
      if (!running) {
        return;
      }
      if (w.epoch > newest) {
        heldWrites += (sizeof(heldWrites), w);
        return;
      }
      HandleWrite(w.wid, w.epoch);
      AfterEvent();
    }
    on eCrash do {
      running = false;
      send mon, eDied, (osd = me, addr = addr);
      Go(S_NONE);
      held = default(seq[tMsg]);
      heldWrites = default(seq[(wid: int, epoch: int)]);
      ClearPgMemory();
    }
    on eRestart do {
      assert !running, format("osd.{0} restarted while running", me);
      running = true;
      nonce = nonce + 1;
      addr = nonce;
      booting = true;
      upThruWanted = 0;
      pgTempWant = false;
      pgTempHasPending = false;
      send mon, eBoot, (osd = me, have = newest, addr = addr);
      LoadPg();
    }
  }

  // ---------------------------------------------------------------- the OSD

  fun MapNeeded(m: tMsg): int {
    if (m.kind == K_REPOP) {
      return m.req;
    }
    if (m.kind == K_REPOP_REPLY) {
      return 0;
    }
    return m.epSent;
  }

  fun HandleMaps(ms: seq[tMap]) {
    var i: int;
    var old: int;
    var cur: tMap;
    var pending: seq[tMsg];
    var rebooting: bool;
    old = newest;
    while (i < sizeof(ms)) {
      if (ms[i].epoch == newest + 1) {
        maps[ms[i].epoch] = ms[i];
        newest = ms[i].epoch;
      }
      i = i + 1;
    }
    if (newest == old) {
      return;
    }
    // OSD::_committed_osd_maps: our own state from the newest map of the
    // batch first, then consume_map advances the PGs
    cur = maps[newest];
    rebooting = false;
    if (booting) {
      if (cur.up[me] && cur.addr[me] == addr) {
        booting = false;
      }
    } else if (!cur.up[me] || cur.addr[me] != addr) {
      // "wrongly marked me down": waiting for healthy, then boot again
      booting = true;
      rebooting = true;
    }
    if (hasPg) {
      AdvanceTo(newest);
    }
    if (rebooting) {
      // rebind: a new address
      nonce = nonce + 1;
      addr = nonce;
      send mon, eBoot, (osd = me, have = newest, addr = addr);
    }
    pending = held;
    held = default(seq[tMsg]);
    i = 0;
    while (i < sizeof(pending)) {
      if (pending[i].toAddr == addr && MapNeeded(pending[i]) <= newest) {
        Dispatch(pending[i]);
      } else if (pending[i].toAddr == addr) {
        held += (sizeof(held), pending[i]);
      }
      i = i + 1;
    }
    ReleaseWrites();
  }

  fun ReleaseWrites() {
    var pw: seq[(wid: int, epoch: int)];
    var i: int;
    pw = heldWrites;
    heldWrites = default(seq[(wid: int, epoch: int)]);
    while (i < sizeof(pw)) {
      if (pw[i].epoch <= newest) {
        HandleWrite(pw[i].wid, pw[i].epoch);
      } else {
        heldWrites += (sizeof(heldWrites), pw[i]);
      }
      i = i + 1;
    }
  }

  // after each event: OSD::dequeue_peering_evt's up_thru check, and send_pg_temp
  fun AfterEvent() {
    if (!running) {
      return;
    }
    if (hasPg && needUpThru) {
      QueueWantUpThru(info.h.sis);
    }
    if (pgTempWant) {
      send mon, ePgTemp, (osd = me, addr = addr, want = pgTempWanted, epoch = newest);
      pgTempPending = pgTempWanted;
      pgTempHasPending = true;
      pgTempWant = false;
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

  // OSDService::queue_want_pg_temp
  fun QueueWantPgTemp(want: seq[int]) {
    if (!pgTempHasPending || pgTempPending != want) {
      pgTempWanted = want;
      pgTempWant = true;
    }
  }

  fun ClearWantPgTemp() {
    pgTempWant = false;
    pgTempHasPending = false;
  }

  fun Send(m: tMsg) {
    // OSD::dispatch_context sends a PeeringCtx's messages (queries, notifies,
    // infos) only while this OSD is up and active, to peers up in the PG's map
    if (m.kind == K_QUERY_INFO || m.kind == K_QUERY_LOG || m.kind == K_NOTIFY || m.kind == K_INFO) {
      if (booting || !maps[newest].up[me] || !maps[pgEpoch].up[m.dst]) {
        return;
      }
    }
    SendAt(m, pgEpoch);
  }

  // OSDService::send_message_osd_cluster / get_con_osd_cluster: nothing to a
  // peer that is down in our newest map or has restarted since epoch e; the
  // address is the newest map's
  fun SendAt(m: tMsg, e: int) {
    if (!maps[newest].up[m.dst] || maps[newest].upFrom[m.dst] > e) {
      return;
    }
    m.toAddr = maps[newest].addr[m.dst];
    send osds[m.dst], ePeer, m;
  }

  // ------------------------------------------------------ PG load / memory

  fun LoadPg() {
    ClearPgMemory();
    if (!hasPg) {
      return;
    }
    Go(S_RESET);
    up = UpSet(maps[pgEpoch]);
    acting = ActingSet(maps[pgEpoch]);
    lpr = pgEpoch;
    sendNotify = false;
  }

  fun ClearPgMemory() {
    ClearPrimaryState();
    Go(S_NONE);
    wantActing = default(seq[int]);
    sendNotify = false;
    active = false;
    queuedWrites = default(seq[int]);
    inFlight = default(map[int, (wid: int, waiting: set[int])]);
  }

  // PeeringState::clear_primary_state
  fun ClearPrimaryState() {
    peerInfo = default(map[int, tInfo]);
    infoRequested = default(set[int]);
    missingRequested = default(set[int]);
    peerActivated = default(set[int]);
    arb = default(set[int]);
    needUpThru = false;
  }

  fun Go(s: tSt) {
    if (s != st) {
      print format("osd.{0} e{1} {2} -> {3} up={4} acting={5} lu={6} les={7} h.les={8} sis={9}",
                   me, pgEpoch, st, s, up, acting, info.lu, info.les, info.h.les, info.h.sis);
    }
    st = s;
  }

  fun IsPrimary(): bool {
    return PrimaryOf(acting) == me;
  }

  fun Primary(): int {
    return PrimaryOf(acting);
  }

  fun IsPeering(): bool {
    return st == S_GET_INFO || st == S_GET_LOG || st == S_GET_MISSING ||
      st == S_WAIT_UP_THRU || st == S_DOWN || st == S_INCOMPLETE;
  }

  // ------------------------------------------------------------ map events

  fun AdvanceTo(target: int) {
    var lm: tMap;
    var m: tMap;
    while (pgEpoch < target) {
      lm = maps[pgEpoch];
      m = maps[pgEpoch + 1];
      pgEpoch = pgEpoch + 1;
      AdvMap(lm, m);
    }
    ActMap();
  }

  // PeeringState::should_restart_peering
  fun ShouldRestart(lm: tMap, m: tMap): bool {
    return NewInterval(lm, m) || (!lm.up[me] && m.up[me]);
  }

  fun AdvMap(lm: tMap, m: tMap) {
    var o: int;
    var restart: bool;
    var need: bool;
    restart = ShouldRestart(lm, m);
    if (st == S_RESET) {
      ResetAdvMap(lm, m, restart);
      return;
    }
    // GetLog::react(AdvMap): our log source went down
    if (st == S_GET_LOG && !m.up[authLogShard]) {
      ToReset(lm, m, restart);
      return;
    }
    // Incomplete::react(AdvMap): min_size went down
    if (st == S_INCOMPLETE && lm.minSize > m.minSize) {
      ToReset(lm, m, restart);
      return;
    }
    // WaitActingChange::react(AdvMap): a want_acting target went down
    if (st == S_WAIT_ACTING_CHANGE) {
      foreach (o in wantActing) {
        if (!m.up[o]) {
          ToReset(lm, m, restart);
          return;
        }
      }
    }
    // Peering::react(AdvMap)
    if (IsPeering()) {
      if (AffectedByMap(prior, m)) {
        ToReset(lm, m, restart);
        return;
      }
      if (needUpThru && m.upThru[me] >= info.h.sis) {
        needUpThru = false;
      }
    }
    // Active::react(AdvMap), same interval: a stray in want_acting went down
    if (st == S_ACTIVE && !restart) {
      foreach (o in wantActing) {
        if (!m.up[o] && !Contains(acting, o) && !Contains(up, o)) {
          need = true;
        }
      }
      if (need) {
        RemoveDownPeerInfo(m);
        ChooseActing(false, true);
      }
    }
    // Started::react(AdvMap)
    if (restart) {
      ToReset(lm, m, restart);
      return;
    }
    RemoveDownPeerInfo(m);
  }

  // transit<Reset> with the AdvMap posted again
  fun ToReset(lm: tMap, m: tMap, restart: bool) {
    wantActing = default(seq[int]);    // Primary::exit
    Go(S_RESET);
    lpr = pgEpoch;                     // Reset::Reset -> set_last_peering_reset
    ResetAdvMap(lm, m, restart);
  }

  fun ResetAdvMap(lm: tMap, m: tMap, restart: bool) {
    if (restart) {
      StartPeeringInterval(lm, m);
    }
    RemoveDownPeerInfo(m);
  }

  // PeeringState::start_peering_interval
  fun StartPeeringInterval(lm: tMap, m: tMap) {
    var iv: tInterval;
    lpr = m.epoch;
    up = UpSet(m);
    acting = ActingSet(m);
    if (NewInterval(lm, m)) {
      iv = ClosedInterval(info.h.sis, info.h.lec, lm, m);
      pi = PiAdd(pi, iv);
      info.h.sis = m.epoch;
    }
    active = false;
    ClearPrimaryState();
    ClearWantPgTemp();
    sendNotify = !IsPrimary();
    queuedWrites = default(seq[int]);
    inFlight = default(map[int, (wid: int, waiting: set[int])]);
  }

  // PeeringState::remove_down_peer_info
  fun RemoveDownPeerInfo(m: tMap) {
    var o: int;
    foreach (o in keys(peerInfo)) {
      if (!m.up[o]) {
        peerInfo -= (o);
        missingRequested -= (o);
      }
    }
  }

  fun ActMap() {
    if (st == S_RESET) {
      // Reset::react(ActMap), then Start
      if (sendNotify && Primary() >= 0) {
        SendNotify();
      }
      if (IsPrimary()) {
        EnterGetInfo();
      } else {
        Go(S_STRAY);
      }
      return;
    }
    if (st == S_WAIT_UP_THRU && !needUpThru) {
      EnterActive();
      return;
    }
    if ((st == S_STRAY || st == S_REPLICA_ACTIVE) && sendNotify && Primary() >= 0) {
      SendNotify();
    }
  }

  fun SendNotify() {
    var m: tMsg;
    m = Msg(K_NOTIFY, me, Primary(), 0, pgEpoch, pgEpoch);
    m.info = info;
    m.pi = pi;
    Send(m);
  }

  // PeeringState::update_history
  fun UpdateHistory(h: tHistory) {
    var nh: tHistory;
    nh = MergeHistory(info.h, h);
    if (nh != info.h) {
      info.h = nh;
      if (info.h.lec >= info.h.sis) {
        pi = EmptyPI();
      }
    }
  }

  // ------------------------------------------------------ primary: peering

  fun EnterGetInfo() {
    Go(S_GET_INFO);
    infoRequested = default(set[int]);
    BuildPriorSet();
    GetInfos();
    if (prior.pgDown) {
      Go(S_DOWN);
    } else if (sizeof(infoRequested) == 0) {
      EnterGetLog();
    }
  }

  // PeeringState::build_prior
  fun BuildPriorSet() {
    prior = BuildPrior(pi, info.h.les, up, acting, maps[pgEpoch]);
    needUpThru = cfg.upThruGate && maps[pgEpoch].upThru[me] < info.h.sis;
  }

  // GetInfo::get_infos
  fun GetInfos() {
    var o: int;
    var m: tMsg;
    foreach (o in prior.probe) {
      if (o != me && !(o in peerInfo) && !(o in infoRequested) && maps[pgEpoch].up[o]) {
        m = Msg(K_QUERY_INFO, me, o, 0, pgEpoch, pgEpoch);
        m.info = info;
        Send(m);
        infoRequested += (o);
      }
    }
  }

  // PeeringState::proc_replica_notify
  fun ProcReplicaNotify(m: tMsg): bool {
    if (m.src in peerInfo && peerInfo[m.src].lu == m.info.lu) {
      return false;
    }
    if (!HasBeenUpSince(maps[pgEpoch], m.src, m.epSent)) {
      return false;
    }
    peerInfo[m.src] = m.info;
    UpdateHistory(m.info.h);
    return true;
  }

  // GetInfo::react(MNotifyRec): Ceph erases the peer from
  // peer_info_requested before proc_replica_notify, which may discard the
  // notify (a duplicate, or sent before the peer's current up_from)
  fun GetInfoNotify(m: tMsg) {
    var o: int;
    var old: int;
    var keep: set[int];
    if (!cfg.getInfoKeepsRequest) {
      infoRequested -= (m.src);
    }
    old = info.h.les;
    if (ProcReplicaNotify(m)) {
      infoRequested -= (m.src);
      if (old < info.h.les) {
        BuildPriorSet();
        foreach (o in infoRequested) {
          if (o in prior.probe) {
            keep += (o);
          }
        }
        infoRequested = keep;
        GetInfos();
      }
      if (sizeof(infoRequested) == 0 && !prior.pgDown) {
        EnterGetLog();
      }
    }
  }

  fun DownNotify(m: tMsg) {
    var old: int;
    old = info.h.les;
    if (!(m.src in peerInfo) && HasBeenUpSince(maps[pgEpoch], m.src, m.epSent)) {
      UpdateHistory(m.info.h);
    }
    if (info.h.les > old) {
      EnterGetInfo();
    }
  }

  fun EnterGetLog() {
    var m: tMsg;
    Go(S_GET_LOG);
    if (!ChooseActing(false, false)) {
      if (sizeof(wantActing) > 0) {
        Go(S_WAIT_ACTING_CHANGE);
      } else {
        Go(S_INCOMPLETE);
      }
      return;
    }
    if (authLogShard == me) {
      EnterGetMissing();
      return;
    }
    m = Msg(K_QUERY_LOG, me, authLogShard, 0, pgEpoch, pgEpoch);
    m.info = info;
    Send(m);
  }

  // PeeringState::proc_master_log
  fun ProcMasterLog(m: tMsg) {
    log = MergeLog(log, m.log, m.info.lu, m.tail);
    info.lu = m.info.lu;
    peerInfo[m.src] = m.info;
    if (m.info.les > info.les) {
      info.les = m.info.les;
    }
    if (m.info.lis > info.lis) {
      info.lis = m.info.lis;
    }
    UpdateHistory(m.info.h);
  }

  fun EnterGetMissing() {
    var o: int;
    var m: tMsg;
    var p: tInfo;
    Go(S_GET_MISSING);
    missingRequested = default(set[int]);
    foreach (o in arb) {
      if (o != me) {
        p = peerInfo[o];
        if (VNum(p.lu) != 0 && p.lu != info.lu) {
          m = Msg(K_QUERY_LOG, me, o, 0, pgEpoch, pgEpoch);
          m.info = info;
          Send(m);
          missingRequested += (o);
        }
      }
    }
    if (sizeof(missingRequested) == 0) {
      AfterGetMissing();
    }
  }

  fun AfterGetMissing() {
    if (needUpThru) {
      Go(S_WAIT_UP_THRU);
    } else {
      EnterActive();
    }
  }

  // PeeringState::proc_replica_log: the peer's last_update becomes the
  // newest entry it shares with the authoritative log
  fun ProcReplicaLog(m: tMsg) {
    var p: tInfo;
    p = m.info;
    p.lu = CommonHead(log, m.log);
    peerInfo[m.src] = p;
  }

  // PeeringState::choose_acting (replicated)
  fun ChooseActing(restrict: bool, pgTempOnly: bool): bool {
    var o: int;
    var all: map[int, tInfo];
    var auth: int;
    var prim: int;
    var want: seq[int];
    var cur: tMap;
    cur = maps[pgEpoch];
    all = peerInfo;
    all[me] = info;
    auth = FindBestInfo(all, cfg.nOsds, restrict, up, acting, me, cfg.historyLes);
    if (auth == -1) {
      if (up != acting) {
        wantActing = up;
        QueueWantPgTemp(default(seq[int]));
      } else {
        wantActing = default(seq[int]);
      }
      return false;
    }
    // select_replicated_primary: up[0] unless it is not contiguous (never, untrimmed)
    prim = auth;
    if (sizeof(up) > 0 && PrimaryOf(up) in all) {
      prim = PrimaryOf(up);
    }
    // calc_replicated_acting dereferences all_info.find() for every up and acting member
    foreach (o in up) {
      assert o in all, format("osd.{0} e{1} chooses an acting set without info from up osd.{2}", me, pgEpoch, o);
    }
    foreach (o in acting) {
      assert o in all, format("osd.{0} e{1} chooses an acting set without info from acting osd.{2}", me, pgEpoch, o);
    }
    want = CalcReplicatedActing(prim, cur.size, acting, up, all, cfg.nOsds, restrict);
    // recoverable(): osd_allow_recovery_below_min_size, any one copy
    if (sizeof(want) == 0) {
      wantActing = default(seq[int]);
      return false;
    }
    while (sizeof(want) > cur.size) {
      want -= (sizeof(want) - 1);
    }
    if (want != acting) {
      wantActing = want;
      if (want == up) {
        QueueWantPgTemp(default(seq[int]));
      } else {
        QueueWantPgTemp(want);
      }
      return false;
    }
    if (pgTempOnly) {
      return true;
    }
    wantActing = default(seq[int]);
    arb = ToSet(want);
    authLogShard = auth;
    return true;
  }

  // ----------------------------------------------------- primary: activation

  fun Writeable(): bool {
    return sizeof(acting) >= maps[pgEpoch].minSize;
  }

  // Active::Active -> PeeringState::activate
  fun EnterActive() {
    var o: int;
    var m: tMsg;
    var p: tInfo;
    var olog: seq[tEntry];
    var tail: int;
    Go(S_ACTIVE);
    if (Writeable()) {
      info.les = pgEpoch;
      info.lis = info.h.sis;
    }
    needUpThru = false;
    sendNotify = false;
    foreach (o in arb) {
      if (o != me) {
        assert o in peerInfo, format("osd.{0} activating without info from osd.{1}", me, o);
        p = peerInfo[o];
        if (p.lu == info.lu && VNum(p.lu) != 0) {
          m = Msg(K_INFO, me, o, 0, pgEpoch, pgEpoch);
          m.info = info;
          Send(m);
        } else {
          tail = CopyAfterTail(log, p.lu);
          olog = CopyAfter(log, p.lu);
          m = Msg(K_LOG, me, o, 0, pgEpoch, lpr);
          m.info = info;
          m.log = olog;
          m.tail = tail;
          if (p.h.created == 0) {
            m.pi = pi;
          }
          Send(m);
        }
        p.lu = info.lu;
        peerInfo[o] = p;
      }
    }
    if (Writeable()) {
      announce mActivated, (osd = me, epoch = pgEpoch, wids = Wids(log));
    }
    // ActivateCommitted for ourselves
    peerActivated += (me);
    if (sizeof(peerActivated) == sizeof(arb)) {
      AllActivated();
    }
  }

  fun ActiveInfo(m: tMsg) {
    if (m.src in arb && !(m.src in peerActivated)) {
      peerActivated += (m.src);
      if (sizeof(peerActivated) == sizeof(arb)) {
        AllActivated();
      }
    }
  }

  // Active::react(AllReplicasActivated)
  fun AllActivated() {
    var w: seq[int];
    var i: int;
    active = Writeable();
    info.h.les = info.les;
    info.h.lis = info.lis;
    SharePgInfo();
    if (active) {
      announce mActive, (osd = me, sis = info.h.sis);
      send env, eNoteActive, info.h.sis;
    }
    send this, eRecovered, pgEpoch;
    w = queuedWrites;
    queuedWrites = default(seq[int]);
    if (active) {
      while (i < sizeof(w)) {
        DoWrite(w[i]);
        i = i + 1;
      }
    }
  }

  // Recovered::Recovered, then Clean::Clean -> try_mark_clean
  fun Recovered() {
    if (acting != up) {
      ChooseActing(true, false);
    }
    if (sizeof(acting) == maps[pgEpoch].size) {
      info.h.lec = pgEpoch;
      pi = EmptyPI();
    }
    SharePgInfo();
  }

  fun SharePgInfo() {
    var o: int;
    var m: tMsg;
    foreach (o in arb) {
      if (o != me) {
        m = Msg(K_INFO, me, o, 0, pgEpoch, pgEpoch);
        m.info = info;
        Send(m);
      }
    }
  }

  fun ActiveNotify(m: tMsg) {
    if (!(m.src in peerInfo)) {
      ProcReplicaNotify(m);
      ChooseActing(false, true);
    }
  }

  // --------------------------------------------------------- replica side

  fun FulfillQuery(m: tMsg) {
    var r: tMsg;
    UpdateHistory(m.info.h);
    if (m.kind == K_QUERY_INFO) {
      r = Msg(K_NOTIFY, me, m.src, 0, pgEpoch, m.epSent);
      r.info = info;
      r.pi = pi;
    } else {
      r = Msg(K_LOG, me, m.src, 0, pgEpoch, m.epSent);
      r.info = info;
      r.log = log;
    }
    Send(r);
  }

  // Stray::react(MLogRec) / Stray::react(MInfoRec), then ReplicaActive::react(Activate)
  fun StrayActivate(m: tMsg) {
    var r: tMsg;
    var i: tInfo;
    if (m.kind == K_LOG) {
      log = MergeLog(log, m.log, m.info.lu, m.tail);
      info.lu = m.info.lu;
    } else {
      if (info.lu > m.info.lu) {
        log = RewindTo(log, m.info.lu);
        info.lu = m.info.lu;
      }
      assert info.lu == m.info.lu,
        format("osd.{0} got activation info at {1} while at {2}", me, m.info.lu, info.lu);
    }
    Go(S_REPLICA_ACTIVE);
    if (info.les < m.info.les) {
      info.les = m.info.les;
      info.lis = info.h.sis;
    }
    sendNotify = false;
    // ReplicaActive::react(ActivateCommitted)
    i = info;
    i.h.les = m.info.les;
    i.h.lis = i.h.sis;
    r = Msg(K_INFO, me, Primary(), 0, pgEpoch, pgEpoch);
    r.info = i;
    Send(r);
  }

  fun ApplyRepop(m: tMsg) {
    var r: tMsg;
    if (cfg.repopFilter && lpr > m.epSent) {
      return;
    }
    if (m.ent.ver > info.lu) {
      log += (sizeof(log), m.ent);
      info.lu = m.ent.ver;
    }
    r = Msg(K_REPOP_REPLY, me, m.src, 0, pgEpoch, m.epSent);
    r.ent = m.ent;
    Send(r);
  }

  // OSD::handle_pg_query_nopg, OSD::handle_pg_create_info
  fun NoPg(m: tMsg) {
    var r: tMsg;
    if (m.kind == K_QUERY_INFO || m.kind == K_QUERY_LOG) {
      if (m.kind == K_QUERY_INFO) {
        r = Msg(K_NOTIFY, me, m.src, 0, newest, m.epSent);
      } else {
        r = Msg(K_LOG, me, m.src, 0, newest, m.epSent);
      }
      SendAt(r, newest);
      return;
    }
    if (m.kind != K_LOG && m.kind != K_NOTIFY) {
      return;
    }
    // ShardedOpWQ::_process: create only if the PG maps here now
    if (!MapsHere()) {
      return;
    }
    // PGCreateInfo: MOSDPGLog creates the PG as of its query epoch,
    // MOSDPGNotify2 as of its epoch_sent, with the sender's history and
    // past intervals
    hasPg = true;
    announce mHolds, me;
    info = EmptyInfo();
    info.h = m.info.h;
    log = default(seq[tEntry]);
    pi = m.pi;
    if (m.kind == K_LOG) {
      pgEpoch = m.req;
    } else {
      pgEpoch = m.epSent;
    }
    print format("osd.{0} creates the PG at e{1} from osd.{2}'s {3}, history {4}, past intervals {5}",
                 me, pgEpoch, m.src, m.kind, m.info.h, m.pi);
    ClearPgMemory();
    Go(S_RESET);
    up = UpSet(maps[pgEpoch]);
    acting = ActingSet(maps[pgEpoch]);
    lpr = pgEpoch;
    sendNotify = false;
    ActMap();
    AdvanceTo(newest);
    Dispatch(m);
    ReleaseWrites();
  }

  fun MapsHere(): bool {
    return Contains(UpSet(maps[newest]), me) || Contains(ActingSet(maps[newest]), me);
  }

  // ------------------------------------------------------------- dispatch

  fun Dispatch(m: tMsg) {
    if (!hasPg) {
      NoPg(m);
      return;
    }
    if (m.kind == K_REPOP) {
      if (st == S_REPLICA_ACTIVE) {
        ApplyRepop(m);
      }
      return;
    }
    if (m.kind == K_REPOP_REPLY) {
      RepopReply(m);
      return;
    }
    // PG::old_peering_msg
    if (cfg.staleFilter && (lpr > m.epSent || lpr > m.req)) {
      return;
    }
    if (m.kind == K_QUERY_INFO || m.kind == K_QUERY_LOG) {
      if (st == S_STRAY || st == S_REPLICA_ACTIVE) {
        FulfillQuery(m);
      }
    } else if (m.kind == K_NOTIFY) {
      if (st == S_GET_INFO) {
        GetInfoNotify(m);
      } else if (st == S_DOWN) {
        DownNotify(m);
      } else if (st == S_INCOMPLETE) {
        if (ProcReplicaNotify(m)) {
          EnterGetLog();
        }
      } else if (st == S_ACTIVE) {
        ActiveNotify(m);
      } else if (st == S_GET_LOG || st == S_GET_MISSING || st == S_WAIT_UP_THRU) {
        ProcReplicaNotify(m);
      }
    } else if (m.kind == K_LOG) {
      if (st == S_GET_LOG) {
        if (m.src == authLogShard) {
          ProcMasterLog(m);
          EnterGetMissing();
        }
      } else if (st == S_GET_MISSING) {
        missingRequested -= (m.src);
        ProcReplicaLog(m);
        if (sizeof(missingRequested) == 0) {
          AfterGetMissing();
        }
      } else if (st == S_WAIT_UP_THRU) {
        peerInfo[m.src] = m.info;
      } else if (st == S_ACTIVE) {
        ProcReplicaLog(m);
      } else if (st == S_STRAY) {
        StrayActivate(m);
      } else if (st == S_REPLICA_ACTIVE) {
        log = MergeLog(log, m.log, m.info.lu, m.tail);
        info.lu = m.info.lu;
      }
    } else if (m.kind == K_INFO) {
      if (st == S_ACTIVE) {
        ActiveInfo(m);
      } else if (st == S_STRAY) {
        StrayActivate(m);
      } else if (st == S_REPLICA_ACTIVE) {
        UpdateHistory(m.info.h);
      }
    }
  }

  // ----------------------------------------------------------------- writes

  fun HandleWrite(wid: int, epoch: int) {
    if (!hasPg) {
      // a PG that should exist here and does not yet: the op waits
      if (MapsHere()) {
        heldWrites += (sizeof(heldWrites), (wid = wid, epoch = epoch));
      }
      return;
    }
    if (!IsPrimary() || epoch < info.h.sis) {
      return;
    }
    if (!active) {
      if (!Contains(queuedWrites, wid)) {
        queuedWrites += (sizeof(queuedWrites), wid);
      }
      return;
    }
    DoWrite(wid);
  }

  fun DoWrite(wid: int) {
    var o: int;
    var i: int;
    var v: int;
    var e: tEntry;
    var waiting: set[int];
    var m: tMsg;
    while (i < sizeof(log)) {
      if (log[i].wid == wid) {
        if (!(log[i].ver in inFlight)) {
          Ack(log[i]);
        }
        return;
      }
      i = i + 1;
    }
    v = Ver(pgEpoch, VNum(info.lu) + 1);
    e = (ver = v, wid = wid);
    log += (sizeof(log), e);
    info.lu = v;
    foreach (o in arb) {
      if (o != me) {
        waiting += (o);
        m = Msg(K_REPOP, me, o, 0, pgEpoch, info.h.sis);
        m.ent = e;
        Send(m);
      }
    }
    if (sizeof(waiting) == 0) {
      Ack(e);
    } else {
      inFlight[v] = (wid = wid, waiting = waiting);
    }
  }

  fun RepopReply(m: tMsg) {
    var f: (wid: int, waiting: set[int]);
    if (!(m.ent.ver in inFlight)) {
      return;
    }
    f = inFlight[m.ent.ver];
    f.waiting -= (m.src);
    if (sizeof(f.waiting) == 0) {
      inFlight -= (m.ent.ver);
      Ack(m.ent);
    } else {
      inFlight[m.ent.ver] = f;
    }
  }

  fun Ack(e: tEntry) {
    announce mAcked, (wid = e.wid, ep = info.les, osd = me);
    send client, eWriteAck, e.wid;
  }
}

// pg_log_t::copy_after: the entries after v, and the tail that makes accurate
fun CopyAfter(log: seq[tEntry], v: int): seq[tEntry] {
  var r: seq[tEntry];
  var i: int;
  while (i < sizeof(log)) {
    if (log[i].ver > v) {
      r += (sizeof(r), log[i]);
    }
    i = i + 1;
  }
  return r;
}

fun CopyAfterTail(log: seq[tEntry], v: int): int {
  var t: int;
  var i: int;
  while (i < sizeof(log)) {
    if (log[i].ver <= v) {
      t = log[i].ver;
    }
    i = i + 1;
  }
  return t;
}

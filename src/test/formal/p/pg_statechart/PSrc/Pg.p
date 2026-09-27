/*
 * One OSD's instance of the PG: PeeringState's statechart and the parts of
 * PG and PrimaryLogPG that feed it (writes, recovery, backfill, log trim,
 * reservations, deletion).
 *
 * The chart is the generated one (Chart.p). Handle() is
 * PeeringState::handle_event: an event goes to the innermost active state
 * and outwards until a state's reaction list has an entry for it; a
 * transition exits every active state inside the innermost state that
 * contains both the reacting state and the target, then enters the target
 * and its initial states; events posted during a reaction are processed
 * after it, in order. Reaching Crashed is the OSD's ceph_abort ("we got a
 * bad state machine event") and a failure of the model.
 *
 * Persistence: info, log, missing, objects, past intervals and the PG's
 * epoch are on disk; each handler's changes commit when it ends.
 */
enum tBit {
  B_CREATING, B_ACTIVE, B_PEERED, B_CLEAN, B_DOWN, B_INCOMPLETE, B_PEERING, B_ACTIVATING,
  B_DEGRADED, B_UNDERSIZED, B_REMAPPED, B_RECOVERY_WAIT, B_RECOVERING, B_RECOVERY_TOOFULL,
  B_RECOVERY_UNFOUND, B_BACKFILL_WAIT, B_BACKFILLING, B_BACKFILL_TOOFULL, B_BACKFILL_UNFOUND,
  B_FORCED_RECOVERY, B_FORCED_BACKFILL, B_WAIT, B_LAGGY, B_DELETING
}

enum tResult { RES_DISCARD, RES_FORWARD, RES_TRANSIT, RES_TERMINATE }

event ePgMaps: (maps: seq[tMap], active: bool);
event ePgMsg: tMsg;
event ePgWrite: (wid: int, oid: int, epoch: int);
event ePgCrash;
event ePgRestart;
event ePgFull: bool;
event eOut: tMsg;
// a PG's events to itself; `inc` is its process incarnation (a restart
// loses everything queued before)
event eRecoveryTick: (e: int, inc: int);
event eFinishRecovery: (e: int, inc: int);
event eRequeueWrite: (w: tW, inc: int);
event eRequeueMsg: (m: tMsg, inc: int);
// the PG to its OSD, stamped with the process incarnation: what a PG sent
// before a crash dies with the process
event eUpThru: (want: int, inc: int);
event ePgTempReq: (want: seq[int], clear: bool, inc: int, forced: bool);

machine Pg {
  var cfg: tCfg;
  var me: int;
  var osd: machine;
  var client: machine;
  var env: machine;

  // ------------------------------------------------------------- on disk
  var maps: map[int, tMap];
  var newest: int;
  var hasPg: bool;
  var info: tInfo;
  var log: seq[tEntry];
  var missing: map[int, tMiss];
  var objs: map[int, tObjv];
  var pi: tPI;
  var pgEpoch: int;

  // ------------------------------------------------------------- in memory
  var running: bool;
  var incarnation: int;
  var osdActive: bool;              // the OSD is up and active (for dispatch_context)
  var chart: seq[tS];               // the active states, outermost first
  var posted: seq[tEvt];
  var bits: set[tBit];

  // PeeringState
  var up: seq[int];
  var acting: seq[int];
  var lpr: int;                     // last_peering_reset
  var sendNotify: bool;
  var needUpThru: bool;
  var wantActing: seq[int];
  var peerInfo: map[int, tInfo];
  var peerMissing: map[int, map[int, tMiss]];
  var peerActivated: set[int];
  var peerPurged: set[int];
  var strays: set[int];             // stray_set
  var mightHaveUnfound: set[int];
  var peerMissingRequested: set[int];
  var arb: set[int];                // acting_recovery_backfill
  var backfillTargets: set[int];
  var asyncTargets: set[int];
  var peerLb: map[int, int];        // peer_last_backfill (primary's view)
  var missingLoc: map[int, set[int]];   // object -> OSDs that have its needed version
  var minLcod: int;                 // min_last_complete_ondisk
  var peerLcod: map[int, int];      // peer_last_complete_ondisk

  // state-local data (members of the boost state structs)
  var prior: tPrior;                // Peering::prior_set
  var infoRequested: set[int];      // GetInfo::peer_info_requested
  var authLogShard: int;            // GetLog::auth_log_shard
  var getLogMsg: tMsg;              // GetLog::msg
  var getLogHaveMsg: bool;
  var allReplicasActivated: bool;   // Active::all_replicas_activated
  var resvRecovery: seq[int];       // Active::remote_shards_to_reserve_recovery
  var resvBackfill: seq[int];       // Active::remote_shards_to_reserve_backfill
  var resvAt: int;                  // WaitRemote*Reserved's iterator
  var deletePriority: int;          // ToDelete::priority
  var deleteNext: int;              // Deleting::next

  var needsRecov: map[int, int];    // MissingLoc::needs_recovery_map: object -> version needed
  var pgCommittedTo: int;
  var pgTrimTo: int;
  var lcod: int;                    // last_complete_ondisk
  var pgFull: bool;                 // the OSD is backfillfull (tentative_backfill_full, check_backfill_full)
  var others: int;                  // the OSD's other PGs
  var pendingCreate: bool;          // OSD::pending_creates_from_osd holds this PG
  var spaceReserved: bool;          // try_reserve_recovery_space held

  var localSlot: bool;              // (model) the local reserver's grant is held
  var resvSeq: int;                 // (proposed) reservation request numbers
  var localSeq: int;
  var remoteSeq: int;

  // PrimaryLogPG
  var heldWrites: seq[tW];          // for a PG that does not exist here yet
  var queuedWrites: seq[tW];        // waiting_for_peered / waiting_for_active
  var blockedWrites: seq[tW];       // waiting_for_unreadable_object / degraded_object / the recovery read
  var inFlight: map[int, (wid: int, oid: int, waiting: set[int])];
  var waitingPeered: seq[tMsg];     // replica ops before the PG is peered
  var recoveryQueued: bool;
  var recovering: map[int, int];    // object -> RC_PULL / RC_PUSH / RC_BACKFILL
  var pulling: map[int, int];       // object -> the peer it is pulled from
  var pushing: map[int, set[int]];  // object -> peers a push is outstanding to
  var pushVer: map[int, int];       // object -> the version pushed
  var recoveryOps: int;             // recovery_ops_active
  var workStarted: bool;
  var backfillReservedFlag: bool;   // PeeringState::backfill_reserved
  var backfillReserving: bool;
  var newBackfill: bool;
  var lastBackfillStarted: int;
  var waitingOnBackfill: set[int];
  var pbi: map[int, tPbi];          // peer_backfill_info
  var backfillsInFlight: set[int];
  var pendingBackfill: set[int];    // pending_backfill_updates
  var recoveryReadMarker: set[int]; // objects backfill found write-locked (rwstate.recovery_read_marker)
  var deleting: bool;

  start state Init {
    entry (p: (cfg: tCfg, me: int, osd: machine, m: tMap)) {
      cfg = p.cfg;
      me = p.me;
      osd = p.osd;
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
      client = s.client;
      env = s.env;
      running = true;
      osdActive = true;
      if (hasPg) {
        announce mHolds, me;
        Load();
        ActMapEvt();
      }
      goto Run;
    }
    defer ePgMaps, ePgMsg, ePgWrite, ePgCrash, ePgRestart, ePgFull, eQueued, eRecoveryTick, eFinishRecovery,
          eRequeueWrite, eRequeueMsg, ePgOthers;
  }

  state Run {
    on ePgMaps do (b: (maps: seq[tMap], active: bool)) {
      var i: int;
      if (!running) {
        return;
      }
      osdActive = b.active;
      while (i < sizeof(b.maps)) {
        maps[b.maps[i].epoch] = b.maps[i];
        newest = b.maps[i].epoch;
        i = i + 1;
      }
      // OSD::consume_map: forget a withheld creation for a PG whose acting
      // set no longer has this OSD
      if (pendingCreate && !Contains(ActingSet(maps[newest]), me) &&
          !(cfg.pendingKeepsUp && Contains(UpSet(maps[newest]), me))) {
        pendingCreate = false;
        print format("osd.{0} e{1} discards its pending create (not in acting {2})", me, newest, ActingSet(maps[newest]));
      }
      if (hasPg) {
        AdvanceTo(newest);
      }
      ResumeCreatingPg();
      AfterEvent();
    }
    on ePgMsg do (m: tMsg) {
      if (!running) {
        return;
      }
      HandleMsg(m);
      AfterEvent();
    }
    on eQueued do (q: (evt: tEvt, es: int, er: int, inc: int)) {
      if (!running || !hasPg || (q.inc != -1 && q.inc != incarnation)) {
        return;
      }
      // PG::do_peering_event -> old_peering_evt
      if (lpr > q.es || lpr > q.er) {
        return;
      }
      if (StaleCallback(q.evt)) {
        return;
      }
      NoteResvCallback(q.evt);
      Handle(q.evt);
      AfterEvent();
    }
    on ePgWrite do (w: (wid: int, oid: int, epoch: int)) {
      if (!running) {
        return;
      }
      HandleWrite(w.wid, w.oid, w.epoch);
      AfterEvent();
    }
    on ePgFull do (f: bool) {
      pgFull = f;
    }
    on ePgOthers do (n: int) {
      others = n;
      if (running) {
        ResumeCreatingPg();
      }
    }
    on eRecoveryTick do (t: (e: int, inc: int)) {
      if (!running || !hasPg || t.inc != incarnation) {
        return;
      }
      RecoveryTick(t.e);
      AfterEvent();
    }
    on eRequeueWrite do (r: (w: tW, inc: int)) {
      if (!running || r.inc != incarnation) {
        return;
      }
      HandleWrite(r.w.wid, r.w.oid, r.w.epoch);
      AfterEvent();
    }
    on eRequeueMsg do (r: (m: tMsg, inc: int)) {
      if (!running || r.inc != incarnation) {
        return;
      }
      HandleMsg(r.m);
      AfterEvent();
    }
    on eFinishRecovery do (t: (e: int, inc: int)) {
      if (!running || !hasPg || t.inc != incarnation || lpr > t.e) {
        return;
      }
      // PG::_finish_recovery
      if (Test(B_CLEAN) && !deleting) {
        PurgeStrays();
      }
      AfterEvent();
    }
    on ePgCrash do {
      running = false;
      incarnation = incarnation + 1;
      pendingCreate = false;
      heldWrites = default(seq[tW]);          // the OSD's slot waiters die with it
      ClearMemory();
    }
    on ePgRestart do {
      running = true;
      osdActive = false;
      Load();
    }
  }

  // ================================================================ the chart

  // PeeringState::handle_event
  fun Handle(ev: tEvt) {
    var e: tEvt;
    Deliver(ev);
    while (sizeof(posted) > 0) {
      e = posted[0];
      posted -= (0);
      Deliver(e);
    }
  }

  fun Post(ev: tEvt) {
    posted += (sizeof(posted), ev);
  }

  fun Deliver(ev: tEvt) {
    var i: int;
    var s: tS;
    var r: tReaction;
    var res: tResult;
    print format("osd.{0} e{1} {2} in {3}", me, pgEpoch, EventName(ev.e), StateName(Innermost()));
    if (sizeof(chart) == 0) {
      return;
    }
    i = sizeof(chart) - 1;
    while (i >= 0) {
      s = chart[i];
      if (cfg.advMapFullCheck && ev.e == E_AdvMap &&
          (s == S_WaitLocalRecoveryReserved || s == S_WaitRemoteRecoveryReserved)) {
        res = R70670(ev);
        if (res != RES_FORWARD) {
          return;
        }
        i = i - 1;
        continue;
      }
      if (cfg.resetIgnoresCommands && s == S_Reset && IsCommand(ev.e)) {
        return;
      }
      if ((cfg.grantFromCheck || cfg.deferWhileWaiting) && Proposed(s, ev)) {
        return;
      }
      r = Reaction(s, ev.e);
      if (r.kind == RK_TRANSITION) {
        if (r.target == S_Crashed) {
          announce mCrashed, (osd = me, where = StateName(Innermost()), ev = ev.e);
          assert false, format("osd.{0} e{1}: we got a bad state machine event: {2} in {3} (lpr {4})",
                               me, pgEpoch, EventName(ev.e), StateName(Innermost()), lpr);
        }
        Transit(s, r.target, ev.e);
        return;
      }
      if (r.kind == RK_DISCARD) {
        return;
      }
      if (r.kind == RK_CUSTOM) {
        res = Custom(s, ev);
        if (res != RES_FORWARD) {
          return;
        }
      }
      i = i - 1;
    }
  }

  // proposed reactions (tCfg's proposed fixes); true if the event was consumed
  fun Proposed(s: tS, ev: tEvt): bool {
    var i: int;
    var grant: bool;
    grant = ev.e == E_RemoteBackfillReserved || ev.e == E_RemoteRecoveryReserved;
    if (cfg.grantFromCheck && grant && ev.m.kind == K_RESERVE) {
      if (s == S_WaitRemoteBackfillReserved) {
        return resvAt == 0 || ev.m.src != resvBackfill[resvAt - 1];
      }
      if (s == S_WaitRemoteRecoveryReserved) {
        return resvAt == 0 || ev.m.src != resvRecovery[resvAt - 1];
      }
      if (s == S_Active) {
        return true;                          // a grant for a round that is over
      }
    }
    if (cfg.deferWhileWaiting) {
      if (s == S_WaitRemoteRecoveryReserved && ev.e == E_DeferRecovery) {
        while (i < resvAt) {
          SendResv(resvRecovery[i], false, RES_RELEASE, 0);
          i = i + 1;
        }
        CancelLocalReservation();
        Set(B_RECOVERY_WAIT);
        Queue(Evt(E_DoRecovery));
        Transit(S_WaitRemoteRecoveryReserved, S_NotRecovering, ev.e);
        return true;
      }
      if (s == S_WaitRemoteBackfillReserved && ev.e == E_DeferBackfill) {
        RetryBackfill();
        Transit(S_WaitRemoteBackfillReserved, S_NotBackfilling, ev.e);
        return true;
      }
      if (s == S_RepWaitRecoveryReserved && ev.e == E_RecoveryDone) {
        spaceReserved = false;
        CancelRemoteReservation();
        Transit(S_RepWaitRecoveryReserved, S_RepNotRecovering, ev.e);
        return true;
      }
    }
    return false;
  }

  // the events Started discards through react(const event_base&)
  fun IsCommand(e: tE): bool {
    return e == E_SetForceRecovery || e == E_UnsetForceRecovery || e == E_SetForceBackfill ||
           e == E_UnsetForceBackfill || e == E_RequestScrub;
  }

  fun IsIn(s: tS): bool {
    var i: int;
    while (i < sizeof(chart)) {
      if (chart[i] == s) {
        return true;
      }
      i = i + 1;
    }
    return false;
  }

  fun Innermost(): tS {
    if (sizeof(chart) == 0) {
      return S_NONE;
    }
    return chart[sizeof(chart) - 1];
  }

  // transit<Target>() from state src
  fun Transit(src: tS, tgt: tS, why: tE) {
    var anc: set[tS];
    var lca: tS;
    var s: tS;
    var path: seq[tS];
    s = Parent(tgt);
    while (s != S_NONE) {
      anc += (s);
      s = Parent(s);
    }
    lca = Parent(src);
    while (lca != S_NONE && !(lca in anc)) {
      lca = Parent(lca);
    }
    // exit, innermost first, every state inside lca
    while (sizeof(chart) > 0 && chart[sizeof(chart) - 1] != lca) {
      s = chart[sizeof(chart) - 1];
      Exit(s);
      chart -= (sizeof(chart) - 1);
    }
    // enter the target from lca down, then its initial states
    s = tgt;
    while (s != lca) {
      path += (0, s);
      s = Parent(s);
    }
    foreach (s in path) {
      EnterState(s, why);
    }
    s = InitialOf(tgt);
    while (s != S_NONE) {
      EnterState(s, why);
      s = InitialOf(s);
    }
  }

  fun EnterState(s: tS, why: tE) {
    chart += (sizeof(chart), s);
    print format("osd.{0} e{1} enter {2} ({3})", me, pgEpoch, StateName(s), EventName(why));
    if (Parent(s) == S_Active || Parent(s) == S_ReplicaActive || Parent(s) == S_ToDelete) {
      send env, eNoteState, (osd = me, st = s);
    }
    Enter(s);
  }

  // terminate(): the PG is deleted
  fun Terminate() {
    while (sizeof(chart) > 0) {
      Exit(chart[sizeof(chart) - 1]);
      chart -= (sizeof(chart) - 1);
    }
    hasPg = false;
    info = EmptyInfo();
    log = default(seq[tEntry]);
    missing = default(map[int, tMiss]);
    objs = default(map[int, tObjv]);
    pi = EmptyPI();
    ClearMemory();
  }


  // ================================================================ OSD glue

  // PG load: init_from_disk_state, handle_initialize
  fun Load() {
    ClearMemory();
    if (!hasPg) {
      return;
    }
    up = UpSet(maps[pgEpoch]);
    acting = ActingSet(maps[pgEpoch]);
    lpr = pgEpoch;
    EnterState(S_Initial, E_NullEvt);
    Handle(Evt(E_Initialize));
  }

  fun ClearMemory() {
    chart = default(seq[tS]);
    posted = default(seq[tEvt]);
    bits = default(set[tBit]);
    sendNotify = false;
    wantActing = default(seq[int]);
    ClearPrimaryState();
    queuedWrites = default(seq[tW]);
    blockedWrites = default(seq[tW]);
    inFlight = default(map[int, (wid: int, oid: int, waiting: set[int])]);
    waitingPeered = default(seq[tMsg]);
    recoveryQueued = false;
    backfillReserving = false;
    backfillReservedFlag = false;
    newBackfill = false;
    deleting = false;
    localSlot = false;
    spaceReserved = false;
    pgFull = false;
    prior = default(tPrior);
    infoRequested = default(set[int]);
    getLogHaveMsg = false;
    allReplicasActivated = false;
    resvRecovery = default(seq[int]);
    resvBackfill = default(seq[int]);
    resvAt = 0;
    RecoveryClear();
  }

  // PeeringState::clear_primary_state
  fun ClearPrimaryState() {
    strays = default(set[int]);
    peerMissingRequested = default(set[int]);
    peerInfo = default(map[int, tInfo]);
    peerMissing = default(map[int, map[int, tMiss]]);
    peerLcod = default(map[int, int]);
    peerActivated = default(set[int]);
    minLcod = 0;
    mightHaveUnfound = default(set[int]);
    needUpThru = false;
    missingLoc = default(map[int, set[int]]);
    // clear_recovery_state
    asyncTargets = default(set[int]);
    backfillTargets = default(set[int]);
    peerLb = default(map[int, int]);
    arb = default(set[int]);
  }

  fun Store(): tStore {
    return (log = log, tail = info.tail, lu = info.lu, missing = missing, objs = objs, failed = "");
  }

  fun SetStore(st: tStore) {
    var oid: int;
    assert st.failed == "", format("osd.{0}: ceph_assert: {1}", me, st.failed);
    log = st.log;
    info.tail = st.tail;
    info.lu = st.lu;
    missing = st.missing;
    foreach (oid in keys(objs)) {
      if (!(oid in st.objs)) {
        RemoveObj(oid);
      }
    }
    info.lc = LastComplete(log, info.tail, info.lu, missing);
  }

  // the object store: every change is announced for the durability spec
  fun SetObj(oid: int, ob: tObjv) {
    objs[oid] = ob;
    announce mObj, (osd = me, oid = oid, obj = ob, valid = true);
  }
  fun RemoveObj(oid: int) {
    if (oid in objs) {
      objs -= (oid);
      announce mObj, (osd = me, oid = oid, obj = default(tObjv), valid = false);
    }
  }

  fun IsPrimary(): bool {
    return PrimaryOf(acting) == me;
  }

  fun Primary(): int {
    return PrimaryOf(acting);
  }

  fun Map(): tMap {
    return maps[pgEpoch];
  }

  fun Set(b: tBit) { bits += (b); }
  fun Clear(b: tBit) { bits -= (b); }
  fun Test(b: tBit): bool { return b in bits; }

  // OSD::advance_pg: every map in order, then ActMap
  fun AdvanceTo(target: int) {
    var ev: tEvt;
    if (pgEpoch >= target) {
      return;
    }
    while (pgEpoch < target) {
      ev = Evt(E_AdvMap);
      ev.lastEpoch = pgEpoch;
      ev.newEpoch = pgEpoch + 1;
      pgEpoch = pgEpoch + 1;
      Handle(ev);
      if (!hasPg) {
        return;
      }
    }
    ActMapEvt();
  }

  fun ActMapEvt() {
    Handle(Evt(E_ActMap));
  }

  // queue a PGPeeringEvent for ourselves at the current epoch
  fun Queue(ev: tEvt) {
    send this, eQueued, (evt = ev, es = pgEpoch, er = pgEpoch, inc = incarnation);
  }

  // after each event: OSD::dequeue_peering_evt's up_thru check
  fun AfterEvent() {
    if (running && hasPg && needUpThru) {
      send osd, eUpThru, (want = info.h.sis, inc = incarnation);
    }
  }

  fun WantPgTemp(want: seq[int]) {
    send osd, ePgTempReq, (want = want, clear = false, inc = incarnation, forced = false);
  }

  // messages go out through the OSD's send rules, from the PG's epoch
  fun Send(m: tMsg) {
    if ((m.kind == K_QUERY_INFO || m.kind == K_QUERY_LOG || m.kind == K_NOTIFY || m.kind == K_INFO) &&
        (!osdActive || !Map().up[m.dst])) {
      return;
    }
    m.epSent = pgEpoch;
    m.pinc = incarnation;
    send osd, eOut, m;
  }

  // a peer message the OSD has the map for
  fun HandleMsg(m: tMsg) {
    var ev: tEvt;
    if (!hasPg) {
      NoPg(m);
      return;
    }
    if (m.kind == K_REPOP || m.kind == K_REPOP_REPLY || m.kind == K_PUSH || m.kind == K_PUSH_REPLY ||
        m.kind == K_PULL || m.kind == K_SCAN || m.kind == K_SCAN_DIGEST || m.kind == K_BACKFILL_PROGRESS ||
        m.kind == K_BACKFILL_FINISH || m.kind == K_BACKFILL_FINISH_ACK || m.kind == K_BACKFILL_REMOVE) {
      DataMsg(m);
      return;
    }
    // a peering event: PG::do_peering_event -> old_peering_evt
    if (cfg.staleFilter && (lpr > m.epSent || lpr > m.req)) {
      return;
    }
    if (m.kind == K_QUERY_INFO || m.kind == K_QUERY_LOG) {
      ev = EvtMsg(E_MQuery, m);
    } else if (m.kind == K_NOTIFY) {
      ev = EvtMsg(E_MNotifyRec, m);
    } else if (m.kind == K_LOG) {
      ev = EvtMsg(E_MLogRec, m);
    } else if (m.kind == K_INFO) {
      ev = EvtMsg(E_MInfoRec, m);
    } else if (m.kind == K_TRIM) {
      ev = EvtMsg(E_MTrim, m);
    } else if (m.kind == K_REMOVE) {
      ev = EvtMsg(E_DeleteStart, m);
    } else {
      ev = ReserveEvent(m);
    }
    Handle(ev);
  }

  // OSD::resume_creating_pg (from the OSD's tick, while active): with room
  // again, twiddle the acting set by a forced pg_temp so the PG peers again
  fun ResumeCreatingPg() {
    var num: int;
    var acting2: seq[int];
    var tw: seq[int];
    var o: int;
    if (cfg.maxPgs == 0 || !pendingCreate || !osdActive) {
      return;
    }
    num = others;
    if (hasPg) {
      num = num + 1;
    }
    if (num >= cfg.maxPgs) {
      return;
    }
    pendingCreate = false;
    acting2 = ActingSet(maps[newest]);
    if (sizeof(acting2) > 1) {
      tw += (0, acting2[0]);
    } else if (cfg.twiddleFix && sizeof(acting2) == 1) {
      // (proposed) a second up OSD, as OSDMonitor's "pg repeer" does
      tw = acting2;
      if (me != acting2[0]) {
        tw += (1, me);
      } else {
        o = 0;
        while (o < cfg.nOsds && sizeof(tw) == 1) {
          if (o != me && maps[newest].up[o]) {
            tw += (1, o);
          }
          o = o + 1;
        }
      }
    } else {
      // [x, CRUSH_ITEM_NONE]: _get_temp_osds drops the NONE for a replicated
      // pool, so this is the acting set the PG already has
      tw = acting2;
    }
    print format("osd.{0} e{1} resumes creating the PG: pg_temp {2}", me, newest, tw);
    send osd, ePgTempReq, (want = tw, clear = false, inc = incarnation, forced = true);
  }

  fun MapsHere(): bool {
    return Contains(UpSet(maps[newest]), me) || Contains(ActingSet(maps[newest]), me);
  }

  // OSD::handle_pg_query_nopg; OSD::handle_pg_create_info
  fun NoPg(m: tMsg) {
    var r: tMsg;
    if (m.kind == K_QUERY_INFO || m.kind == K_QUERY_LOG) {
      if (m.kind == K_QUERY_INFO) {
        r = Msg(K_NOTIFY, me, m.src, newest, m.epSent);
      } else {
        r = Msg(K_LOG, me, m.src, newest, m.epSent);
      }
      r.info.lb = LB_MAX();
      r.pinc = incarnation;
      send osd, eOut, r;
      return;
    }
    if (m.kind != K_LOG && m.kind != K_NOTIFY) {
      return;
    }
    if (!MapsHere()) {
      return;
    }
    // OSD::maybe_wait_for_max_pg
    if (cfg.maxPgs > 0 && others >= cfg.maxPgs) {
      pendingCreate = true;
      print format("osd.{0} e{1} withholds creation of the PG ({2} PGs)", me, newest, others);
      return;
    }
    hasPg = true;
    announce mHolds, me;
    info = EmptyInfo();
    info.h = m.info.h;
    log = default(seq[tEntry]);
    missing = default(map[int, tMiss]);
    objs = default(map[int, tObjv]);
    pi = m.pi;
    if (m.kind == K_LOG) {
      pgEpoch = m.req;
    } else {
      pgEpoch = m.epSent;
    }
    Load();
    ActMapEvt();
    AdvanceTo(newest);
    HandleMsg(m);
    ReleaseWrites();
  }


  // ================================================================ peering

  // PeeringState::should_restart_peering
  fun ShouldRestart(lm: tMap, m: tMap): bool {
    return NewInterval(lm, m) || (!lm.up[me] && m.up[me]);
  }

  // PeeringState::start_peering_interval
  fun StartPeeringInterval(lm: tMap, m: tMap) {
    var iv: tInterval;
    var wasPrimary: bool;
    wasPrimary = IsPrimary();
    lpr = m.epoch;
    up = UpSet(m);
    acting = ActingSet(m);
    if (up != acting) {
      Set(B_REMAPPED);
    } else {
      Clear(B_REMAPPED);
    }
    if (NewInterval(lm, m)) {
      iv = ClosedInterval(info.h.sis, info.h.lec, lm, m);
      pi = PiAdd(pi, iv);
      info.h.sis = m.epoch;
    }
    Clear(B_ACTIVE);
    Clear(B_PEERED);
    Clear(B_DOWN);
    Clear(B_RECOVERY_WAIT);
    Clear(B_RECOVERY_TOOFULL);
    Clear(B_RECOVERING);
    peerPurged = default(set[int]);
    arb = default(set[int]);
    send osd, ePgTempReq, (want = default(seq[int]), clear = true, inc = incarnation, forced = false);
    ClearPrimaryState();
    OnChange();
    assert !deleting, format("osd.{0} start_peering_interval while deleting", me);
    sendNotify = !IsPrimary();
    if (wasPrimary != IsPrimary()) {
      Clear(B_CLEAN);
    } else if (IsPrimary()) {
      Clear(B_CLEAN);
    }
  }

  // PeeringState::remove_down_peer_info
  fun RemoveDownPeerInfo(m: tMap) {
    var o: int;
    foreach (o in keys(peerInfo)) {
      if (!m.up[o]) {
        peerInfo -= (o);
        if (o in peerMissing) {
          peerMissing -= (o);
        }
        peerMissingRequested -= (o);
      }
    }
    foreach (o in peerPurged) {
      if (!m.up[o]) {
        peerPurged -= (o);
      }
    }
    CheckRecoverySources(m);
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

  // PeeringState::build_prior
  fun BuildPriorSet() {
    prior = BuildPrior(pi, info.h.les, up, acting, Map());
    if (prior.pgDown) {
      Set(B_DOWN);
    }
    needUpThru = cfg.upThruGate && Map().upThru[me] < info.h.sis;
  }

  // GetInfo::get_infos
  fun GetInfos() {
    var o: int;
    var m: tMsg;
    foreach (o in prior.probe) {
      if (o != me && !(o in peerInfo) && !(o in infoRequested) && Map().up[o]) {
        m = Msg(K_QUERY_INFO, me, o, pgEpoch, pgEpoch);
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
    if (!HasBeenUpSince(Map(), m.src, m.epSent)) {
      return false;
    }
    peerInfo[m.src] = m.info;
    mightHaveUnfound += (m.src);
    UpdateHistory(m.info.h);
    if (!Contains(up, m.src) && !Contains(acting, m.src)) {
      strays += (m.src);
      if (Test(B_CLEAN)) {
        PurgeStrays();
      }
    }
    return true;
  }

  // PeeringState::proc_master_log
  fun ProcMasterLog(m: tMsg) {
    var st: tStore;
    st = MergeLog(Store(), info.lb, m.log, m.tail, m.info.lu);
    SetStore(st);
    peerInfo[m.src] = m.info;
    mightHaveUnfound += (m.src);
    if (m.info.les > info.les) {
      info.les = m.info.les;
    }
    if (m.info.lis > info.lis) {
      info.lis = m.info.lis;
    }
    UpdateHistory(m.info.h);
    assert !cfg.historyLes || info.les >= info.h.les,
      format("osd.{0} proc_master_log: last_epoch_started {1} < history's {2}", me, info.les, info.h.les);
    peerMissing[m.src] = m.missing;
  }

  // PeeringState::proc_replica_log: the peer's info and missing as of our log
  fun ProcReplicaLog(m: tMsg) {
    var p: tInfo;
    var pst: tStore;
    var lu: int;
    p = m.info;
    pst = (log = m.log, tail = m.tail, lu = m.info.lu, missing = m.missing, objs = default(map[int, tObjv]), failed = "");
    if (m.info.lu >= info.tail && m.info.lu != info.lu) {
      lu = ProcReplicaLogLu(log, info.tail, m.info.lu, m.tail);
      pst.log = UpTo(m.log, lu);
      pst = MergeDivergent(pst, p.lb, After(m.log, lu), false);
      assert pst.failed == "", format("osd.{0} proc_replica_log from osd.{1}: {2}", me, m.src, pst.failed);
      if (lu < p.lu) {
        p.lu = lu;
      }
    }
    peerInfo[m.src] = p;
    peerMissing[m.src] = pst.missing;
    mightHaveUnfound += (m.src);
  }

  // PeeringState::choose_acting (replicated)
  fun ChooseActing(restrict: bool, pgTempOnly: bool): bool {
    var o: int;
    var all: map[int, tInfo];
    var auth: int;
    var prim: int;
    var oldest: int;
    var want: seq[int];
    var wantBackfill: set[int];
    var wantArb: set[int];
    var wantAsync: set[int];
    var r: (want: seq[int], backfill: set[int], arb: set[int]);
    var cur: tMap;
    cur = Map();
    all = peerInfo;
    all[me] = info;
    auth = FindBestInfo(all, cfg.nOsds, restrict, up, acting, me, cfg.historyLes);
    if (auth == -1) {
      if (up != acting) {
        wantActing = up;
        WantPgTemp(default(seq[int]));
      } else {
        assert sizeof(wantActing) == 0, format("osd.{0} choose_acting failed with want_acting set", me);
      }
      return false;
    }
    // select_replicated_primary
    prim = auth;
    if (sizeof(up) > 0 && PrimaryOf(up) in all && !Incomplete(all[PrimaryOf(up)]) &&
        all[PrimaryOf(up)].lu >= all[auth].tail) {
      prim = PrimaryOf(up);
    }
    oldest = all[prim].tail;
    if (all[auth].tail < oldest) {
      oldest = all[auth].tail;
    }
    foreach (o in up) {
      assert o in all, format("osd.{0} e{1} chooses an acting set without info from up osd.{2}", me, pgEpoch, o);
    }
    foreach (o in acting) {
      assert o in all, format("osd.{0} e{1} chooses an acting set without info from acting osd.{2}", me, pgEpoch, o);
    }
    r = CalcReplicatedActing(prim, oldest, cur.size, acting, up, all, cfg.nOsds, restrict);
    want = r.want;
    wantBackfill = r.backfill;
    wantArb = r.arb;
    // recoverable(): osd_allow_recovery_below_min_size, any one copy
    if (sizeof(want) == 0) {
      wantActing = default(seq[int]);
      return false;
    }
    r = ChooseAsyncRecovery(all, all[auth], want, wantArb);
    want = r.want;
    wantAsync = r.backfill;
    while (sizeof(want) > cur.size) {
      want -= (sizeof(want) - 1);
    }
    if (want != acting) {
      wantActing = want;
      if (want == up) {
        assert sizeof(wantBackfill) == 0, format("osd.{0} wants up {1} with backfill {2}", me, up, wantBackfill);
        WantPgTemp(default(seq[int]));
      } else {
        WantPgTemp(want);
      }
      return false;
    }
    if (pgTempOnly) {
      return true;
    }
    wantActing = default(seq[int]);
    arb = wantArb;
    assert sizeof(backfillTargets) == 0 || backfillTargets == wantBackfill,
      format("osd.{0} choose_acting: backfill tgts {1} became {2}", me, backfillTargets, wantBackfill);
    if (sizeof(backfillTargets) == 0) {
      backfillTargets = wantBackfill;
    }
    assert sizeof(asyncTargets) == 0 || asyncTargets == wantAsync || !NeedsRecovery(),
      format("osd.{0} choose_acting: async recovery tgts {1} became {2}", me, asyncTargets, wantAsync);
    if (sizeof(asyncTargets) == 0 || !NeedsRecovery()) {
      asyncTargets = wantAsync;
    }
    foreach (o in wantBackfill) {
      assert !(o in strays), format("osd.{0} backfill target osd.{1} is a stray", me, o);
    }
    authLogShard = auth;
    return true;
  }

  // PeeringState::choose_async_recovery_replicated: the costliest peers to
  // bring up to date by log recovery leave the acting set (while it keeps
  // min_size members) and recover in the background
  fun ChooseAsyncRecovery(all: map[int, tInfo], auth: tInfo, want: seq[int], wantArb: set[int]): tActing {
    var r: tActing;
    var cost: map[int, int];
    var o: int;
    var c: int;
    var best: int;
    var i: int;
    var cand: seq[int];
    r.want = want;
    r.arb = wantArb;
    foreach (o in want) {
      if (!(o in strays) && Map().up[o]) {
        c = VNum(auth.lu) - VNum(all[o].lu);
        if (c < 0) {
          c = 0 - c;
        }
        if (c > cfg.asyncMinCost) {
          cost[o] = c;
        }
      }
    }
    // highest cost first; among equal costs the higher osd
    while (sizeof(cost) > 0) {
      best = -1;
      foreach (o in keys(cost)) {
        if (best == -1 || cost[o] > cost[best] || (cost[o] == cost[best] && o > best)) {
          best = o;
        }
      }
      if (sizeof(r.want) <= Map().minSize) {
        return r;
      }
      cand = default(seq[int]);
      i = 0;
      while (i < sizeof(r.want)) {
        if (r.want[i] != best) {
          cand += (sizeof(cand), r.want[i]);
        }
        i = i + 1;
      }
      r.want = cand;
      r.backfill += (best);
      cost -= (best);
    }
    return r;
  }

  // PeeringState::share_pg_info
  fun SharePgInfo() {
    var o: int;
    var m: tMsg;
    var p: tInfo;
    foreach (o in arb) {
      if (o != me) {
        if (o in peerInfo) {
          p = peerInfo[o];
          p.les = info.les;
          p.lis = info.lis;
          p.h = MergeHistory(p.h, info.h);
          peerInfo[o] = p;
        }
        m = Msg(K_INFO, me, o, pgEpoch, pgEpoch);
        m.info = info;
        Send(m);
      }
    }
  }

  // PeeringState::fulfill_query
  fun FulfillQuery(m: tMsg) {
    var r: tMsg;
    UpdateHistory(m.info.h);
    if (m.kind == K_QUERY_INFO) {
      r = Msg(K_NOTIFY, me, m.src, pgEpoch, m.epSent);
      r.info = info;
      r.pi = pi;
    } else {
      r = Msg(K_LOG, me, m.src, pgEpoch, m.epSent);
      r.info = info;
      r.log = log;
      r.tail = info.tail;
      r.missing = missing;
    }
    Send(r);
  }

  fun SendNotify() {
    var m: tMsg;
    m = Msg(K_NOTIFY, me, Primary(), pgEpoch, pgEpoch);
    m.info = info;
    m.pi = pi;
    Send(m);
  }

  // ----------------------------------------------------------- states

  fun En_Crashed() {}
  fun Ex_Crashed() {}
  fun En_Initial() {}
  fun Ex_Initial() {}

  fun En_Reset() {
    lpr = pgEpoch;                      // set_last_peering_reset
  }
  fun Ex_Reset() {}
  fun R_Reset_QueryState(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Reset_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Reset_AdvMap(ev: tEvt): tResult {
    var lm: tMap;
    var m: tMap;
    lm = maps[ev.lastEpoch];
    m = maps[ev.newEpoch];
    if (ShouldRestart(lm, m)) {
      StartPeeringInterval(lm, m);
    }
    RemoveDownPeerInfo(m);
    return RES_DISCARD;
  }
  fun R_Reset_ActMap(ev: tEvt): tResult {
    if (sendNotify && Primary() >= 0) {
      SendNotify();
    }
    Transit(S_Reset, S_Started, ev.e);
    return RES_TRANSIT;
  }
  fun R_Reset_IntervalFlush(ev: tEvt): tResult { return RES_DISCARD; }

  fun En_Started() {}
  fun Ex_Started() {
    Clear(B_WAIT);
    Clear(B_LAGGY);
  }
  fun R_Started_QueryState(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Started_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Started_IntervalFlush(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Started_AdvMap(ev: tEvt): tResult {
    var lm: tMap;
    var m: tMap;
    lm = maps[ev.lastEpoch];
    m = maps[ev.newEpoch];
    if (ShouldRestart(lm, m)) {
      Post(ev);
      Transit(S_Started, S_Reset, ev.e);
      return RES_TRANSIT;
    }
    RemoveDownPeerInfo(m);
    return RES_DISCARD;
  }

  fun En_Start() {
    if (IsPrimary()) {
      Post(Evt(E_MakePrimary));
    } else {
      Post(Evt(E_MakeStray));
    }
  }
  fun Ex_Start() {}

  fun En_Primary() {
    assert sizeof(wantActing) == 0, format("osd.{0} enters Primary with want_acting {1}", me, wantActing);
    if (info.h.les == 0) {
      Set(B_CREATING);
    }
  }
  fun Ex_Primary() {
    wantActing = default(seq[int]);
    PrimaryExitGlue();
    Clear(B_CREATING);
  }
  fun R_Primary_ActMap(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Primary_MNotifyRec(ev: tEvt): tResult {
    ProcReplicaNotify(ev.m);
    return RES_DISCARD;
  }
  fun R_Primary_SetForceRecovery(ev: tEvt): tResult { SetForce(true, true); return RES_DISCARD; }
  fun R_Primary_UnsetForceRecovery(ev: tEvt): tResult { SetForce(true, false); return RES_DISCARD; }
  fun R_Primary_SetForceBackfill(ev: tEvt): tResult { SetForce(false, true); return RES_DISCARD; }
  fun R_Primary_UnsetForceBackfill(ev: tEvt): tResult { SetForce(false, false); return RES_DISCARD; }
  fun R_Primary_RequestScrub(ev: tEvt): tResult { return RES_DISCARD; }

  fun En_WaitActingChange() {}
  fun Ex_WaitActingChange() {}
  fun R_WaitActingChange_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_WaitActingChange_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_WaitActingChange_AdvMap(ev: tEvt): tResult {
    var o: int;
    var m: tMap;
    m = maps[ev.newEpoch];
    foreach (o in wantActing) {
      if (!m.up[o]) {
        Post(ev);
        Transit(S_WaitActingChange, S_Reset, ev.e);
        return RES_TRANSIT;
      }
    }
    return RES_FORWARD;
  }
  fun R_WaitActingChange_MLogRec(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_WaitActingChange_MInfoRec(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_WaitActingChange_MNotifyRec(ev: tEvt): tResult { return RES_DISCARD; }

  fun En_Peering() {
    assert !Test(B_ACTIVE) && !Test(B_PEERED), format("osd.{0} enters Peering peered", me);
    assert !Test(B_PEERING), format("osd.{0} enters Peering while peering", me);
    assert IsPrimary(), format("osd.{0} enters Peering but is not primary", me);
    Set(B_PEERING);
  }
  fun Ex_Peering() {
    Clear(B_PEERING);
  }
  fun R_Peering_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_Peering_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Peering_AdvMap(ev: tEvt): tResult {
    var m: tMap;
    m = maps[ev.newEpoch];
    if (AffectedByMap(prior, m)) {
      Post(ev);
      Transit(S_Peering, S_Reset, ev.e);
      return RES_TRANSIT;
    }
    if (needUpThru && m.upThru[me] >= info.h.sis) {
      needUpThru = false;
    }
    return RES_FORWARD;
  }

  fun En_GetInfo() {
    infoRequested = default(set[int]);
    BuildPriorSet();
    GetInfos();
    if (prior.pgDown) {
      Post(Evt(E_IsDown));
    } else if (sizeof(infoRequested) == 0) {
      Post(Evt(E_GotInfo));
    }
  }
  fun Ex_GetInfo() {}
  fun R_GetInfo_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_GetInfo_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  // Ceph erases the peer from peer_info_requested before
  // proc_replica_notify, which may discard the notify
  fun R_GetInfo_MNotifyRec(ev: tEvt): tResult {
    var o: int;
    var old: int;
    var keep: set[int];
    if (!cfg.getInfoKeepsRequest) {
      infoRequested -= (ev.m.src);
    }
    old = info.h.les;
    if (ProcReplicaNotify(ev.m)) {
      infoRequested -= (ev.m.src);
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
        Post(Evt(E_GotInfo));
      }
    }
    return RES_DISCARD;
  }

  fun En_GetLog() {
    var m: tMsg;
    var o: int;
    var since: int;
    var best: tInfo;
    var ri: tInfo;
    getLogHaveMsg = false;
    if (!ChooseActing(false, false)) {
      if (sizeof(wantActing) > 0) {
        Post(Evt(E_NeedActingChange));
      } else {
        Post(Evt(E_IsIncomplete));
      }
      return;
    }
    if (authLogShard == me) {
      Post(Evt(E_GotLog));
      return;
    }
    best = peerInfo[authLogShard];
    if (info.lu < best.tail) {
      Post(Evt(E_IsIncomplete));
      return;
    }
    // how much log to request
    since = info.lu;
    foreach (o in arb) {
      if (o != me) {
        ri = peerInfo[o];
        if (ri.lu < info.tail && ri.lu >= best.tail && ri.lu < since) {
          since = ri.lu;
        }
      }
    }
    m = Msg(K_QUERY_LOG, me, authLogShard, pgEpoch, pgEpoch);
    m.info = info;
    m.info.lu = since;              // pg_query_t::since
    Send(m);
  }
  fun Ex_GetLog() {}
  fun R_GetLog_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_GetLog_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_GetLog_MLogRec(ev: tEvt): tResult {
    assert !getLogHaveMsg, format("osd.{0} GetLog got a second log", me);
    if (ev.m.src != authLogShard) {
      return RES_DISCARD;
    }
    getLogMsg = ev.m;
    getLogHaveMsg = true;
    Post(Evt(E_GotLog));
    return RES_DISCARD;
  }
  fun R_GetLog_GotLog(ev: tEvt): tResult {
    if (getLogHaveMsg) {
      ProcMasterLog(getLogMsg);
    }
    Transit(S_GetLog, S_GetMissing, ev.e);
    return RES_TRANSIT;
  }
  fun R_GetLog_AdvMap(ev: tEvt): tResult {
    if (!maps[ev.newEpoch].up[authLogShard]) {
      Post(ev);
      Transit(S_GetLog, S_Reset, ev.e);
      return RES_TRANSIT;
    }
    return RES_FORWARD;
  }

  fun En_GetMissing() {
    var o: int;
    var p: tInfo;
    var m: tMsg;
    assert sizeof(arb) > 0, format("osd.{0} GetMissing with no acting_recovery_backfill", me);
    peerMissingRequested = default(set[int]);
    foreach (o in arb) {
      if (o != me) {
        assert o in peerInfo, format("osd.{0} GetMissing without info from osd.{1}", me, o);
        p = peerInfo[o];
        peerMissing[o] = default(map[int, tMiss]);
        if (VNum(p.lu) == 0 && p.lu == p.tail) {
          // is_empty: no pg data, nothing divergent
        } else if (p.lu < info.tail) {
          // not contiguous: will backfill
        } else if (p.lb == 0) {
          // will fully backfill
        } else if (p.lu == p.lc && p.lu == info.lu) {
          // no missing, identical log
        } else {
          assert p.lu >= info.tail, format("osd.{0}: osd.{1} last_update below our tail", me, o);
          m = Msg(K_QUERY_LOG, me, o, pgEpoch, pgEpoch);
          m.info = info;
          m.info.lu = Ver(p.les, 0);    // since: its last_epoch_started
          Send(m);
          peerMissingRequested += (o);
        }
      }
    }
    if (sizeof(peerMissingRequested) == 0) {
      if (needUpThru) {
        Post(Evt(E_NeedUpThru));
      } else {
        Post(ActivateEvt(pgEpoch));
      }
    }
  }
  fun Ex_GetMissing() {}
  fun R_GetMissing_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_GetMissing_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_GetMissing_MLogRec(ev: tEvt): tResult {
    peerMissingRequested -= (ev.m.src);
    ProcReplicaLog(ev.m);
    if (sizeof(peerMissingRequested) == 0) {
      if (needUpThru) {
        Post(Evt(E_NeedUpThru));
      } else {
        Post(ActivateEvt(pgEpoch));
      }
    }
    return RES_DISCARD;
  }

  fun ActivateEvt(e: int): tEvt {
    var ev: tEvt;
    ev = Evt(E_Activate);
    ev.a = e;
    return ev;
  }

  fun En_WaitUpThru() {}
  fun Ex_WaitUpThru() {}
  fun R_WaitUpThru_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_WaitUpThru_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_WaitUpThru_ActMap(ev: tEvt): tResult {
    if (!needUpThru) {
      Post(ActivateEvt(pgEpoch));
    }
    return RES_FORWARD;
  }
  fun R_WaitUpThru_MLogRec(ev: tEvt): tResult {
    peerMissing[ev.m.src] = ev.m.missing;
    peerInfo[ev.m.src] = ev.m.info;
    return RES_DISCARD;
  }

  fun En_Down() {
    Clear(B_PEERING);
    Set(B_DOWN);
  }
  fun Ex_Down() {
    Clear(B_DOWN);
  }
  fun R_Down_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_Down_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Down_MNotifyRec(ev: tEvt): tResult {
    var old: int;
    assert IsPrimary(), format("osd.{0} Down but not primary", me);
    old = info.h.les;
    if (!(ev.m.src in peerInfo) && HasBeenUpSince(Map(), ev.m.src, ev.m.epSent)) {
      UpdateHistory(ev.m.info.h);
    }
    if (info.h.les > old) {
      Clear(B_DOWN);
      Set(B_PEERING);
      Transit(S_Down, S_GetInfo, ev.e);
      return RES_TRANSIT;
    }
    return RES_DISCARD;
  }

  fun En_Incomplete() {
    Clear(B_PEERING);
    Set(B_INCOMPLETE);
  }
  fun Ex_Incomplete() {
    Clear(B_INCOMPLETE);
  }
  fun R_Incomplete_AdvMap(ev: tEvt): tResult {
    if (maps[ev.lastEpoch].minSize > maps[ev.newEpoch].minSize) {
      Post(ev);
      Transit(S_Incomplete, S_Reset, ev.e);
      return RES_TRANSIT;
    }
    return RES_FORWARD;
  }
  fun R_Incomplete_MNotifyRec(ev: tEvt): tResult {
    if (ProcReplicaNotify(ev.m)) {
      Transit(S_Incomplete, S_GetLog, ev.e);
      return RES_TRANSIT;
    }
    return RES_DISCARD;
  }
  fun R_Incomplete_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Incomplete_QueryState(ev: tEvt): tResult { return RES_FORWARD; }


  // ================================================================ activation

  fun Writeable(): bool {
    return sizeof(acting) >= Map().minSize;
  }

  fun IsActing(o: int): bool {
    return Contains(acting, o);
  }

  // PeeringState::needs_recovery
  fun NeedsRecovery(): bool {
    var o: int;
    if (sizeof(missing) > 0) {
      return true;
    }
    foreach (o in arb) {
      if (o != me && o in peerMissing && sizeof(peerMissing[o]) > 0) {
        return true;
      }
    }
    return false;
  }

  // PeeringState::needs_backfill
  fun NeedsBackfill(): bool {
    var o: int;
    foreach (o in backfillTargets) {
      if (o in peerInfo && peerInfo[o].lb != LB_MAX()) {
        return true;
      }
    }
    return false;
  }

  // ------------------------------------------------------------ MissingLoc

  fun MLAddActiveMissing(ms: map[int, tMiss]) {
    var oid: int;
    foreach (oid in keys(ms)) {
      needsRecov[oid] = ms[oid].need;
    }
  }

  // MissingLoc::add_source_info
  fun MLAddSource(src: int, oi: tInfo, oms: map[int, tMiss]): bool {
    var oid: int;
    var found: bool;
    var locs: set[int];
    foreach (oid in keys(needsRecov)) {
      if (oi.lu >= needsRecov[oid] && oid <= oi.lb && !(oid in oms)) {
        locs = default(set[int]);
        if (oid in missingLoc) {
          locs = missingLoc[oid];
        }
        if (!(src in locs)) {
          locs += (src);
          missingLoc[oid] = locs;
          found = true;
        }
      }
    }
    return found;
  }

  fun MLAddLocation(oid: int, o: int) {
    var locs: set[int];
    if (oid in missingLoc) {
      locs = missingLoc[oid];
    }
    locs += (o);
    missingLoc[oid] = locs;
  }

  fun MLRecovered(oid: int) {
    if (oid in needsRecov) {
      needsRecov -= (oid);
    }
    if (oid in missingLoc) {
      missingLoc -= (oid);
    }
  }

  fun IsUnfound(oid: int): bool {
    return oid in needsRecov && (!(oid in missingLoc) || sizeof(missingLoc[oid]) == 0);
  }

  fun NumUnfound(): int {
    var n: int;
    var oid: int;
    foreach (oid in keys(needsRecov)) {
      if (IsUnfound(oid)) {
        n = n + 1;
      }
    }
    return n;
  }

  // MissingLoc::check_recovery_sources (via remove_down_peer_info)
  fun CheckRecoverySources(m: tMap) {
    var oid: int;
    var o: int;
    var locs: set[int];
    var keep: set[int];
    foreach (oid in keys(missingLoc)) {
      locs = missingLoc[oid];
      keep = default(set[int]);
      foreach (o in locs) {
        if (m.up[o]) {
          keep += (o);
        }
      }
      missingLoc[oid] = keep;
    }
    // PrimaryLogPG::check_recovery_sources: pulls from peers now down
    foreach (oid in keys(pulling)) {
      if (!m.up[pulling[oid]]) {
        pulling -= (oid);
        CancelPull(oid);
      }
    }
  }

  // PeeringState::search_for_missing
  fun SearchForMissing(src: int, oi: tInfo, oms: map[int, tMiss]): bool {
    var found: bool;
    var m: tMsg;
    found = MLAddSource(src, oi, oms);
    if (found && oi.lu != 0) {
      m = Msg(K_INFO, me, src, pgEpoch, pgEpoch);
      m.info = oi;
      Send(m);
    }
    return found;
  }

  // PeeringState::discover_all_missing: FULLLOG queries to peers that might have it
  fun DiscoverAllMissing(): bool {
    var o: int;
    var asked: bool;
    var m: tMsg;
    foreach (o in mightHaveUnfound) {
      if (Map().up[o] && !(o in peerPurged) && !(o in peerMissing) && !(o in peerMissingRequested) &&
          !(o in peerInfo && (VNum(peerInfo[o].lu) == 0 || peerInfo[o].h.created == 0))) {
        peerMissingRequested += (o);
        m = Msg(K_QUERY_LOG, me, o, pgEpoch, pgEpoch);
        m.info = info;
        m.info.lu = 0;                 // FULLLOG
        Send(m);
        asked = true;
      }
    }
    return asked;
  }

  // PeeringState::build_might_have_unfound
  fun BuildMightHaveUnfound() {
    var o: int;
    mightHaveUnfound = pi.all;
    mightHaveUnfound -= (me);
    foreach (o in keys(peerInfo)) {
      mightHaveUnfound += (o);
    }
  }

  // PeeringState::activate
  fun Activate(ae: int) {
    var o: int;
    var p: tInfo;
    var pm: map[int, tMiss];
    var m: tMsg;
    var sendLog: bool;
    var i: int;
    var complete: set[int];
    var ev: tEvt;
    assert !Test(B_ACTIVE) && !Test(B_PEERED), format("osd.{0} activates while peered", me);
    Clear(B_DOWN);
    sendNotify = false;
    if (IsPrimary()) {
      if (Writeable()) {
        assert !cfg.historyLes || info.les <= ae, format("osd.{0} activate: last_epoch_started {1} > {2}", me, info.les, ae);
        info.les = ae;
        info.lis = info.h.sis;
        pgCommittedTo = info.lu;
      }
    } else if (IsActing(me)) {
      if (info.les < ae) {
        info.les = ae;
        info.lis = info.h.sis;
        pgCommittedTo = info.lu;
      }
    }
    minLcod = 0;
    needUpThru = false;
    // ActivateCommitted on commit
    ev = Evt(E_ActivateCommitted);
    ev.a = ae;
    Queue(ev);
    info.lc = LastComplete(log, info.tail, info.lu, missing);
    if (!IsPrimary()) {
      return;
    }
    foreach (o in arb) {
      if (o != me) {
        assert o in peerInfo, format("osd.{0} activating without info from osd.{1}", me, o);
        p = peerInfo[o];
        pm = default(map[int, tMiss]);
        if (o in peerMissing) {
          pm = peerMissing[o];
        }
        sendLog = false;
        if (p.lu == info.lu) {
          if (!(VNum(p.lu) == 0 && p.lu == p.tail)) {
            m = Msg(K_INFO, me, o, pgEpoch, pgEpoch);
            m.info = info;
            Send(m);
          } else {
            m = Msg(K_LOG, me, o, pgEpoch, lpr);
            m.info = info;
            m.tail = info.lu;
            sendLog = true;
          }
        } else if (info.tail > p.lu || p.lb == 0 || (o in backfillTargets && p.lb == LB_MAX())) {
          // backfill
          p.lu = info.lu;
          p.lc = info.lu;
          p.lb = 0;
          p.les = info.les;
          p.lis = info.lis;
          p.h = info.h;
          m = Msg(K_LOG, me, o, pgEpoch, lpr);
          m.log = log;
          m.tail = info.tail;
          p.tail = info.tail;
          m.info = p;
          pm = default(map[int, tMiss]);
          sendLog = true;
        } else {
          // catch up
          m = Msg(K_LOG, me, o, pgEpoch, lpr);
          m.info = info;
          m.log = After(log, p.lu);
          m.tail = TailFor(log, info.tail, p.lu);
          sendLog = true;
        }
        if (sendLog) {
          if (peerInfo[o].h.created == 0) {
            m.pi = pi;
          }
          if (p.lb != 0) {
            i = 0;
            while (i < sizeof(m.log)) {
              if (m.log[i].oid <= p.lb) {
                pm = AddNextEvent(pm, m.log[i]);
              }
              i = i + 1;
            }
          }
          Send(m);
        }
        p.lu = info.lu;
        if (sizeof(pm) == 0) {
          p.lc = p.lu;
        }
        peerInfo[o] = p;
        peerMissing[o] = pm;
      }
    }
    // missing_loc
    needsRecov = default(map[int, int]);
    missingLoc = default(map[int, set[int]]);
    MLAddActiveMissing(missing);
    if (sizeof(missing) == 0) {
      complete += (me);
    }
    foreach (o in arb) {
      if (o != me) {
        MLAddActiveMissing(peerMissing[o]);
        if (sizeof(peerMissing[o]) == 0 && peerInfo[o].lb == LB_MAX()) {
          complete += (o);
        }
      }
    }
    mightHaveUnfound = default(set[int]);
    if (NeedsRecovery()) {
      if (sizeof(complete) + 1 == sizeof(arb)) {
        // add_batch_sources_info
        foreach (i in keys(needsRecov)) {
          foreach (o in complete) {
            MLAddLocation(i, o);
          }
        }
      } else {
        MLAddSource(me, info, missing);
        foreach (o in arb) {
          if (o != me) {
            MLAddSource(o, peerInfo[o], peerMissing[o]);
          }
        }
      }
      foreach (o in keys(peerMissing)) {
        if (!(o in arb) && o in peerInfo) {
          SearchForMissing(o, peerInfo[o], peerMissing[o]);
        }
      }
      BuildMightHaveUnfound();
      DiscoverAllMissing();
    }
    if (Map().size > sizeof(acting)) {
      Set(B_UNDERSIZED);
    }
    Set(B_ACTIVATING);
  }

  // Active::all_activated_and_committed
  fun AllActivatedAndCommitted() {
    assert IsPrimary(), format("osd.{0} all_activated_and_committed but not primary", me);
    Post(Evt(E_AllReplicasActivated));
  }

  fun En_Active() {
    var o: int;
    // remote_shards_to_reserve_recovery / _backfill, in osd order
    resvRecovery = default(seq[int]);
    resvBackfill = default(seq[int]);
    o = 0;
    while (o < cfg.nOsds) {
      if (o != me && o in arb) {
        resvRecovery += (sizeof(resvRecovery), o);
      }
      if (o != me && o in backfillTargets) {
        resvBackfill += (sizeof(resvBackfill), o);
      }
      o = o + 1;
    }
    assert !backfillReservedFlag, format("osd.{0} enters Active with backfill reserved", me);
    assert IsPrimary(), format("osd.{0} enters Active but is not primary", me);
    allReplicasActivated = false;
    Activate(pgEpoch);
  }
  fun Ex_Active() {
    CancelLocalReservation();
    backfillReservedFlag = false;
    backfillReserving = false;                // on_active_exit
    Clear(B_ACTIVATING);
    Clear(B_DEGRADED);
    Clear(B_UNDERSIZED);
    Clear(B_BACKFILL_TOOFULL);
    Clear(B_BACKFILL_WAIT);
    Clear(B_RECOVERY_WAIT);
    Clear(B_RECOVERY_TOOFULL);
  }
  fun R_Active_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_Active_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_AdvMap(ev: tEvt): tResult {
    var o: int;
    var need: bool;
    var lm: tMap;
    var m: tMap;
    lm = maps[ev.lastEpoch];
    m = maps[ev.newEpoch];
    if (ShouldRestart(lm, m)) {
      return RES_FORWARD;
    }
    foreach (o in wantActing) {
      if (!m.up[o] && !Contains(acting, o) && !Contains(up, o)) {
        need = true;
      }
    }
    if (need) {
      RemoveDownPeerInfo(m);
      ChooseActing(false, true);
    }
    if (lm.size != m.size) {
      if (m.size <= sizeof(acting)) {
        Clear(B_UNDERSIZED);
      } else {
        Set(B_UNDERSIZED);
      }
    }
    return RES_FORWARD;
  }
  fun R_Active_ActMap(ev: tEvt): tResult {
    assert IsPrimary(), format("osd.{0} Active ActMap but not primary", me);
    // on_active_actmap
    if ((Test(B_ACTIVE) || Test(B_PEERED)) && !Test(B_CLEAN)) {
      QueueRecovery();
    }
    if (NumUnfound() > 0) {
      DiscoverAllMissing();
    }
    return RES_FORWARD;
  }
  fun R_Active_MNotifyRec(ev: tEvt): tResult {
    assert IsPrimary(), format("osd.{0} Active notify but not primary", me);
    if (ev.m.src in peerInfo || ev.m.src in peerPurged) {
      return RES_DISCARD;
    }
    ProcReplicaNotify(ev.m);
    if (NumUnfound() > 0 || (Test(B_DEGRADED) && ev.m.src in mightHaveUnfound)) {
      DiscoverAllMissing();
    }
    ChooseActing(false, true);
    return RES_DISCARD;
  }
  fun R_Active_MInfoRec(ev: tEvt): tResult {
    assert IsPrimary(), format("osd.{0} Active info but not primary", me);
    assert sizeof(arb) > 0, format("osd.{0} Active info with no acting_recovery_backfill", me);
    if (ev.m.src in arb && !(ev.m.src in peerActivated)) {
      peerActivated += (ev.m.src);
      if (sizeof(peerActivated) == sizeof(arb)) {
        AllActivatedAndCommitted();
      }
    }
    return RES_DISCARD;
  }
  fun R_Active_MLogRec(ev: tEvt): tResult {
    var got: bool;
    ProcReplicaLog(ev.m);
    got = SearchForMissing(ev.m.src, peerInfo[ev.m.src], peerMissing[ev.m.src]);
    if (got && Test(B_ACTIVE)) {
      Post(Evt(E_DoRecovery));
    }
    return RES_DISCARD;
  }
  fun R_Active_MTrim(ev: tEvt): tResult {
    assert IsPrimary(), format("osd.{0} Active trim but not primary", me);
    peerLcod[ev.m.src] = ev.m.lcod;
    CalcMinLcod();
    return RES_DISCARD;
  }
  fun R_Active_Backfilled(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_ActivateCommitted(ev: tEvt): tResult {
    assert !(me in peerActivated), format("osd.{0} ActivateCommitted twice", me);
    peerActivated += (me);
    assert sizeof(arb) > 0, format("osd.{0} ActivateCommitted with no acting_recovery_backfill", me);
    if (sizeof(peerActivated) == sizeof(arb)) {
      AllActivatedAndCommitted();
    }
    return RES_DISCARD;
  }
  fun R_Active_AllReplicasActivated(ev: tEvt): tResult {
    var w: seq[tW];
    var i: int;
    allReplicasActivated = true;
    Clear(B_ACTIVATING);
    Clear(B_CREATING);
    if (!Writeable()) {
      Set(B_PEERED);
    } else {
      Set(B_ACTIVE);
    }
    info.h.les = info.les;
    info.h.lis = info.lis;
    SharePgInfo();
    if (Test(B_ACTIVE)) {
      announce mActive, (osd = me, sis = info.h.sis);
      send env, eNoteActive, info.h.sis;
    }
    OnActivateComplete();
    // requeue waiting_for_peered / waiting_for_active
    w = queuedWrites;
    queuedWrites = default(seq[tW]);
    while (i < sizeof(w)) {
      send this, eRequeueWrite, (w = w[i], inc = incarnation);
      i = i + 1;
    }
    ReplayPeered();
    return RES_DISCARD;
  }
  fun R_Active_DeferRecovery(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_DeferBackfill(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_UnfoundRecovery(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_UnfoundBackfill(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_RemoteReservationRevokedTooFull(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_RemoteReservationRevoked(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_DoRecovery(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_RenewLease(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_MLeaseAck(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_CheckReadable(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_Active_PgCreateEvt(ev: tEvt): tResult { return RES_DISCARD; }

  // PrimaryLogPG::on_activate_complete
  fun OnActivateComplete() {
    var o: int;
    if (NeedsRecovery()) {
      Queue(Evt(E_DoRecovery));
    } else if (NeedsBackfill()) {
      Queue(Evt(E_RequestBackfill));
    } else {
      Queue(Evt(E_AllReplicasRecovered));
    }
    if (sizeof(backfillTargets) > 0) {
      lastBackfillStarted = EarliestBackfill();
      newBackfill = true;
      assert lastBackfillStarted != LB_MAX(), format("osd.{0} backfill tgts but nothing to backfill", me);
    }
  }

  fun EarliestBackfill(): int {
    var o: int;
    var e: int;
    e = LB_MAX();
    foreach (o in backfillTargets) {
      if (peerInfo[o].lb < e) {
        e = peerInfo[o].lb;
      }
    }
    return e;
  }

  // PeeringState::calc_min_last_complete_ondisk
  fun CalcMinLcod() {
    var o: int;
    var mn: int;
    mn = lcod;
    foreach (o in arb) {
      if (o != me) {
        if (!(o in peerLcod)) {
          return;
        }
        if (peerLcod[o] < mn) {
          mn = peerLcod[o];
        }
      }
    }
    minLcod = mn;
  }

  // Recovered, Clean
  fun En_Recovered() {
    assert !NeedsRecovery(), format("osd.{0} Recovered while needing recovery", me);
    if (Map().size <= sizeof(arb)) {
      Clear(B_FORCED_BACKFILL);
      Clear(B_FORCED_RECOVERY);
    }
    if (acting != up) {
      if (!ChooseActing(true, false)) {
        assert sizeof(wantActing) > 0, format("osd.{0} Recovered: choose_acting failed without want_acting", me);
      }
    } else if (sizeof(asyncTargets) > 0) {
      ChooseActing(true, false);
    }
    if (allReplicasActivated && sizeof(asyncTargets) == 0) {
      Post(Evt(E_GoClean));
    }
  }
  fun Ex_Recovered() {}
  fun R_Recovered_AllReplicasActivated(ev: tEvt): tResult {
    Post(Evt(E_GoClean));
    return RES_FORWARD;
  }

  fun En_Clean() {
    assert info.lc == info.lu, format("osd.{0} Clean with last_complete {1} != last_update {2}", me, info.lc, info.lu);
    TryMarkClean();
    send this, eFinishRecovery, (e = pgEpoch, inc = incarnation);
  }
  fun Ex_Clean() {
    Clear(B_CLEAN);
  }

  // PeeringState::try_mark_clean
  fun TryMarkClean() {
    if (sizeof(acting) == Map().size) {
      Clear(B_FORCED_BACKFILL);
      Clear(B_FORCED_RECOVERY);
      Set(B_CLEAN);
      info.h.lec = pgEpoch;
      pi = EmptyPI();
      announce mClean, (osd = me, sis = info.h.sis, acting = acting);
      send env, eNoteClean, info.h.sis;
    }
    Clear(B_FORCED_RECOVERY);
    Clear(B_FORCED_BACKFILL);
    SharePgInfo();
    // clear_recovery_state
    asyncTargets = default(set[int]);
    backfillTargets = default(set[int]);
  }

  // PeeringState::purge_strays
  fun PurgeStrays() {
    var o: int;
    var m: tMsg;
    foreach (o in strays) {
      if (Map().up[o]) {
        m = Msg(K_REMOVE, me, o, pgEpoch, pgEpoch);
        Send(m);
      }
      if (o in peerMissing) {
        peerMissing -= (o);
      }
      if (o in peerInfo) {
        peerInfo -= (o);
      }
      peerPurged += (o);
    }
    strays = default(set[int]);
    peerMissingRequested = default(set[int]);
  }

  // ---------------------------------------------------------- non-primary

  fun En_Stray() {
    assert !Test(B_ACTIVE) && !Test(B_PEERED), format("osd.{0} Stray while peered", me);
    assert !Test(B_PEERING), format("osd.{0} Stray while peering", me);
    assert !IsPrimary(), format("osd.{0} Stray but primary", me);
  }
  fun Ex_Stray() {}
  fun R_Stray_MQuery(ev: tEvt): tResult {
    FulfillQuery(ev.m);
    return RES_DISCARD;
  }
  fun R_Stray_MLogRec(ev: tEvt): tResult {
    var st: tStore;
    if (ev.m.info.lb == 0) {
      // restart backfill: take the primary's info and log
      info = ev.m.info;
      log = ev.m.log;
      missing = default(map[int, tMiss]);
      info.lc = info.lu;
    } else {
      st = MergeLog(Store(), info.lb, ev.m.log, ev.m.tail, ev.m.info.lu);
      SetStore(st);
    }
    assert Head(log, info.tail) == info.lu, format("osd.{0} log head {1} != last_update {2}", me, Head(log, info.tail), info.lu);
    Post(ActivateEvt(ev.m.info.les));
    Transit(S_Stray, S_ReplicaActive, ev.e);
    return RES_TRANSIT;
  }
  fun R_Stray_MInfoRec(ev: tEvt): tResult {
    var st: tStore;
    if (info.lu > ev.m.info.lu) {
      st = RewindDivergent(Store(), info.lb, ev.m.info.lu);
      SetStore(st);
    }
    assert ev.m.info.lu == info.lu,
      format("osd.{0} Stray got info at {1} while at {2}", me, ev.m.info.lu, info.lu);
    Post(ActivateEvt(ev.m.info.les));
    Transit(S_Stray, S_ReplicaActive, ev.e);
    return RES_TRANSIT;
  }
  fun R_Stray_ActMap(ev: tEvt): tResult {
    if (sendNotify && Primary() >= 0) {
      SendNotify();
    }
    return RES_DISCARD;
  }
  fun R_Stray_RecoveryDone(ev: tEvt): tResult { return RES_DISCARD; }

  fun En_ReplicaActive() {}
  fun Ex_ReplicaActive() {
    spaceReserved = false;                  // unreserve_recovery_space
    CancelRemoteReservation();
    minLcod = 0;
  }
  fun R_ReplicaActive_QueryState(ev: tEvt): tResult { return RES_FORWARD; }
  fun R_ReplicaActive_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_ActMap(ev: tEvt): tResult {
    if (sendNotify && Primary() >= 0) {
      SendNotify();
    }
    return RES_DISCARD;
  }
  fun R_ReplicaActive_MQuery(ev: tEvt): tResult {
    FulfillQuery(ev.m);
    return RES_DISCARD;
  }
  fun R_ReplicaActive_MInfoRec(ev: tEvt): tResult {
    UpdateHistory(ev.m.info.h);           // proc_primary_info
    return RES_DISCARD;
  }
  fun R_ReplicaActive_MLogRec(ev: tEvt): tResult {
    var st: tStore;
    st = MergeLog(Store(), info.lb, ev.m.log, ev.m.tail, ev.m.info.lu);
    SetStore(st);
    assert Head(log, info.tail) == info.lu, format("osd.{0} log head {1} != last_update {2}", me, Head(log, info.tail), info.lu);
    return RES_DISCARD;
  }
  fun R_ReplicaActive_MTrim(ev: tEvt): tResult {
    SetStore(TrimLog(Store(), ev.m.lcod));
    return RES_DISCARD;
  }
  fun R_ReplicaActive_Activate(ev: tEvt): tResult {
    Activate(ev.a);
    return RES_DISCARD;
  }
  fun R_ReplicaActive_ActivateCommitted(ev: tEvt): tResult {
    var i: tInfo;
    var m: tMsg;
    i = info;
    i.h.les = ev.a;
    i.h.lis = i.h.sis;
    m = Msg(K_INFO, me, Primary(), pgEpoch, pgEpoch);
    m.info = i;
    Send(m);
    if (Writeable()) {
      Set(B_ACTIVE);
    } else {
      Set(B_PEERED);
    }
    ReplayPeered();                           // PG::on_activate_committed
    return RES_DISCARD;
  }
  fun R_ReplicaActive_DeferRecovery(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_DeferBackfill(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_UnfoundRecovery(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_UnfoundBackfill(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_RemoteBackfillPreempted(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_RemoteRecoveryPreempted(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_RecoveryDone(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_BackfillTooFull(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ReplicaActive_MLease(ev: tEvt): tResult { return RES_DISCARD; }

  // ================================================================ reservations

  // PeeringState::get_recovery_priority / get_backfill_priority / get_delete_priority
  fun RecoveryPriority(): int {
    if (Test(B_FORCED_RECOVERY)) {
      return 255;
    }
    if (sizeof(acting) < Map().minSize) {
      return 220 + Map().minSize - sizeof(acting);
    }
    return 180;
  }
  fun BackfillPriority(): int {
    if (Test(B_FORCED_BACKFILL)) {
      return 254;
    }
    if (sizeof(acting) < Map().minSize) {
      return 220 + Map().minSize - sizeof(acting);
    }
    if (Test(B_UNDERSIZED)) {
      return 140 + Map().size - sizeof(acting);
    }
    return 100;
  }
  fun DeletePriority(): int {
    if (me in Map().fullOsds) {
      return 255;
    }
    return 179;
  }

  fun Resv(op: tRq, isLocal: bool, prio: int, grant: tE, preempt: tE, hasPreempt: bool) {
    if (op == RQ_REQUEST) {
      resvSeq = resvSeq + 1;
      if (isLocal) {
        localSeq = resvSeq;
      } else {
        remoteSeq = resvSeq;
      }
    }
    send osd, eResv, (op = op, item = PG_ITEM(), local = isLocal, prio = prio, grant = Evt(grant),
                      preempt = Evt(preempt), hasPreempt = hasPreempt, epoch = pgEpoch, rseq = resvSeq,
                      inc = incarnation);
  }

  // (proposed) a reserver callback for a request since cancelled or replaced
  fun StaleCallback(ev: tEvt): bool {
    if (!cfg.grantFromCheck || !ev.fromResv) {
      return false;
    }
    if (ev.e == E_LocalRecoveryReserved || ev.e == E_LocalBackfillReserved || ev.e == E_DeleteReserved ||
        ev.e == E_DeferRecovery || ev.e == E_DeferBackfill || ev.e == E_DeleteInterrupted) {
      return ev.rseq != localSeq;
    }
    return ev.rseq != remoteSeq;
  }
  fun RequestLocal(prio: int, grant: tE, preempt: tE) {
    localSlot = false;
    Resv(RQ_REQUEST, true, prio, grant, preempt, true);
  }
  fun CancelLocalReservation() {
    localSlot = false;
    localSeq = -1;
    Resv(RQ_CANCEL, true, 0, E_NullEvt, E_NullEvt, false);
  }
  fun UpdateLocalPriority(prio: int) {
    Resv(RQ_PRIO, true, prio, E_NullEvt, E_NullEvt, false);
  }
  fun RequestRemote(prio: int, grant: tE, preempt: tE) {
    Resv(RQ_REQUEST, false, prio, grant, preempt, true);
  }
  fun CancelRemoteReservation() {
    remoteSeq = -1;
    Resv(RQ_CANCEL, false, 0, E_NullEvt, E_NullEvt, false);
  }

  // an AsyncReserver callback got past the epoch filter: track whether the
  // local slot is held
  fun NoteResvCallback(ev: tEvt) {
    if (!ev.fromResv) {
      return;
    }
    if (ev.e == E_LocalRecoveryReserved || ev.e == E_LocalBackfillReserved || ev.e == E_DeleteReserved) {
      localSlot = true;
    } else if (ev.e == E_DeferRecovery || ev.e == E_DeferBackfill || ev.e == E_DeleteInterrupted) {
      localSlot = false;
    }
  }

  fun CheckSlot(what: string) {
    assert !cfg.slotCheck || localSlot,
      format("osd.{0} e{1} enters {2} without its local reservation: the reserver preempted it and the PG discarded the preemption",
             me, pgEpoch, what);
  }

  // MRecoveryReserve / MBackfillReserve
  fun SendResv(dst: int, backfill: bool, res: tRes, prio: int) {
    var m: tMsg;
    m = Msg(K_RESERVE, me, dst, pgEpoch, pgEpoch);
    m.backfill = backfill;
    m.res = res;
    m.prio = prio;
    Send(m);
  }

  // MRecoveryReserve::get_event, MBackfillReserve::get_event
  fun ReserveEvent(m: tMsg): tEvt {
    var ev: tEvt;
    if (!m.backfill) {
      if (m.res == RES_REQUEST) {
        ev = EvtMsg(E_RequestRecoveryPrio, m);
        ev.a = m.prio;
        return ev;
      }
      if (m.res == RES_GRANT) { return EvtMsg(E_RemoteRecoveryReserved, m); }
      if (m.res == RES_RELEASE) { return EvtMsg(E_RecoveryDone, m); }
      if (m.res == RES_REVOKE) { return EvtMsg(E_DeferRecovery, m); }
    } else {
      if (m.res == RES_REQUEST) {
        ev = EvtMsg(E_RequestBackfillPrio, m);
        ev.a = m.prio;
        return ev;
      }
      if (m.res == RES_GRANT) { return EvtMsg(E_RemoteBackfillReserved, m); }
      if (m.res == RES_REJECT_TOOFULL) { return EvtMsg(E_RemoteReservationRejectedTooFull, m); }
      if (m.res == RES_RELEASE) { return EvtMsg(E_RemoteReservationCanceled, m); }
      if (m.res == RES_REVOKE_TOOFULL) { return EvtMsg(E_RemoteReservationRevokedTooFull, m); }
      if (m.res == RES_REVOKE) { return EvtMsg(E_RemoteReservationRevoked, m); }
    }
    assert false, format("osd.{0}: reservation message {1}", me, m.res);
    return ev;
  }

  // OSDMap::check_full
  fun CheckFull(s: set[int]): bool {
    var o: int;
    foreach (o in s) {
      if (o in Map().fullOsds) {
        return true;
      }
    }
    return false;
  }

  // PeeringState::set_force_recovery / set_force_backfill
  fun SetForce(recovery: bool, turnOn: bool) {
    var did: bool;
    if (recovery) {
      if (turnOn) {
        if (!Test(B_FORCED_RECOVERY) && (Test(B_DEGRADED) || Test(B_RECOVERY_WAIT) || Test(B_RECOVERING))) {
          Set(B_FORCED_RECOVERY);
          did = true;
        }
      } else if (Test(B_FORCED_RECOVERY)) {
        Clear(B_FORCED_RECOVERY);
        did = true;
      }
      if (did) {
        UpdateLocalPriority(RecoveryPriority());
      }
    } else {
      if (turnOn) {
        if (!Test(B_FORCED_BACKFILL) && (Test(B_DEGRADED) || Test(B_BACKFILL_WAIT) || Test(B_BACKFILLING))) {
          Set(B_FORCED_BACKFILL);
          did = true;
        }
      } else if (Test(B_FORCED_BACKFILL)) {
        Clear(B_FORCED_BACKFILL);
        did = true;
      }
      if (did) {
        UpdateLocalPriority(BackfillPriority());
      }
    }
  }

  // ---------------------------------------------------- recovery reservations

  fun En_Activating() {}
  fun Ex_Activating() {}

  fun En_WaitLocalRecoveryReserved() {
    if (CheckFull(arb)) {
      Post(Evt(E_RecoveryTooFull));
      return;
    }
    Clear(B_RECOVERY_TOOFULL);
    Set(B_RECOVERY_WAIT);
    RequestLocal(RecoveryPriority(), E_LocalRecoveryReserved, E_DeferRecovery);
  }
  fun Ex_WaitLocalRecoveryReserved() {}
  fun R_WaitLocalRecoveryReserved_RecoveryTooFull(ev: tEvt): tResult {
    Set(B_RECOVERY_TOOFULL);
    Queue(Evt(E_DoRecovery));                 // after osd_recovery_retry_interval
    Transit(S_WaitLocalRecoveryReserved, S_NotRecovering, ev.e);
    return RES_TRANSIT;
  }

  // the AdvMap reactions b8d2c6832fb wrote for both states but left out of
  // their reaction lists (cfg.advMapFullCheck registers them)
  fun R70670(ev: tEvt): tResult {
    if (CheckFull(arb)) {
      Post(Evt(E_RecoveryTooFull));
      return RES_DISCARD;
    }
    return RES_FORWARD;
  }

  fun En_WaitRemoteRecoveryReserved() {
    resvAt = 0;
    Post(Evt(E_RemoteRecoveryReserved));
  }
  fun Ex_WaitRemoteRecoveryReserved() {}
  fun R_WaitRemoteRecoveryReserved_RemoteRecoveryReserved(ev: tEvt): tResult {
    if (resvAt < sizeof(resvRecovery)) {
      assert resvRecovery[resvAt] != me, format("osd.{0} reserves recovery on itself", me);
      SendResv(resvRecovery[resvAt], false, RES_REQUEST, RecoveryPriority());
      resvAt = resvAt + 1;
    } else {
      Post(Evt(E_AllRemotesReserved));
    }
    return RES_DISCARD;
  }

  fun En_Recovering() {
    Clear(B_RECOVERY_WAIT);
    Clear(B_RECOVERY_TOOFULL);
    Set(B_RECOVERING);
    CheckSlot("Recovering");
    QueueRecovery();                          // on_recovery_reserved
    assert !Test(B_ACTIVATING), format("osd.{0} Recovering while activating", me);
  }
  fun Ex_Recovering() {
    Clear(B_RECOVERING);
  }
  // Recovering::release_reservations
  fun ReleaseRecoveryReservations(cancel: bool) {
    var i: int;
    assert cancel || sizeof(missing) == 0, format("osd.{0} releases recovery reservations while missing objects", me);
    while (i < sizeof(resvRecovery)) {
      if (resvRecovery[i] != me) {
        SendResv(resvRecovery[i], false, RES_RELEASE, 0);
      }
      i = i + 1;
    }
  }
  fun R_Recovering_AllReplicasRecovered(ev: tEvt): tResult {
    Clear(B_FORCED_RECOVERY);
    ReleaseRecoveryReservations(false);
    CancelLocalReservation();
    Transit(S_Recovering, S_Recovered, ev.e);
    return RES_TRANSIT;
  }
  fun R_Recovering_RequestBackfill(ev: tEvt): tResult {
    ReleaseRecoveryReservations(false);
    Clear(B_FORCED_RECOVERY);
    CancelLocalReservation();
    if (sizeof(asyncTargets) > 0) {
      ChooseActing(true, false);
    }
    Transit(S_Recovering, S_WaitLocalBackfillReserved, ev.e);
    return RES_TRANSIT;
  }
  fun R_Recovering_DeferRecovery(ev: tEvt): tResult {
    if (!Test(B_RECOVERING)) {
      return RES_DISCARD;
    }
    Set(B_RECOVERY_WAIT);
    CancelLocalReservation();
    ReleaseRecoveryReservations(true);
    Queue(Evt(E_DoRecovery));                 // after the delay
    Transit(S_Recovering, S_NotRecovering, ev.e);
    return RES_TRANSIT;
  }
  fun R_Recovering_UnfoundRecovery(ev: tEvt): tResult {
    Set(B_RECOVERY_UNFOUND);
    CancelLocalReservation();
    ReleaseRecoveryReservations(true);
    Transit(S_Recovering, S_NotRecovering, ev.e);
    return RES_TRANSIT;
  }

  fun En_NotRecovering() {}
  fun Ex_NotRecovering() {
    Clear(B_RECOVERY_UNFOUND);
  }
  fun R_NotRecovering_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_NotRecovering_DeferRecovery(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_NotRecovering_UnfoundRecovery(ev: tEvt): tResult { return RES_DISCARD; }

  // ---------------------------------------------------- backfill reservations

  fun En_WaitLocalBackfillReserved() {
    Set(B_BACKFILL_WAIT);
    RequestLocal(BackfillPriority(), E_LocalBackfillReserved, E_DeferBackfill);
  }
  fun Ex_WaitLocalBackfillReserved() {}
  fun R_WaitLocalBackfillReserved_RemoteBackfillReserved(ev: tEvt): tResult { return RES_DISCARD; }

  fun En_WaitRemoteBackfillReserved() {
    Set(B_BACKFILL_WAIT);
    resvAt = 0;
    Post(Evt(E_RemoteBackfillReserved));
  }
  fun Ex_WaitRemoteBackfillReserved() {}
  fun R_WaitRemoteBackfillReserved_RemoteBackfillReserved(ev: tEvt): tResult {
    if (resvAt < sizeof(resvBackfill)) {
      assert resvBackfill[resvAt] != me, format("osd.{0} reserves backfill on itself", me);
      SendResv(resvBackfill[resvAt], true, RES_REQUEST, BackfillPriority());
      resvAt = resvAt + 1;
    } else {
      Post(Evt(E_AllBackfillsReserved));
    }
    return RES_DISCARD;
  }
  // WaitRemoteBackfillReserved::retry
  fun RetryBackfill() {
    var i: int;
    CancelLocalReservation();
    assert sizeof(resvBackfill) > 0, format("osd.{0} retries backfill without backfill tgts", me);
    while (i < resvAt) {
      SendResv(resvBackfill[i], true, RES_RELEASE, 0);
      i = i + 1;
    }
    Clear(B_BACKFILL_WAIT);
    Queue(Evt(E_RequestBackfill));            // after osd_backfill_retry_interval
  }
  fun R_WaitRemoteBackfillReserved_RemoteReservationRejectedTooFull(ev: tEvt): tResult {
    Set(B_BACKFILL_TOOFULL);
    RetryBackfill();
    Transit(S_WaitRemoteBackfillReserved, S_NotBackfilling, ev.e);
    return RES_TRANSIT;
  }
  fun R_WaitRemoteBackfillReserved_RemoteReservationRevoked(ev: tEvt): tResult {
    RetryBackfill();
    Transit(S_WaitRemoteBackfillReserved, S_NotBackfilling, ev.e);
    return RES_TRANSIT;
  }

  fun En_Backfilling() {
    backfillReservedFlag = true;
    Clear(B_BACKFILL_TOOFULL);
    Clear(B_BACKFILL_WAIT);
    Set(B_BACKFILLING);
    CheckSlot("Backfilling");
    backfillReserving = false;                // on_backfill_reserved
    QueueRecovery();
  }
  fun Ex_Backfilling() {
    backfillReservedFlag = false;
    Clear(B_BACKFILLING);
    Clear(B_FORCED_BACKFILL);
    Clear(B_FORCED_RECOVERY);
  }
  fun BackfillReleaseReservations() {
    var o: int;
    CancelLocalReservation();
    foreach (o in backfillTargets) {
      assert o != me, format("osd.{0} is its own backfill target", me);
      SendResv(o, true, RES_RELEASE, 0);
    }
  }
  fun SuspendBackfill() {
    BackfillReleaseReservations();
    // PG::on_backfill_suspended
    if (sizeof(waitingOnBackfill) > 0) {
      waitingOnBackfill = default(set[int]);
      FinishRecoveryOp();
    }
  }
  fun R_Backfilling_Backfilled(ev: tEvt): tResult {
    BackfillReleaseReservations();
    Transit(S_Backfilling, S_Recovered, ev.e);
    return RES_TRANSIT;
  }
  fun R_Backfilling_DeferBackfill(ev: tEvt): tResult {
    if (NeedsBackfill()) {
      Set(B_BACKFILL_WAIT);
      Clear(B_BACKFILLING);
      SuspendBackfill();
      Queue(Evt(E_RequestBackfill));          // after the delay
      Transit(S_Backfilling, S_NotBackfilling, ev.e);
      return RES_TRANSIT;
    }
    return RES_DISCARD;
  }
  fun R_Backfilling_UnfoundBackfill(ev: tEvt): tResult {
    Set(B_BACKFILL_UNFOUND);
    Clear(B_BACKFILLING);
    SuspendBackfill();
    Transit(S_Backfilling, S_NotBackfilling, ev.e);
    return RES_TRANSIT;
  }
  fun R_Backfilling_RemoteReservationRevokedTooFull(ev: tEvt): tResult {
    Set(B_BACKFILL_TOOFULL);
    Clear(B_BACKFILLING);
    SuspendBackfill();
    Queue(Evt(E_RequestBackfill));            // after osd_backfill_retry_interval
    Transit(S_Backfilling, S_NotBackfilling, ev.e);
    return RES_TRANSIT;
  }
  fun R_Backfilling_RemoteReservationRevoked(ev: tEvt): tResult {
    if (NeedsBackfill()) {
      Set(B_BACKFILL_WAIT);
      SuspendBackfill();
      Transit(S_Backfilling, S_WaitLocalBackfillReserved, ev.e);
      return RES_TRANSIT;
    }
    return RES_DISCARD;
  }
  fun R_Backfilling_RemoteReservationRejectedTooFull(ev: tEvt): tResult {
    Post(Evt(E_RemoteReservationRevokedTooFull));
    return RES_DISCARD;
  }

  fun En_NotBackfilling() {}
  fun Ex_NotBackfilling() {
    Clear(B_BACKFILL_UNFOUND);
  }
  fun R_NotBackfilling_QueryUnfound(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_NotBackfilling_RemoteBackfillReserved(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_NotBackfilling_RemoteReservationRejectedTooFull(ev: tEvt): tResult { return RES_DISCARD; }

  // ------------------------------------------------- replica reservations

  // PG::try_reserve_recovery_space: tentative_backfill_full
  fun TryReserveRecoverySpace(): bool {
    if (pgFull) {
      return false;
    }
    spaceReserved = true;
    return true;
  }
  // PeeringState::reject_reservation
  fun RejectReservation() {
    spaceReserved = false;
    SendResv(Primary(), true, RES_REJECT_TOOFULL, 0);
  }

  fun En_RepNotRecovering() {}
  fun Ex_RepNotRecovering() {}
  fun R_RepNotRecovering_RequestBackfillPrio(ev: tEvt): tResult {
    if (!TryReserveRecoverySpace()) {
      Post(Evt(E_RejectTooFullRemoteReservation));
    } else {
      RequestRemote(ev.a, E_RemoteBackfillReserved, E_RemoteBackfillPreempted);
    }
    Transit(S_RepNotRecovering, S_RepWaitBackfillReserved, ev.e);
    return RES_TRANSIT;
  }
  fun R_RepNotRecovering_RequestRecoveryPrio(ev: tEvt): tResult {
    RequestRemote(ev.a, E_RemoteRecoveryReserved, E_RemoteRecoveryPreempted);
    Transit(S_RepNotRecovering, S_RepWaitRecoveryReserved, ev.e);
    return RES_TRANSIT;
  }
  fun R_RepNotRecovering_RejectTooFullRemoteReservation(ev: tEvt): tResult {
    RejectReservation();
    Post(Evt(E_RemoteReservationRejectedTooFull));
    return RES_DISCARD;
  }
  fun R_RepNotRecovering_RemoteBackfillReserved(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_RepNotRecovering_RemoteRecoveryReserved(ev: tEvt): tResult { return RES_DISCARD; }

  fun En_RepWaitRecoveryReserved() {}
  fun Ex_RepWaitRecoveryReserved() {}
  fun R_RepWaitRecoveryReserved_RemoteRecoveryReserved(ev: tEvt): tResult {
    SendResv(Primary(), false, RES_GRANT, 0);
    Transit(S_RepWaitRecoveryReserved, S_RepRecovering, ev.e);
    return RES_TRANSIT;
  }
  fun R_RepWaitRecoveryReserved_RemoteReservationRejectedTooFull(ev: tEvt): tResult {
    Post(Evt(E_RemoteReservationCanceled));
    return RES_DISCARD;
  }
  fun R_RepWaitRecoveryReserved_RemoteReservationCanceled(ev: tEvt): tResult {
    spaceReserved = false;
    CancelRemoteReservation();
    Transit(S_RepWaitRecoveryReserved, S_RepNotRecovering, ev.e);
    return RES_TRANSIT;
  }

  fun En_RepWaitBackfillReserved() {}
  fun Ex_RepWaitBackfillReserved() {}
  fun R_RepWaitBackfillReserved_BackfillTooFull(ev: tEvt): tResult {
    RejectReservation();
    Post(Evt(E_RemoteReservationRejectedTooFull));
    return RES_DISCARD;
  }
  fun R_RepWaitBackfillReserved_RemoteBackfillReserved(ev: tEvt): tResult {
    SendResv(Primary(), true, RES_GRANT, 0);
    Transit(S_RepWaitBackfillReserved, S_RepRecovering, ev.e);
    return RES_TRANSIT;
  }
  fun R_RepWaitBackfillReserved_RejectTooFullRemoteReservation(ev: tEvt): tResult {
    RejectReservation();
    Post(Evt(E_RemoteReservationRejectedTooFull));
    return RES_DISCARD;
  }
  fun R_RepWaitBackfillReserved_RemoteReservationRejectedTooFull(ev: tEvt): tResult {
    spaceReserved = false;
    CancelRemoteReservation();
    Transit(S_RepWaitBackfillReserved, S_RepNotRecovering, ev.e);
    return RES_TRANSIT;
  }
  fun R_RepWaitBackfillReserved_RemoteReservationCanceled(ev: tEvt): tResult {
    spaceReserved = false;
    CancelRemoteReservation();
    Transit(S_RepWaitBackfillReserved, S_RepNotRecovering, ev.e);
    return RES_TRANSIT;
  }

  fun En_RepRecovering() {}
  fun Ex_RepRecovering() {
    spaceReserved = false;
    CancelRemoteReservation();
  }
  fun R_RepRecovering_RemoteRecoveryPreempted(ev: tEvt): tResult {
    spaceReserved = false;
    SendResv(Primary(), false, RES_REVOKE, 0);
    return RES_DISCARD;
  }
  fun R_RepRecovering_BackfillTooFull(ev: tEvt): tResult {
    spaceReserved = false;
    SendResv(Primary(), true, RES_REVOKE_TOOFULL, 0);
    return RES_DISCARD;
  }
  fun R_RepRecovering_RemoteBackfillPreempted(ev: tEvt): tResult {
    spaceReserved = false;
    SendResv(Primary(), true, RES_REVOKE, 0);
    return RES_DISCARD;
  }

  // ----------------------------------------------------------- deletion

  fun En_ToDelete() {}
  fun Ex_ToDelete() {
    CancelLocalReservation();
  }
  fun R_ToDelete_ActMap(ev: tEvt): tResult {
    if (DeletePriority() != deletePriority) {
      Transit(S_ToDelete, S_ToDelete, ev.e);
      return RES_TRANSIT;
    }
    return RES_DISCARD;
  }
  fun R_ToDelete_ActivateCommitted(ev: tEvt): tResult { return RES_DISCARD; }
  fun R_ToDelete_DeleteSome(ev: tEvt): tResult { return RES_DISCARD; }

  fun En_WaitDeleteReserved() {
    deletePriority = DeletePriority();
    CancelLocalReservation();
    RequestLocal(deletePriority, E_DeleteReserved, E_DeleteInterrupted);
  }
  fun Ex_WaitDeleteReserved() {}

  fun En_Deleting() {
    deleting = true;
    CheckSlot("Deleting");
    // roll forward, then treat what is left as an unfinished backfill
    info.lb = 0;
    missing = default(map[int, tMiss]);
    info.lc = info.lu;
    // PG::on_removal -> OSDService::queue_for_pg_delete
    Queue(Evt(E_DeleteSome));
  }
  fun Ex_Deleting() {
    deleting = false;
    CancelLocalReservation();
  }
  // PG::do_delete_work: an object per transaction; then the collection
  fun R_Deleting_DeleteSome(ev: tEvt): tResult {
    var oid: int;
    foreach (oid in keys(objs)) {
      RemoveObj(oid);
      Queue(Evt(E_DeleteSome));               // C_DeleteMore
      return RES_DISCARD;
    }
    CancelLocalReservation();
    announce mDeleted, me;
    Terminate();
    return RES_TERMINATE;
  }

  // ================================================================ PrimaryLogPG

  fun IsPeered(): bool {
    return Test(B_ACTIVE) || Test(B_PEERED);
  }

  // PrimaryLogPG::on_change, PG::cancel_recovery
  fun OnChange() {
    var i: int;
    var w: seq[tW];
    var q: seq[tMsg];
    recoveryQueued = false;
    w = queuedWrites;
    i = 0;
    while (i < sizeof(blockedWrites)) {
      w += (sizeof(w), blockedWrites[i]);
      i = i + 1;
    }
    queuedWrites = default(seq[tW]);
    blockedWrites = default(seq[tW]);
    inFlight = default(map[int, (wid: int, oid: int, waiting: set[int])]);
    RecoveryClear();
    q = waitingPeered;
    waitingPeered = default(seq[tMsg]);
    // requeue: examined again after this event
    i = 0;
    while (i < sizeof(w)) {
      send this, eRequeueWrite, (w = w[i], inc = incarnation);
      i = i + 1;
    }
    i = 0;
    while (i < sizeof(q)) {
      send this, eRequeueMsg, (m = q[i], inc = incarnation);
      i = i + 1;
    }
  }

  // PG::clear_recovery_state, PrimaryLogPG::_clear_recovery_state
  fun RecoveryClear() {
    recoveryOps = 0;
    recovering = default(map[int, int]);
    pulling = default(map[int, int]);
    pushing = default(map[int, set[int]]);
    pushVer = default(map[int, int]);
    waitingOnBackfill = default(set[int]);
    pbi = default(map[int, tPbi]);
    lastBackfillStarted = 0;
    backfillsInFlight = default(set[int]);
    pendingBackfill = default(set[int]);
    recoveryReadMarker = default(set[int]);
  }

  fun PrimaryExitGlue() {}

  // PG::queue_recovery
  fun QueueRecovery() {
    if (!IsPrimary() || !IsPeered()) {
      assert !recoveryQueued, format("osd.{0} queue_recovery: queued while not primary or not peered", me);
      return;
    }
    if (recoveryQueued) {
      return;
    }
    recoveryQueued = true;
    send this, eRecoveryTick, (e = pgEpoch, inc = incarnation);
  }

  fun StartRecoveryOp() {
    recoveryOps = recoveryOps + 1;
  }

  // PG::finish_recovery_op
  fun FinishRecoveryOp() {
    assert recoveryOps > 0, format("osd.{0} finish_recovery_op with no recovery op active", me);
    recoveryOps = recoveryOps - 1;
    QueueRecovery();
  }

  // OSD::do_recovery
  fun RecoveryTick(e: int) {
    if (deleting || e < lpr) {
      return;                                 // pg_has_reset_since
    }
    if (StartRecoveryOps(cfg.maxRecoveryOps)) {
      FindUnfound(e);
    }
  }

  // PG::find_unfound
  fun FindUnfound(e: int) {
    if (!DiscoverAllMissing()) {
      if (Test(B_BACKFILLING)) {
        send this, eQueued, (evt = Evt(E_UnfoundBackfill), es = e, er = e, inc = incarnation);
      } else if (Test(B_RECOVERING)) {
        send this, eQueued, (evt = Evt(E_UnfoundRecovery), es = e, er = e, inc = incarnation);
      }
    } else {
      QueueRecovery();
    }
  }

  fun AllMissingUnfound(): bool {
    var oid: int;
    foreach (oid in keys(missing)) {
      if (!IsUnfound(oid)) {
        return false;
      }
    }
    return true;
  }

  // PrimaryLogPG::start_recovery_ops: true if the caller should look for unfound objects
  fun StartRecoveryOps(max: int): bool {
    var started: int;
    var unfound: int;
    var deferred: bool;
    assert IsPrimary(), format("osd.{0} start_recovery_ops but not primary", me);
    assert IsPeered(), format("osd.{0} start_recovery_ops but not peered", me);
    assert !deleting, format("osd.{0} start_recovery_ops while deleting", me);
    assert recoveryQueued, format("osd.{0} start_recovery_ops not queued", me);
    recoveryQueued = false;
    if (!Test(B_RECOVERING) && !Test(B_BACKFILLING)) {
      return NumUnfound() > 0;
    }
    workStarted = false;
    unfound = NumUnfound();
    if (sizeof(missing) == 0) {
      info.lc = info.lu;                      // local_recovery_complete
    }
    if (sizeof(missing) == 0 || AllMissingUnfound()) {
      started = RecoverReplicas(max);
    }
    if (started == 0) {
      started = started + RecoverPrimary(max);
    }
    if (started == 0 && unfound != NumUnfound()) {
      started = RecoverReplicas(max);
    }
    if (started > 0) {
      workStarted = true;
    }
    if (sizeof(recovering) == 0 && Test(B_BACKFILLING) && sizeof(backfillTargets) > 0 && started < max &&
        sizeof(missing) == 0 && sizeof(waitingOnBackfill) == 0) {
      if (!backfillReservedFlag) {
        if (!backfillReserving) {
          backfillReserving = true;
          Queue(Evt(E_RequestBackfill));
        }
        deferred = true;
      } else {
        started = started + RecoverBackfill(max - started);
      }
    }
    if (sizeof(recovering) > 0 || workStarted || recoveryOps > 0 || deferred) {
      return !workStarted && NumUnfound() > 0;
    }
    assert sizeof(recovering) == 0 && recoveryOps == 0, format("osd.{0} recovery done with ops active", me);
    if (NumUnfound() > 0) {
      return true;
    }
    assert sizeof(missing) == 0, format("osd.{0}: Unexpected Error: recovery ending with missing {1}", me, missing);
    assert !NeedsRecovery(), format("osd.{0}: Unexpected Error: recovery ending with missing replicas {1}", me, peerMissing);
    if (Test(B_RECOVERING)) {
      Clear(B_RECOVERING);
      Clear(B_FORCED_RECOVERY);
      if (NeedsBackfill()) {
        Queue(Evt(E_RequestBackfill));
      } else {
        Clear(B_FORCED_BACKFILL);
        Queue(Evt(E_AllReplicasRecovered));
      }
    } else {
      Clear(B_BACKFILLING);
      Clear(B_FORCED_BACKFILL);
      Clear(B_FORCED_RECOVERY);
      Queue(Evt(E_Backfilled));
    }
    return false;
  }

  // objects of a missing set, oldest need first (get_rmissing)
  fun ByNeed(ms: map[int, tMiss]): seq[int] {
    var r: seq[int];
    var left: map[int, tMiss];
    var oid: int;
    var best: int;
    left = ms;
    while (sizeof(left) > 0) {
      best = -1;
      foreach (oid in keys(left)) {
        if (best == -1 || left[oid].need < left[best].need) {
          best = oid;
        }
      }
      r += (sizeof(r), best);
      left -= (best);
    }
    return r;
  }

  // PrimaryLogPG::recover_primary
  fun RecoverPrimary(max: int): int {
    var started: int;
    var order: seq[int];
    var i: int;
    order = ByNeed(missing);
    while (i < sizeof(order) && started < max) {
      if (!(order[i] in recovering) && !IsUnfound(order[i])) {
        started = started + RecoverMissing(order[i]);
      }
      i = i + 1;
    }
    return started;
  }

  // PrimaryLogPG::recover_missing: pull from a peer that has it
  fun RecoverMissing(oid: int): int {
    var src: int;
    var o: int;
    var m: tMsg;
    if (IsUnfound(oid)) {
      return 0;
    }
    src = -1;
    foreach (o in missingLoc[oid]) {
      if (o != me && Map().up[o] && (src == -1 || o < src)) {
        src = o;
      }
    }
    if (src == -1) {
      return 0;
    }
    assert !(oid in recovering), format("osd.{0} recover_missing: object {1} already recovering", me, oid);
    recovering[oid] = RC_PULL();
    pulling[oid] = src;
    StartRecoveryOp();
    m = Msg(K_PULL, me, src, pgEpoch, info.h.sis);
    m.oid = oid;
    Send(m);
    return 1;
  }

  // PrimaryLogPG::recover_replicas
  fun RecoverReplicas(max: int): int {
    var started: int;
    var peers: seq[int];
    var order: seq[int];
    var p: int;
    var i: int;
    var j: int;
    var oid: int;
    peers = PeersByMissing();
    while (i < sizeof(peers)) {
      p = peers[i];
      order = ByNeed(peerMissing[p]);
      j = 0;
      while (j < sizeof(order) && started < max) {
        oid = order[j];
        j = j + 1;
        if (oid in recovering) {
          workStarted = true;
          continue;
        }
        if (IsUnfound(oid)) {
          continue;
        }
        if (p in backfillTargets && oid > peerInfo[p].lb) {
          assert false, format("osd.{0} recover_replicas: object {1} added to missing set for backfill of osd.{2}, but is not in recovering, error!", me, oid, p);
        }
        if (oid in missing) {
          started = started + RecoverMissing(oid);
          continue;
        }
        started = started + PrepObjectReplicaPushes(oid);
      }
      i = i + 1;
    }
    return started;
  }

  // replicas_by_num_missing, then async_by_num_missing
  fun PeersByMissing(): seq[int] {
    var r: seq[int];
    var a: seq[int];
    var o: int;
    var left: set[int];
    var best: int;
    var pass: int;
    while (pass < 2) {
      left = default(set[int]);
      foreach (o in arb) {
        if (o != me && o in peerMissing && sizeof(peerMissing[o]) > 0 && ((o in asyncTargets) == (pass == 1))) {
          left += (o);
        }
      }
      while (sizeof(left) > 0) {
        best = -1;
        foreach (o in left) {
          if (best == -1 || sizeof(peerMissing[o]) < sizeof(peerMissing[best]) ||
              (sizeof(peerMissing[o]) == sizeof(peerMissing[best]) && o < best)) {
            best = o;
          }
        }
        r += (sizeof(r), best);
        left -= (best);
      }
      pass = pass + 1;
    }
    return r;
  }

  // PrimaryLogPG::prep_object_replica_pushes
  fun PrepObjectReplicaPushes(oid: int): int {
    var o: int;
    var tgts: set[int];
    assert !(oid in recovering), format("osd.{0} prep_object_replica_pushes: object {1} already recovering", me, oid);
    assert oid in objs, format("osd.{0} pushes object {1} it does not have", me, oid);
    StartRecoveryOp();
    recovering[oid] = RC_PUSH();
    workStarted = true;
    foreach (o in arb) {
      if (o != me && o in peerMissing && oid in peerMissing[o]) {
        tgts += (o);
      }
    }
    if (sizeof(tgts) == 0) {
      OnGlobalRecover(oid);
      return 1;
    }
    StartPushes(oid, tgts);
    return 1;
  }

  fun StartPushes(oid: int, tgts: set[int]) {
    var o: int;
    var m: tMsg;
    pushing[oid] = tgts;
    pushVer[oid] = objs[oid].ver;
    foreach (o in tgts) {
      m = Msg(K_PUSH, me, o, pgEpoch, info.h.sis);
      m.oid = oid;
      m.obj = objs[oid];
      m.objValid = true;
      Send(m);
    }
  }

  // PrimaryLogPG::maybe_kick_recovery
  fun MaybeKickRecovery(oid: int) {
    if (!(oid in needsRecov) || oid in recovering || IsUnfound(oid)) {
      return;
    }
    if (oid in missing) {
      RecoverMissing(oid);
    } else {
      PrepObjectReplicaPushes(oid);
    }
  }

  // PrimaryLogPG::on_global_recover
  fun OnGlobalRecover(oid: int) {
    MLRecovered(oid);
    assert oid in recovering, format("osd.{0} on_global_recover: object {1} not recovering", me, oid);
    backfillsInFlight -= (oid);
    recovering -= (oid);
    if (oid in pushing) {
      pushing -= (oid);
    }
    if (oid in pushVer) {
      pushVer -= (oid);
    }
    FinishRecoveryOp();
    ReleaseBlocked(oid);
  }

  // PrimaryLogPG::cancel_pull
  fun CancelPull(oid: int) {
    assert oid in recovering, format("osd.{0} cancel_pull: object {1} not recovering", me, oid);
    recovering -= (oid);
    pulling -= (oid);
    FinishRecoveryOp();
    ReleaseBlocked(oid);
  }

  // ReplicatedBackend::_failed_pull, PrimaryLogPG::on_failed_pull
  fun FailedPull(oid: int, src: int) {
    var ms: map[int, tMiss];
    assert oid in recovering, format("osd.{0} on_failed_pull: object {1} not recovering", me, oid);
    recovering -= (oid);
    pulling -= (oid);
    // force_object_missing
    if (src in peerMissing) {
      ms = peerMissing[src];
    }
    ms[oid] = (need = missing[oid].need, have = 0);
    peerMissing[src] = ms;
    MLRebuild(oid);
    FinishRecoveryOp();
  }

  // MissingLoc::rebuild
  fun MLRebuild(oid: int) {
    var need: int;
    var have: bool;
    var o: int;
    var locs: set[int];
    MLRecovered(oid);
    if (oid in missing) {
      need = missing[oid].need;
      have = true;
    } else {
      foreach (o in arb) {
        if (o != me && !have) {
          assert o in peerMissing, format("osd.{0} MissingLoc::rebuild: no missing set for osd.{1}", me, o);
          if (oid in peerMissing[o]) {
            need = peerMissing[o][oid].need;
            have = true;
          }
        }
      }
    }
    if (!have) {
      return;
    }
    needsRecov[oid] = need;
    assert info.lb == LB_MAX(), format("osd.{0} MissingLoc::rebuild on a backfilling primary", me);
    assert info.lu >= need, format("osd.{0} MissingLoc::rebuild: last_update {1} < need {2}", me, info.lu, need);
    if (!(oid in missing)) {
      locs += (me);
    }
    foreach (o in keys(peerMissing)) {
      if (o != me) {
        assert o in peerInfo, format("osd.{0} MissingLoc::rebuild: no info for osd.{1}", me, o);
        if (need <= peerInfo[o].lu && oid <= peerInfo[o].lb && !(oid in peerMissing[o])) {
          locs += (o);
        }
      }
    }
    missingLoc[oid] = locs;
  }

  fun ReleaseBlocked(oid: int) {
    var keep: seq[tW];
    var i: int;
    while (i < sizeof(blockedWrites)) {
      if (blockedWrites[i].oid == oid) {
        send this, eRequeueWrite, (w = blockedWrites[i], inc = incarnation);
      } else {
        keep += (sizeof(keep), blockedWrites[i]);
      }
      i = i + 1;
    }
    blockedWrites = keep;
  }

  // ---------------------------------------------------------------- backfill

  fun BiBegin(): int {
    var oid: int;
    var b: int;
    b = LB_MAX();
    foreach (oid in keys(objs)) {
      if (oid > lastBackfillStarted && oid < b) {
        b = oid;
      }
    }
    return b;
  }

  fun PbiBegin(p: tPbi): int {
    var oid: int;
    var b: int;
    if (sizeof(p.objs) == 0) {
      return p.end;
    }
    b = LB_MAX();
    foreach (oid in keys(p.objs)) {
      if (oid < b) {
        b = oid;
      }
    }
    return b;
  }

  fun PbiTrimTo(p: tPbi, upto: int): tPbi {
    var oid: int;
    foreach (oid in keys(p.objs)) {
      if (oid <= upto) {
        p.objs -= (oid);
      }
    }
    return p;
  }

  fun EarliestPeerBackfill(): int {
    var bt: int;
    var e: int;
    e = LB_MAX();
    foreach (bt in backfillTargets) {
      assert bt in pbi, format("osd.{0} earliest_peer_backfill: no interval for osd.{1}", me, bt);
      if (PbiBegin(pbi[bt]) < e) {
        e = PbiBegin(pbi[bt]);
      }
    }
    return e;
  }

  fun AllPeerDone(): bool {
    var bt: int;
    foreach (bt in backfillTargets) {
      if (pbi[bt].end != LB_MAX() || sizeof(pbi[bt].objs) > 0) {
        return false;
      }
    }
    return true;
  }

  fun WriteLocked(oid: int): bool {
    var v: int;
    foreach (v in keys(inFlight)) {
      if (inFlight[v].oid == oid) {
        return true;
      }
    }
    return false;
  }

  // PrimaryLogPG::recover_backfill
  fun RecoverBackfill(max: int): int {
    var ops: int;
    var bt: int;
    var sentScan: bool;
    var bib: int;
    var check: int;
    var v: int;
    var p: tPbi;
    var need: set[int];
    var gone: set[int];
    var keep: set[int];
    var toRemove: seq[(oid: int, osd: int)];
    var m: tMsg;
    var i: int;
    var pos: int;
    var next: int;
    var nlb: int;
    var oid: int;
    var pi2: tInfo;
    assert sizeof(backfillTargets) > 0, format("osd.{0} recover_backfill without backfill tgts", me);
    if (newBackfill) {
      assert lastBackfillStarted == EarliestBackfill(),
        format("osd.{0} recover_backfill: last_backfill_started {1} != earliest_backfill {2}", me, lastBackfillStarted, EarliestBackfill());
      newBackfill = false;
      foreach (bt in backfillTargets) {
        pbi[bt] = (begin = peerInfo[bt].lb, end = peerInfo[bt].lb, objs = default(map[int, int]));
      }
      backfillsInFlight = default(set[int]);
      pendingBackfill = default(set[int]);
    }
    foreach (bt in backfillTargets) {
      if (peerInfo[bt].lb > lastBackfillStarted) {
        pbi[bt] = PbiTrimTo(pbi[bt], peerInfo[bt].lb);
      } else {
        pbi[bt] = PbiTrimTo(pbi[bt], lastBackfillStarted);
      }
    }
    while (ops < max) {
      bib = BiBegin();
      sentScan = false;
      foreach (bt in backfillTargets) {
        p = pbi[bt];
        if (PbiBegin(p) <= bib && p.end != LB_MAX() && sizeof(p.objs) == 0) {
          m = Msg(K_SCAN, me, bt, pgEpoch, lpr);
          m.begin = p.end;
          Send(m);
          assert !(bt in waitingOnBackfill), format("osd.{0} recover_backfill: already waiting on osd.{1}", me, bt);
          waitingOnBackfill += (bt);
          sentScan = true;
        }
      }
      if (sentScan) {
        ops = ops + 1;
        StartRecoveryOp();
        break;
      }
      if (bib == LB_MAX() && AllPeerDone()) {
        break;
      }
      check = EarliestPeerBackfill();
      if (check < bib) {
        foreach (bt in backfillTargets) {
          p = pbi[bt];
          if (PbiBegin(p) == check) {
            toRemove += (sizeof(toRemove), (oid = check, osd = bt));
            p.objs -= (check);
            pbi[bt] = p;
          }
        }
        lastBackfillStarted = check;
      } else {
        v = objs[bib].ver;
        need = default(set[int]);
        gone = default(set[int]);
        keep = default(set[int]);
        foreach (bt in backfillTargets) {
          p = pbi[bt];
          if (check == bib && PbiBegin(p) == bib) {
            if (p.objs[bib] != v) {
              need += (bt);
            } else {
              keep += (bt);
            }
          } else if (bib > peerInfo[bt].lb) {
            gone += (bt);
          }
        }
        if (sizeof(need) > 0 || sizeof(gone) > 0) {
          // obc->get_recovery_read()
          if (WriteLocked(bib)) {
            recoveryReadMarker += (bib);
            workStarted = true;
            break;
          }
          if (bib in recovering) {
            workStarted = true;
            break;
          }
          foreach (bt in gone) {
            need += (bt);
          }
          PrepBackfillObjectPush(bib, v, need);
          ops = ops + 1;
        }
        lastBackfillStarted = bib;
        pendingBackfill += (bib);
        foreach (bt in need) {
          if (!(bt in gone)) {
            p = pbi[bt];
            p.objs -= (bib);
            pbi[bt] = p;
          }
        }
        foreach (bt in keep) {
          p = pbi[bt];
          p.objs -= (bib);
          pbi[bt] = p;
        }
      }
    }
    i = 0;
    while (i < sizeof(toRemove)) {
      m = Msg(K_BACKFILL_REMOVE, me, toRemove[i].osd, pgEpoch, info.h.sis);
      m.oid = toRemove[i].oid;
      Send(m);
      if (toRemove[i].oid <= lastBackfillStarted) {
        pendingBackfill += (toRemove[i].oid);
      }
      i = i + 1;
    }
    pos = BiBegin();
    if (EarliestPeerBackfill() < pos) {
      pos = EarliestPeerBackfill();
    }
    next = pos;
    foreach (oid in backfillsInFlight) {
      if (oid < next) {
        next = oid;
      }
    }
    if (sizeof(backfillsInFlight) == 0) {
      next = pos;
    }
    nlb = EarliestBackfill();
    while (sizeof(pendingBackfill) > 0 && MinOf(pendingBackfill) < next) {
      oid = MinOf(pendingBackfill);
      assert oid > nlb, format("osd.{0} recover_backfill: pending update {1} not after new_last_backfill {2}", me, oid, nlb);
      nlb = oid;
      pendingBackfill -= (oid);
    }
    assert sizeof(pendingBackfill) > 0 || nlb == lastBackfillStarted,
      format("osd.{0} recover_backfill: no pending updates but new_last_backfill {1} != last_backfill_started {2}", me, nlb, lastBackfillStarted);
    if (sizeof(pendingBackfill) == 0 && pos == LB_MAX()) {
      assert sizeof(backfillsInFlight) == 0, format("osd.{0} recover_backfill: done with backfills in flight", me);
      nlb = pos;
      lastBackfillStarted = pos;
    }
    foreach (bt in backfillTargets) {
      if (nlb > peerInfo[bt].lb) {
        pi2 = peerInfo[bt];
        pi2.lb = nlb;
        peerInfo[bt] = pi2;
        if (nlb == LB_MAX()) {
          m = Msg(K_BACKFILL_FINISH, me, bt, pgEpoch, lpr);
          StartRecoveryOp();
        } else {
          m = Msg(K_BACKFILL_PROGRESS, me, bt, pgEpoch, lpr);
        }
        m.lb = nlb;
        Send(m);
      }
    }
    if (ops > 0) {
      workStarted = true;
    }
    return ops;
  }

  fun MinOf(s: set[int]): int {
    var x: int;
    var r: int;
    r = LB_MAX() + 1;
    foreach (x in s) {
      if (x < r) {
        r = x;
      }
    }
    return r;
  }

  // PrimaryLogPG::prep_backfill_object_push
  fun PrepBackfillObjectPush(oid: int, v: int, tgts: set[int]) {
    var bt: int;
    var ms: map[int, tMiss];
    assert sizeof(tgts) > 0, format("osd.{0} backfill push with no tgts", me);
    backfillsInFlight += (oid);
    // prepare_backfill_for_missing
    foreach (bt in tgts) {
      ms = default(map[int, tMiss]);
      if (bt in peerMissing) {
        ms = peerMissing[bt];
      }
      ms[oid] = (need = v, have = 0);
      peerMissing[bt] = ms;
    }
    assert !(oid in recovering), format("osd.{0} prep_backfill_object_push: object {1} already recovering", me, oid);
    StartRecoveryOp();
    recovering[oid] = RC_BACKFILL();
    StartPushes(oid, tgts);
  }

  // --------------------------------------------------------- data messages

  // PG::can_discard_replica_op, can_discard_scan, can_discard_backfill
  fun DataStale(m: tMsg): bool {
    var n: tMap;
    if (m.kind == K_SCAN || m.kind == K_SCAN_DIGEST || m.kind == K_BACKFILL_PROGRESS ||
        m.kind == K_BACKFILL_FINISH || m.kind == K_BACKFILL_FINISH_ACK) {
      return lpr > m.req;
    }
    n = maps[newest];
    return !n.up[m.src] || n.downAt[m.src] >= m.epSent || lpr > m.epSent;
  }

  // PrimaryLogPG::do_request: before the PG is peered only a pull is served
  fun DataMsg(m: tMsg) {
    if (DataStale(m)) {
      return;
    }
    if (!IsPeered() && m.kind != K_PULL) {
      waitingPeered += (sizeof(waitingPeered), m);
      return;
    }
    if (m.kind == K_REPOP) { HandleRepop(m); return; }
    if (m.kind == K_REPOP_REPLY) { HandleRepopReply(m); return; }
    if (m.kind == K_PUSH && m.pull) { HandlePullResponse(m); return; }
    if (m.kind == K_PUSH) { HandlePush(m); return; }
    if (m.kind == K_PUSH_REPLY) { HandlePushReply(m); return; }
    if (m.kind == K_PULL) { HandlePull(m); return; }
    HandleBackfillMsg(m);
  }

  fun ReplayPeered() {
    var q: seq[tMsg];
    var i: int;
    q = waitingPeered;
    waitingPeered = default(seq[tMsg]);
    while (i < sizeof(q)) {
      send this, eRequeueMsg, (m = q[i], inc = incarnation);
      i = i + 1;
    }
  }

  // ReplicatedBackend::handle_pull
  fun HandlePull(m: tMsg) {
    var r: tMsg;
    r = Msg(K_PUSH, me, m.src, pgEpoch, m.req);
    r.oid = m.oid;
    r.pull = true;
    if (m.oid in objs) {
      r.obj = objs[m.oid];
      r.objValid = true;
    }
    Send(r);
  }

  // ReplicatedBackend::handle_pull_response
  fun HandlePullResponse(m: tMsg) {
    var o: int;
    var tgts: set[int];
    if (!m.objValid) {
      if (m.oid in pulling && pulling[m.oid] == m.src) {
        FailedPull(m.oid, m.src);
      }
      return;
    }
    if (!(m.oid in pulling) || pulling[m.oid] != m.src) {
      return;
    }
    pulling -= (m.oid);
    // on_local_recover -> recover_got: only a version at least the one needed
    SetObj(m.oid, m.obj);
    if (m.oid in missing && missing[m.oid].need <= m.obj.ver) {
      missing -= (m.oid);
    }
    info.lc = LastComplete(log, info.tail, info.lu, missing);
    // C_ReplicatedBackend_OnPullComplete: push on to the peers missing it
    foreach (o in arb) {
      if (o != me && o in peerMissing && m.oid in peerMissing[o]) {
        tgts += (o);
      }
    }
    if (sizeof(tgts) == 0) {
      OnGlobalRecover(m.oid);
    } else {
      recovering[m.oid] = RC_PUSH();
      StartPushes(m.oid, tgts);
    }
  }

  // ReplicatedBackend::handle_push (a replica or backfill target)
  fun HandlePush(m: tMsg) {
    var r: tMsg;
    if (m.objValid) {
      SetObj(m.oid, m.obj);
    }
    if (m.oid in missing && missing[m.oid].need <= m.obj.ver) {
      missing -= (m.oid);
    }
    info.lc = LastComplete(log, info.tail, info.lu, missing);
    r = Msg(K_PUSH_REPLY, me, m.src, pgEpoch, m.req);
    r.oid = m.oid;
    Send(r);
    // PG::_committed_pushed_object -> recovery_committed_to
    lcod = info.lc;
    if (lcod == info.lu && !IsPrimary()) {
      r = Msg(K_TRIM, me, Primary(), pgEpoch, pgEpoch);
      r.lcod = lcod;
      Send(r);
    }
  }

  // ReplicatedBackend::handle_push_reply -> on_peer_recover
  fun HandlePushReply(m: tMsg) {
    var left: set[int];
    var ms: map[int, tMiss];
    if (!(m.oid in pushing) || !(m.src in pushing[m.oid])) {
      return;
    }
    left = pushing[m.oid];
    left -= (m.src);
    pushing[m.oid] = left;
    // pg_missing_t::got
    assert m.src in peerMissing && m.oid in peerMissing[m.src],
      format("osd.{0} on_peer_recover: object {1} not missing on osd.{2}", me, m.oid, m.src);
    ms = peerMissing[m.src];
    assert ms[m.oid].need <= pushVer[m.oid],
      format("osd.{0} on_peer_recover: osd.{1} needs {2} of object {3}, pushed {4}", me, m.src, ms[m.oid].need, m.oid, pushVer[m.oid]);
    ms -= (m.oid);
    peerMissing[m.src] = ms;
    MLAddLocation(m.oid, m.src);
    if (sizeof(left) == 0) {
      OnGlobalRecover(m.oid);
    }
  }

  // PrimaryLogPG::do_scan, do_backfill, do_backfill_remove
  fun HandleBackfillMsg(m: tMsg) {
    var r: tMsg;
    var oid: int;
    var p: tPbi;
    if (m.kind == K_SCAN) {
      if (pgFull) {
        Queue(Evt(E_BackfillTooFull));
        return;
      }
      r = Msg(K_SCAN_DIGEST, me, m.src, pgEpoch, m.req);
      r.begin = m.begin;
      foreach (oid in keys(objs)) {
        if (oid >= m.begin) {
          r.digest[oid] = objs[oid].ver;
        }
      }
      Send(r);
    } else if (m.kind == K_SCAN_DIGEST) {
      assert m.src in backfillTargets, format("osd.{0} do_scan: digest from osd.{1}, not a backfill target", me, m.src);
      p = (begin = m.begin, end = LB_MAX(), objs = m.digest);
      pbi[m.src] = p;
      if (m.src in waitingOnBackfill) {
        waitingOnBackfill -= (m.src);
        if (sizeof(waitingOnBackfill) == 0) {
          assert sizeof(pbi) == sizeof(backfillTargets), format("osd.{0} do_scan: backfill intervals do not match tgts", me);
          FinishRecoveryOp();
        }
      }
    } else if (m.kind == K_BACKFILL_PROGRESS || m.kind == K_BACKFILL_FINISH) {
      if (m.kind == K_BACKFILL_FINISH) {
        r = Msg(K_BACKFILL_FINISH_ACK, me, m.src, pgEpoch, m.req);
        Send(r);
        Queue(Evt(E_RecoveryDone));
      }
      info.lb = m.lb;                         // update_backfill_progress
    } else if (m.kind == K_BACKFILL_FINISH_ACK) {
      assert IsPrimary(), format("osd.{0} backfill finish ack but not primary", me);
      FinishRecoveryOp();
    } else if (m.kind == K_BACKFILL_REMOVE) {
      RemoveObj(m.oid);
    }
  }

  // ------------------------------------------------------------------ writes

  // PrimaryLogPG::do_request / do_op
  fun HandleWrite(wid: int, oid: int, epoch: int) {
    var w: tW;
    w = (wid = wid, oid = oid, epoch = epoch);
    if (!hasPg) {
      // a PG that should exist here and does not yet: the op waits
      if (MapsHere()) {
        heldWrites += (sizeof(heldWrites), w);
      }
      return;
    }
    if (!IsPrimary() || epoch < info.h.sis) {
      return;                                 // misdirected, or sent to an old interval: the client resends
    }
    if (!IsPeered()) {
      queuedWrites += (sizeof(queuedWrites), w);
      return;
    }
    TryWrite(w);
  }

  fun ReleaseWrites() {
    var pw: seq[tW];
    var i: int;
    pw = heldWrites;
    heldWrites = default(seq[tW]);
    while (i < sizeof(pw)) {
      HandleWrite(pw[i].wid, pw[i].oid, pw[i].epoch);
      i = i + 1;
    }
  }

  fun TryWrite(w: tW) {
    var i: int;
    if (!Test(B_ACTIVE)) {
      queuedWrites += (sizeof(queuedWrites), w);  // waiting_for_active
      return;
    }
    if (w.oid in missing || IsDegradedOrBackfilling(w.oid)) {
      blockedWrites += (sizeof(blockedWrites), w);
      MaybeKickRecovery(w.oid);
      return;
    }
    if (w.oid in recovering) {
      blockedWrites += (sizeof(blockedWrites), w);  // the recovery read lock
      return;
    }
    // a resent write already in the log (the reqid dup check)
    while (i < sizeof(log)) {
      if (log[i].wid == w.wid) {
        if (!(log[i].ver in inFlight)) {
          Ack(log[i]);
        }
        return;
      }
      i = i + 1;
    }
    DoWrite(w.wid, w.oid);
  }

  // PrimaryLogPG::is_degraded_or_backfilling_object
  fun IsDegradedOrBackfilling(oid: int): bool {
    var o: int;
    if (oid in missing) {
      return true;
    }
    foreach (o in arb) {
      if (o != me) {
        if (o in peerMissing && oid in peerMissing[o] && !(o in asyncTargets)) {
          return true;
        }
        if (o in backfillTargets && peerInfo[o].lb <= oid && lastBackfillStarted >= oid && oid in backfillsInFlight) {
          return true;
        }
      }
    }
    return false;
  }

  // PrimaryLogPG::should_send_op
  fun ShouldSendOp(peer: int, oid: int): bool {
    if (peer in backfillTargets && oid > peerInfo[peer].lb && oid > lastBackfillStarted) {
      return false;
    }
    if (peer in asyncTargets && peer in peerMissing && oid in peerMissing[peer]) {
      return false;
    }
    return true;
  }

  // execute_ctx -> issue_repop
  fun DoWrite(wid: int, oid: int) {
    var v: int;
    var prior: int;
    var e: tEntry;
    var o: int;
    var p: tInfo;
    var ob: tObjv;
    var waiting: set[int];
    var m: tMsg;
    var locs: set[int];
    var needLoc: bool;
    v = Ver(pgEpoch, VNum(info.lu) + 1);
    if (oid in objs) {
      prior = objs[oid].ver;
      ob.wids = objs[oid].wids;
    }
    e = (ver = v, oid = oid, wid = wid, prior = prior);
    // PeeringState::pre_submit_op
    foreach (o in arb) {
      if (o != me) {
        p = peerInfo[o];
        if (p.lc == p.lu) {
          p.lc = v;
        }
        p.lu = v;
        peerInfo[o] = p;
      }
    }
    foreach (o in asyncTargets) {
      if (o != me && o in peerMissing && oid in peerMissing[o]) {
        peerMissing[o] = AddNextEvent(peerMissing[o], e);
        needLoc = true;
      }
    }
    if (needLoc) {
      needsRecov[oid] = v;
      foreach (o in acting) {
        if (o == me || !(o in peerMissing) || !(oid in peerMissing[o])) {
          locs += (o);
        }
      }
      missingLoc[oid] = locs;
    }
    // append_log: add_log_entry, then trim
    assert v > info.lu, format("osd.{0} add_log_entry: version {1} <= last_update {2}", me, v, info.lu);
    log += (sizeof(log), e);
    info.lu = v;
    ob.ver = v;
    ob.wids += (wid);
    SetObj(oid, ob);
    UpdateTrimTo();
    SetStore(TrimChecked(Store(), pgTrimTo, true, false));
    foreach (o in arb) {
      if (o != me) {
        waiting += (o);
        m = Msg(K_REPOP, me, o, pgEpoch, info.h.sis);
        m.ent = e;
        m.logOnly = !ShouldSendOp(o, oid);
        m.trimTo = pgTrimTo;
        m.pct = pgCommittedTo;
        Send(m);
      }
    }
    inFlight[v] = (wid = wid, oid = oid, waiting = waiting);
    DrainCommitted();
  }

  // PeeringState::calc_trim_to_aggressive (osd_pg_log_trim_min 1)
  fun UpdateTrimTo() {
    var limit: int;
    var keep: int;
    limit = info.lu;
    if (pgCommittedTo < limit) {
      limit = pgCommittedTo;
    }
    if (limit == 0 || limit == pgTrimTo || sizeof(log) <= cfg.maxLog) {
      return;
    }
    keep = log[sizeof(log) - cfg.maxLog - 1].ver;
    if (keep < limit) {
      pgTrimTo = keep;
    } else {
      pgTrimTo = limit;
    }
  }

  // PGLog::trim
  fun TrimChecked(st: tStore, trimTo: int, applied: bool, async: bool): tStore {
    var lc: int;
    if (trimTo > st.tail) {
      lc = LastComplete(st.log, st.tail, st.lu, st.missing);
      if (applied && !async && sizeof(st.missing) == 0 && trimTo > lc) {
        st.failed = format("PGLog::trim: trim_to {0} > last_complete {1}", trimTo, lc);
        return st;
      }
      st = TrimLog(st, trimTo);
    }
    return st;
  }

  // ReplicatedBackend::do_repop
  fun HandleRepop(m: tMsg) {
    var async: bool;
    var ob: tObjv;
    var r: tMsg;
    assert m.epSent >= info.h.sis,
      format("osd.{0} do_repop: map_epoch {1} < same_interval_since {2}", me, m.epSent, info.h.sis);
    async = m.ent.oid in missing;
    if (async) {
      missing = AddNextEvent(missing, m.ent);   // add_local_next_event
    }
    assert m.ent.ver > info.lu,
      format("osd.{0} add_log_entry: version {1} <= last_update {2}", me, m.ent.ver, info.lu);
    log += (sizeof(log), m.ent);
    info.lu = m.ent.ver;
    if (!m.logOnly) {
      if (m.ent.oid in objs) {
        ob.wids = objs[m.ent.oid].wids;
      }
      ob.ver = m.ent.ver;
      ob.wids += (m.ent.wid);
      SetObj(m.ent.oid, ob);
    }
    SetStore(TrimChecked(Store(), m.trimTo, !m.logOnly, async));
    pgCommittedTo = m.pct;
    r = Msg(K_REPOP_REPLY, me, m.src, pgEpoch, m.req);
    r.ent = m.ent;
    r.lcod = info.lc;
    Send(r);
  }

  fun HandleRepopReply(m: tMsg) {
    var f: (wid: int, oid: int, waiting: set[int]);
    if (!(m.ent.ver in inFlight)) {
      return;
    }
    f = inFlight[m.ent.ver];
    f.waiting -= (m.src);
    inFlight[m.ent.ver] = f;
    peerLcod[m.src] = m.lcod;
    DrainCommitted();
  }

  // repops complete in order: complete_write, then the ack
  fun DrainCommitted() {
    var v: int;
    var first: int;
    var e: tEntry;
    while (sizeof(inFlight) > 0) {
      first = -1;
      foreach (v in keys(inFlight)) {
        if (first == -1 || v < first) {
          first = v;
        }
      }
      if (sizeof(inFlight[first].waiting) > 0) {
        return;
      }
      e = (ver = first, oid = inFlight[first].oid, wid = inFlight[first].wid, prior = 0);
      inFlight -= (first);
      pgCommittedTo = first;
      lcod = info.lc;
      CalcMinLcod();
      Ack(e);
      ReleaseBlocked(e.oid);
      // ObcLockManager::put_locks: the lock is free and recovery wanted it
      if (e.oid in recoveryReadMarker && !WriteLocked(e.oid)) {
        recoveryReadMarker -= (e.oid);
        QueueRecovery();
      }
    }
  }

  fun Ack(e: tEntry) {
    announce mAcked, (wid = e.wid, oid = e.oid, ver = e.ver, ep = info.les, osd = me);
    send client, eWriteAck, e.wid;
  }

  // BEGIN GENERATED DISPATCH
  // generated by chartgen.py dispatch; do not edit
  fun Custom(s: tS, ev: tEvt): tResult {
    if (s == S_Reset) {
      if (ev.e == E_QueryState) { return R_Reset_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_Reset_QueryUnfound(ev); }
      if (ev.e == E_AdvMap) { return R_Reset_AdvMap(ev); }
      if (ev.e == E_ActMap) { return R_Reset_ActMap(ev); }
      if (ev.e == E_IntervalFlush) { return R_Reset_IntervalFlush(ev); }
    }
    if (s == S_Started) {
      if (ev.e == E_QueryState) { return R_Started_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_Started_QueryUnfound(ev); }
      if (ev.e == E_AdvMap) { return R_Started_AdvMap(ev); }
      if (ev.e == E_IntervalFlush) { return R_Started_IntervalFlush(ev); }
    }
    if (s == S_Primary) {
      if (ev.e == E_ActMap) { return R_Primary_ActMap(ev); }
      if (ev.e == E_MNotifyRec) { return R_Primary_MNotifyRec(ev); }
      if (ev.e == E_SetForceRecovery) { return R_Primary_SetForceRecovery(ev); }
      if (ev.e == E_UnsetForceRecovery) { return R_Primary_UnsetForceRecovery(ev); }
      if (ev.e == E_SetForceBackfill) { return R_Primary_SetForceBackfill(ev); }
      if (ev.e == E_UnsetForceBackfill) { return R_Primary_UnsetForceBackfill(ev); }
      if (ev.e == E_RequestScrub) { return R_Primary_RequestScrub(ev); }
    }
    if (s == S_WaitActingChange) {
      if (ev.e == E_QueryState) { return R_WaitActingChange_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_WaitActingChange_QueryUnfound(ev); }
      if (ev.e == E_AdvMap) { return R_WaitActingChange_AdvMap(ev); }
      if (ev.e == E_MLogRec) { return R_WaitActingChange_MLogRec(ev); }
      if (ev.e == E_MInfoRec) { return R_WaitActingChange_MInfoRec(ev); }
      if (ev.e == E_MNotifyRec) { return R_WaitActingChange_MNotifyRec(ev); }
    }
    if (s == S_Peering) {
      if (ev.e == E_QueryState) { return R_Peering_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_Peering_QueryUnfound(ev); }
      if (ev.e == E_AdvMap) { return R_Peering_AdvMap(ev); }
    }
    if (s == S_Active) {
      if (ev.e == E_QueryState) { return R_Active_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_Active_QueryUnfound(ev); }
      if (ev.e == E_ActMap) { return R_Active_ActMap(ev); }
      if (ev.e == E_AdvMap) { return R_Active_AdvMap(ev); }
      if (ev.e == E_MInfoRec) { return R_Active_MInfoRec(ev); }
      if (ev.e == E_MNotifyRec) { return R_Active_MNotifyRec(ev); }
      if (ev.e == E_MLogRec) { return R_Active_MLogRec(ev); }
      if (ev.e == E_MTrim) { return R_Active_MTrim(ev); }
      if (ev.e == E_Backfilled) { return R_Active_Backfilled(ev); }
      if (ev.e == E_ActivateCommitted) { return R_Active_ActivateCommitted(ev); }
      if (ev.e == E_AllReplicasActivated) { return R_Active_AllReplicasActivated(ev); }
      if (ev.e == E_DeferRecovery) { return R_Active_DeferRecovery(ev); }
      if (ev.e == E_DeferBackfill) { return R_Active_DeferBackfill(ev); }
      if (ev.e == E_UnfoundRecovery) { return R_Active_UnfoundRecovery(ev); }
      if (ev.e == E_UnfoundBackfill) { return R_Active_UnfoundBackfill(ev); }
      if (ev.e == E_RemoteReservationRevokedTooFull) { return R_Active_RemoteReservationRevokedTooFull(ev); }
      if (ev.e == E_RemoteReservationRevoked) { return R_Active_RemoteReservationRevoked(ev); }
      if (ev.e == E_DoRecovery) { return R_Active_DoRecovery(ev); }
      if (ev.e == E_RenewLease) { return R_Active_RenewLease(ev); }
      if (ev.e == E_MLeaseAck) { return R_Active_MLeaseAck(ev); }
      if (ev.e == E_CheckReadable) { return R_Active_CheckReadable(ev); }
      if (ev.e == E_PgCreateEvt) { return R_Active_PgCreateEvt(ev); }
    }
    if (s == S_Recovered) {
      if (ev.e == E_AllReplicasActivated) { return R_Recovered_AllReplicasActivated(ev); }
    }
    if (s == S_Backfilling) {
      if (ev.e == E_Backfilled) { return R_Backfilling_Backfilled(ev); }
      if (ev.e == E_DeferBackfill) { return R_Backfilling_DeferBackfill(ev); }
      if (ev.e == E_UnfoundBackfill) { return R_Backfilling_UnfoundBackfill(ev); }
      if (ev.e == E_RemoteReservationRejectedTooFull) { return R_Backfilling_RemoteReservationRejectedTooFull(ev); }
      if (ev.e == E_RemoteReservationRevokedTooFull) { return R_Backfilling_RemoteReservationRevokedTooFull(ev); }
      if (ev.e == E_RemoteReservationRevoked) { return R_Backfilling_RemoteReservationRevoked(ev); }
    }
    if (s == S_WaitRemoteBackfillReserved) {
      if (ev.e == E_RemoteBackfillReserved) { return R_WaitRemoteBackfillReserved_RemoteBackfillReserved(ev); }
      if (ev.e == E_RemoteReservationRejectedTooFull) { return R_WaitRemoteBackfillReserved_RemoteReservationRejectedTooFull(ev); }
      if (ev.e == E_RemoteReservationRevoked) { return R_WaitRemoteBackfillReserved_RemoteReservationRevoked(ev); }
    }
    if (s == S_WaitLocalBackfillReserved) {
      if (ev.e == E_RemoteBackfillReserved) { return R_WaitLocalBackfillReserved_RemoteBackfillReserved(ev); }
    }
    if (s == S_NotBackfilling) {
      if (ev.e == E_QueryUnfound) { return R_NotBackfilling_QueryUnfound(ev); }
      if (ev.e == E_RemoteBackfillReserved) { return R_NotBackfilling_RemoteBackfillReserved(ev); }
      if (ev.e == E_RemoteReservationRejectedTooFull) { return R_NotBackfilling_RemoteReservationRejectedTooFull(ev); }
    }
    if (s == S_NotRecovering) {
      if (ev.e == E_QueryUnfound) { return R_NotRecovering_QueryUnfound(ev); }
      if (ev.e == E_DeferRecovery) { return R_NotRecovering_DeferRecovery(ev); }
      if (ev.e == E_UnfoundRecovery) { return R_NotRecovering_UnfoundRecovery(ev); }
    }
    if (s == S_ReplicaActive) {
      if (ev.e == E_QueryState) { return R_ReplicaActive_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_ReplicaActive_QueryUnfound(ev); }
      if (ev.e == E_ActMap) { return R_ReplicaActive_ActMap(ev); }
      if (ev.e == E_MQuery) { return R_ReplicaActive_MQuery(ev); }
      if (ev.e == E_MInfoRec) { return R_ReplicaActive_MInfoRec(ev); }
      if (ev.e == E_MLogRec) { return R_ReplicaActive_MLogRec(ev); }
      if (ev.e == E_MTrim) { return R_ReplicaActive_MTrim(ev); }
      if (ev.e == E_Activate) { return R_ReplicaActive_Activate(ev); }
      if (ev.e == E_ActivateCommitted) { return R_ReplicaActive_ActivateCommitted(ev); }
      if (ev.e == E_DeferRecovery) { return R_ReplicaActive_DeferRecovery(ev); }
      if (ev.e == E_DeferBackfill) { return R_ReplicaActive_DeferBackfill(ev); }
      if (ev.e == E_UnfoundRecovery) { return R_ReplicaActive_UnfoundRecovery(ev); }
      if (ev.e == E_UnfoundBackfill) { return R_ReplicaActive_UnfoundBackfill(ev); }
      if (ev.e == E_RemoteBackfillPreempted) { return R_ReplicaActive_RemoteBackfillPreempted(ev); }
      if (ev.e == E_RemoteRecoveryPreempted) { return R_ReplicaActive_RemoteRecoveryPreempted(ev); }
      if (ev.e == E_RecoveryDone) { return R_ReplicaActive_RecoveryDone(ev); }
      if (ev.e == E_BackfillTooFull) { return R_ReplicaActive_BackfillTooFull(ev); }
      if (ev.e == E_MLease) { return R_ReplicaActive_MLease(ev); }
    }
    if (s == S_RepRecovering) {
      if (ev.e == E_BackfillTooFull) { return R_RepRecovering_BackfillTooFull(ev); }
      if (ev.e == E_RemoteRecoveryPreempted) { return R_RepRecovering_RemoteRecoveryPreempted(ev); }
      if (ev.e == E_RemoteBackfillPreempted) { return R_RepRecovering_RemoteBackfillPreempted(ev); }
    }
    if (s == S_RepWaitBackfillReserved) {
      if (ev.e == E_BackfillTooFull) { return R_RepWaitBackfillReserved_BackfillTooFull(ev); }
      if (ev.e == E_RemoteBackfillReserved) { return R_RepWaitBackfillReserved_RemoteBackfillReserved(ev); }
      if (ev.e == E_RejectTooFullRemoteReservation) { return R_RepWaitBackfillReserved_RejectTooFullRemoteReservation(ev); }
      if (ev.e == E_RemoteReservationRejectedTooFull) { return R_RepWaitBackfillReserved_RemoteReservationRejectedTooFull(ev); }
      if (ev.e == E_RemoteReservationCanceled) { return R_RepWaitBackfillReserved_RemoteReservationCanceled(ev); }
    }
    if (s == S_RepWaitRecoveryReserved) {
      if (ev.e == E_RemoteRecoveryReserved) { return R_RepWaitRecoveryReserved_RemoteRecoveryReserved(ev); }
      if (ev.e == E_RemoteReservationRejectedTooFull) { return R_RepWaitRecoveryReserved_RemoteReservationRejectedTooFull(ev); }
      if (ev.e == E_RemoteReservationCanceled) { return R_RepWaitRecoveryReserved_RemoteReservationCanceled(ev); }
    }
    if (s == S_RepNotRecovering) {
      if (ev.e == E_RequestRecoveryPrio) { return R_RepNotRecovering_RequestRecoveryPrio(ev); }
      if (ev.e == E_RequestBackfillPrio) { return R_RepNotRecovering_RequestBackfillPrio(ev); }
      if (ev.e == E_RejectTooFullRemoteReservation) { return R_RepNotRecovering_RejectTooFullRemoteReservation(ev); }
      if (ev.e == E_RemoteRecoveryReserved) { return R_RepNotRecovering_RemoteRecoveryReserved(ev); }
      if (ev.e == E_RemoteBackfillReserved) { return R_RepNotRecovering_RemoteBackfillReserved(ev); }
    }
    if (s == S_Recovering) {
      if (ev.e == E_AllReplicasRecovered) { return R_Recovering_AllReplicasRecovered(ev); }
      if (ev.e == E_DeferRecovery) { return R_Recovering_DeferRecovery(ev); }
      if (ev.e == E_UnfoundRecovery) { return R_Recovering_UnfoundRecovery(ev); }
      if (ev.e == E_RequestBackfill) { return R_Recovering_RequestBackfill(ev); }
    }
    if (s == S_WaitRemoteRecoveryReserved) {
      if (ev.e == E_RemoteRecoveryReserved) { return R_WaitRemoteRecoveryReserved_RemoteRecoveryReserved(ev); }
    }
    if (s == S_WaitLocalRecoveryReserved) {
      if (ev.e == E_RecoveryTooFull) { return R_WaitLocalRecoveryReserved_RecoveryTooFull(ev); }
    }
    if (s == S_Stray) {
      if (ev.e == E_MQuery) { return R_Stray_MQuery(ev); }
      if (ev.e == E_MLogRec) { return R_Stray_MLogRec(ev); }
      if (ev.e == E_MInfoRec) { return R_Stray_MInfoRec(ev); }
      if (ev.e == E_ActMap) { return R_Stray_ActMap(ev); }
      if (ev.e == E_RecoveryDone) { return R_Stray_RecoveryDone(ev); }
    }
    if (s == S_ToDelete) {
      if (ev.e == E_ActMap) { return R_ToDelete_ActMap(ev); }
      if (ev.e == E_ActivateCommitted) { return R_ToDelete_ActivateCommitted(ev); }
      if (ev.e == E_DeleteSome) { return R_ToDelete_DeleteSome(ev); }
    }
    if (s == S_Deleting) {
      if (ev.e == E_DeleteSome) { return R_Deleting_DeleteSome(ev); }
    }
    if (s == S_GetInfo) {
      if (ev.e == E_QueryState) { return R_GetInfo_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_GetInfo_QueryUnfound(ev); }
      if (ev.e == E_MNotifyRec) { return R_GetInfo_MNotifyRec(ev); }
    }
    if (s == S_GetLog) {
      if (ev.e == E_QueryState) { return R_GetLog_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_GetLog_QueryUnfound(ev); }
      if (ev.e == E_MLogRec) { return R_GetLog_MLogRec(ev); }
      if (ev.e == E_GotLog) { return R_GetLog_GotLog(ev); }
      if (ev.e == E_AdvMap) { return R_GetLog_AdvMap(ev); }
    }
    if (s == S_GetMissing) {
      if (ev.e == E_QueryState) { return R_GetMissing_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_GetMissing_QueryUnfound(ev); }
      if (ev.e == E_MLogRec) { return R_GetMissing_MLogRec(ev); }
    }
    if (s == S_WaitUpThru) {
      if (ev.e == E_QueryState) { return R_WaitUpThru_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_WaitUpThru_QueryUnfound(ev); }
      if (ev.e == E_ActMap) { return R_WaitUpThru_ActMap(ev); }
      if (ev.e == E_MLogRec) { return R_WaitUpThru_MLogRec(ev); }
    }
    if (s == S_Down) {
      if (ev.e == E_QueryState) { return R_Down_QueryState(ev); }
      if (ev.e == E_QueryUnfound) { return R_Down_QueryUnfound(ev); }
      if (ev.e == E_MNotifyRec) { return R_Down_MNotifyRec(ev); }
    }
    if (s == S_Incomplete) {
      if (ev.e == E_AdvMap) { return R_Incomplete_AdvMap(ev); }
      if (ev.e == E_MNotifyRec) { return R_Incomplete_MNotifyRec(ev); }
      if (ev.e == E_QueryUnfound) { return R_Incomplete_QueryUnfound(ev); }
      if (ev.e == E_QueryState) { return R_Incomplete_QueryState(ev); }
    }
    assert false, format("osd.{0}: no react() for {1} in {2}", me, ev.e, StateName(s));
    return RES_DISCARD;
  }

  fun Enter(s: tS) {
    if (s == S_Crashed) { En_Crashed(); return; }
    if (s == S_Initial) { En_Initial(); return; }
    if (s == S_Reset) { En_Reset(); return; }
    if (s == S_Started) { En_Started(); return; }
    if (s == S_Start) { En_Start(); return; }
    if (s == S_Primary) { En_Primary(); return; }
    if (s == S_WaitActingChange) { En_WaitActingChange(); return; }
    if (s == S_Peering) { En_Peering(); return; }
    if (s == S_Active) { En_Active(); return; }
    if (s == S_Clean) { En_Clean(); return; }
    if (s == S_Recovered) { En_Recovered(); return; }
    if (s == S_Backfilling) { En_Backfilling(); return; }
    if (s == S_WaitRemoteBackfillReserved) { En_WaitRemoteBackfillReserved(); return; }
    if (s == S_WaitLocalBackfillReserved) { En_WaitLocalBackfillReserved(); return; }
    if (s == S_NotBackfilling) { En_NotBackfilling(); return; }
    if (s == S_NotRecovering) { En_NotRecovering(); return; }
    if (s == S_ReplicaActive) { En_ReplicaActive(); return; }
    if (s == S_RepRecovering) { En_RepRecovering(); return; }
    if (s == S_RepWaitBackfillReserved) { En_RepWaitBackfillReserved(); return; }
    if (s == S_RepWaitRecoveryReserved) { En_RepWaitRecoveryReserved(); return; }
    if (s == S_RepNotRecovering) { En_RepNotRecovering(); return; }
    if (s == S_Recovering) { En_Recovering(); return; }
    if (s == S_WaitRemoteRecoveryReserved) { En_WaitRemoteRecoveryReserved(); return; }
    if (s == S_WaitLocalRecoveryReserved) { En_WaitLocalRecoveryReserved(); return; }
    if (s == S_Activating) { En_Activating(); return; }
    if (s == S_Stray) { En_Stray(); return; }
    if (s == S_ToDelete) { En_ToDelete(); return; }
    if (s == S_WaitDeleteReserved) { En_WaitDeleteReserved(); return; }
    if (s == S_Deleting) { En_Deleting(); return; }
    if (s == S_GetInfo) { En_GetInfo(); return; }
    if (s == S_GetLog) { En_GetLog(); return; }
    if (s == S_GetMissing) { En_GetMissing(); return; }
    if (s == S_WaitUpThru) { En_WaitUpThru(); return; }
    if (s == S_Down) { En_Down(); return; }
    if (s == S_Incomplete) { En_Incomplete(); return; }
  }

  fun Exit(s: tS) {
    if (s == S_Crashed) { Ex_Crashed(); return; }
    if (s == S_Initial) { Ex_Initial(); return; }
    if (s == S_Reset) { Ex_Reset(); return; }
    if (s == S_Started) { Ex_Started(); return; }
    if (s == S_Start) { Ex_Start(); return; }
    if (s == S_Primary) { Ex_Primary(); return; }
    if (s == S_WaitActingChange) { Ex_WaitActingChange(); return; }
    if (s == S_Peering) { Ex_Peering(); return; }
    if (s == S_Active) { Ex_Active(); return; }
    if (s == S_Clean) { Ex_Clean(); return; }
    if (s == S_Recovered) { Ex_Recovered(); return; }
    if (s == S_Backfilling) { Ex_Backfilling(); return; }
    if (s == S_WaitRemoteBackfillReserved) { Ex_WaitRemoteBackfillReserved(); return; }
    if (s == S_WaitLocalBackfillReserved) { Ex_WaitLocalBackfillReserved(); return; }
    if (s == S_NotBackfilling) { Ex_NotBackfilling(); return; }
    if (s == S_NotRecovering) { Ex_NotRecovering(); return; }
    if (s == S_ReplicaActive) { Ex_ReplicaActive(); return; }
    if (s == S_RepRecovering) { Ex_RepRecovering(); return; }
    if (s == S_RepWaitBackfillReserved) { Ex_RepWaitBackfillReserved(); return; }
    if (s == S_RepWaitRecoveryReserved) { Ex_RepWaitRecoveryReserved(); return; }
    if (s == S_RepNotRecovering) { Ex_RepNotRecovering(); return; }
    if (s == S_Recovering) { Ex_Recovering(); return; }
    if (s == S_WaitRemoteRecoveryReserved) { Ex_WaitRemoteRecoveryReserved(); return; }
    if (s == S_WaitLocalRecoveryReserved) { Ex_WaitLocalRecoveryReserved(); return; }
    if (s == S_Activating) { Ex_Activating(); return; }
    if (s == S_Stray) { Ex_Stray(); return; }
    if (s == S_ToDelete) { Ex_ToDelete(); return; }
    if (s == S_WaitDeleteReserved) { Ex_WaitDeleteReserved(); return; }
    if (s == S_Deleting) { Ex_Deleting(); return; }
    if (s == S_GetInfo) { Ex_GetInfo(); return; }
    if (s == S_GetLog) { Ex_GetLog(); return; }
    if (s == S_GetMissing) { Ex_GetMissing(); return; }
    if (s == S_WaitUpThru) { Ex_WaitUpThru(); return; }
    if (s == S_Down) { Ex_Down(); return; }
    if (s == S_Incomplete) { Ex_Incomplete(); return; }
  }

  // END GENERATED DISPATCH
}

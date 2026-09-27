/*
 * Peering of one replicated placement group (src/osd/PeeringState.cc).
 *
 * A monitor publishes OSDMap epochs; OSDs each host one instance of the
 * PG and run the peering state machine over every epoch in order. An
 * environment crashes, restarts and marks OSDs down, out and lost, and a
 * client writes. The PG's log is a list of versioned entries, each
 * carrying the id of the client write it holds; objects are not modelled
 * apart from the log (no missing sets, no recovery, no backfill).
 *
 * Epochs are small integers. An eversion (epoch, version) is encoded as
 * epoch * 1000 + version, which orders the same way.
 */

// a scripted environment step (tCfg.script); a random one when the script is empty
enum tOp {
  OP_CRASH, OP_DETECT, OP_RESTART, OP_FALSE_DOWN, OP_OUT, OP_IN, OP_LOST, OP_MIN_SIZE, OP_WRITE,
  OP_WAIT_ACTIVE,         // until a primary goes active in a newer interval
  OP_WAIT_UP,             // until the mon marks osd up
  OP_WAIT_ACKED,          // until every write so far is acked
  OP_HOLD_MAPS,           // the mon holds osd's maps back (the OSD lags) ...
  OP_RELEASE_MAPS         // ... and delivers them in one batch
}
type tStep = (op: tOp, osd: int);

type tCfg = (
  nOsds: int,
  crush: seq[int],        // CRUSH preference order for the PG
  size: int,
  minSize: int,
  writes: int,            // client writes issued while the environment runs
  chaos: int,             // failure events before the cluster settles
  // what the environment may do
  crash: bool,            // an OSD process dies; the mon marks it down later
  falseDown: bool,        // the mon marks a running OSD down; it boots again
  outs: bool,             // mark an OSD out (CRUSH remaps the PG) and back in
  lost: bool,             // mark a crashed, down OSD lost (it never returns)
  minSizeChange: bool,    // change the pool's min_size
  batchMaps: bool,        // the mon may hold maps back and deliver them in a batch
  script: seq[tStep],     // if not empty, these steps instead of random ones
  // design, as in Ceph (a bug configuration turns one off)
  upThruGate: bool,       // a primary waits until up_thru covers its interval before activating
  historyLes: bool,       // find_best_info honours history.last_epoch_started
  staleFilter: bool,      // PG::old_peering_msg drops peering messages from before the last reset
  repopFilter: bool,      // PG::can_discard_replica_op drops repops from an old interval
  // proposed fix
  getInfoKeepsRequest: bool  // GetInfo counts a notify as the peer's reply only if proc_replica_notify took it
);

type tHistory = (
  created: int,           // epoch_created (0: the PG does not exist here)
  les: int,               // last_epoch_started
  lis: int,               // last_interval_started
  lec: int,               // last_epoch_clean
  sis: int                // same_interval_since
);

type tInfo = (
  lu: int,                // last_update
  les: int,               // last_epoch_started
  lis: int,               // last_interval_started
  h: tHistory
);

type tEntry = (ver: int, wid: int);

// PastIntervals::pg_interval_t, and pi_compact_rep's view of it
type tInterval = (first: int, last: int, acting: set[int], primary: int, rw: bool);
type tPI = (first: int, last: int, all: set[int], ivs: seq[tInterval]);

// PastIntervals::PriorSet
type tPrior = (probe: set[int], down: set[int], blockedBy: map[int, int], pgDown: bool);

type tMap = (
  epoch: int,
  crush: seq[int],
  up: map[int, bool],
  inn: map[int, bool],
  upFrom: map[int, int],
  addr: map[int, int],    // each up OSD's address (a nonce per boot)
  upThru: map[int, int],
  downAt: map[int, int],
  lostAt: map[int, int],
  pgTemp: seq[int],
  size: int,
  minSize: int
);

enum tKind {
  K_QUERY_INFO,           // MOSDPGQuery2 (pg_query_t::INFO)
  K_QUERY_LOG,            // MOSDPGQuery2 (pg_query_t::LOG)
  K_NOTIFY,               // MOSDPGNotify2
  K_LOG,                  // MOSDPGLog
  K_INFO,                 // MOSDPGInfo2
  K_REPOP,                // MOSDRepOp
  K_REPOP_REPLY           // MOSDRepOpReply
}

type tMsg = (
  kind: tKind,
  src: int,
  dst: int,
  toAddr: int,            // the address the sender's map gives the destination
  epSent: int,              // epoch_sent (map_epoch for a repop)
  req: int,               // query_epoch / min_epoch
  info: tInfo,
  log: seq[tEntry],
  pi: tPI,
  tail: int,              // the log's tail (MOSDPGLog)
  ent: tEntry
);

// OSD <-> OSD
event ePeer: tMsg;
// mon -> OSDs and client
event eMaps: seq[tMap];
// OSD -> mon
event eBoot: (osd: int, have: int, addr: int);
event eAlive: (osd: int, addr: int, want: int, version: int);
event ePgTemp: (osd: int, addr: int, want: seq[int], epoch: int);
// failure detection: the mon eventually learns an OSD process at this address died
event eDied: (osd: int, addr: int);
// environment
event eSetup: (mon: machine, osds: map[int, machine], client: machine, env: machine);
event eCrash;
event eRestart;
event eEnvDown: set[int];
event eEnvOut: (osd: int, out: bool);
event eEnvLost: int;
event eEnvMinSize: int;
event eStep;
event eIssueWrite: int;
event eNoteActive: int;   // osd -> env (scripted runs): went active in the interval starting then
event eNoteUp: int;       // mon -> env (scripted runs): marked osd up
event eNoteAcked: int;    // client -> env (scripted runs): a write was acked
event eHoldMaps: (osd: int, hold: bool);   // env -> mon (scripted runs)
// client
event eWrite: (wid: int, epoch: int);
event eWriteAck: int;

// monitor-only (announced, never sent)
event mMap: (epoch: int, sis: int);                       // the PG's interval start as of this epoch
event mSettled;                                           // the environment has stopped
event mLost: int;                                         // an OSD was marked lost
event mHolds: int;                                        // an OSD has an instance of the PG
event mIssued: int;                                       // a client write was issued
event mAcked: (wid: int, ep: int, osd: int);              // osd acked a write; it had activated in epoch ep
event mActivated: (osd: int, epoch: int, wids: set[int]); // a primary activated a writeable acting set with this log
event mActive: (osd: int, sis: int);                      // AllReplicasActivated, writeable

fun Ver(e: int, v: int): int { return e * 1000 + v; }
fun VEpoch(x: int): int { return x / 1000; }
fun VNum(x: int): int { return x % 1000; }

fun Contains(s: seq[int], x: int): bool {
  var i: int;
  while (i < sizeof(s)) {
    if (s[i] == x) {
      return true;
    }
    i = i + 1;
  }
  return false;
}

fun ToSet(s: seq[int]): set[int] {
  var r: set[int];
  var i: int;
  while (i < sizeof(s)) {
    r += (s[i]);
    i = i + 1;
  }
  return r;
}

// CRUSH: the first `size` OSDs in the preference order that are in
fun RawUp(m: tMap): seq[int] {
  var r: seq[int];
  var i: int;
  while (i < sizeof(m.crush) && sizeof(r) < m.size) {
    if (m.inn[m.crush[i]]) {
      r += (sizeof(r), m.crush[i]);
    }
    i = i + 1;
  }
  return r;
}

// OSDMap::_raw_to_up_osds: a replicated pool drops down OSDs
fun UpSet(m: tMap): seq[int] {
  var raw: seq[int];
  var r: seq[int];
  var i: int;
  raw = RawUp(m);
  while (i < sizeof(raw)) {
    if (m.up[raw[i]]) {
      r += (sizeof(r), raw[i]);
    }
    i = i + 1;
  }
  return r;
}

// OSDMap::_get_temp_osds: pg_temp less its down OSDs, or up if that leaves none
fun ActingSet(m: tMap): seq[int] {
  var r: seq[int];
  var i: int;
  while (i < sizeof(m.pgTemp)) {
    if (m.up[m.pgTemp[i]]) {
      r += (sizeof(r), m.pgTemp[i]);
    }
    i = i + 1;
  }
  if (sizeof(r) == 0) {
    return UpSet(m);
  }
  return r;
}

fun PrimaryOf(s: seq[int]): int {
  if (sizeof(s) == 0) {
    return -1;
  }
  return s[0];
}

// PastIntervals::is_new_interval, for one PG of a replicated pool
fun NewInterval(last: tMap, m: tMap): bool {
  return UpSet(last) != UpSet(m) || ActingSet(last) != ActingSet(m) ||
    last.size != m.size || last.minSize != m.minSize;
}

fun HasBeenUpSince(m: tMap, o: int, e: int): bool {
  return m.up[o] && m.upFrom[o] <= e;
}

fun EmptyHistory(): tHistory {
  return (created = 0, les = 0, lis = 0, lec = 0, sis = 0);
}

fun EmptyInfo(): tInfo {
  return (lu = 0, les = 0, lis = 0, h = EmptyHistory());
}

fun EmptyPI(): tPI {
  return (first = 0, last = 0, all = default(set[int]), ivs = default(seq[tInterval]));
}

// pg_history_t::merge: the fields that cannot be computed from the OSDMap
fun MergeHistory(h: tHistory, o: tHistory): tHistory {
  if (o.created > h.created) { h.created = o.created; }
  if (o.les > h.les) { h.les = o.les; }
  if (o.lis > h.lis) { h.lis = o.lis; }
  if (o.lec > h.lec) { h.lec = o.lec; }
  return h;
}

// pi_compact_rep::add_interval
fun PiAdd(pi: tPI, iv: tInterval): tPI {
  var o: int;
  var i: int;
  var kept: seq[tInterval];
  var last: tInterval;
  if (pi.first == 0) {
    pi.first = iv.first;
  }
  assert iv.last > pi.last, format("interval [{0},{1}] does not follow past_intervals ending {2}", iv.first, iv.last, pi.last);
  pi.last = iv.last;
  foreach (o in iv.acting) {
    pi.all += (o);
  }
  if (!iv.rw) {
    return pi;
  }
  pi.ivs += (sizeof(pi.ivs), iv);
  // drop earlier intervals the new one supersedes (its acting set is a subset of theirs)
  last = pi.ivs[sizeof(pi.ivs) - 1];
  i = 0;
  while (i < sizeof(pi.ivs) - 1) {
    if (!Supersedes(last.acting, pi.ivs[i].acting)) {
      kept += (sizeof(kept), pi.ivs[i]);
    }
    i = i + 1;
  }
  kept += (sizeof(kept), last);
  pi.ivs = kept;
  return pi;
}

fun Supersedes(newer: set[int], older: set[int]): bool {
  var o: int;
  foreach (o in newer) {
    if (!(o in older)) {
      return false;
    }
  }
  return true;
}

fun PiEmpty(pi: tPI): bool {
  return pi.first > pi.last || (pi.first == 0 && pi.last == 0);
}

// PastIntervals::check_new_interval: the interval [sis, m.epoch - 1] that
// `last` ended, and whether it may have gone read-write
fun ClosedInterval(sis: int, lec: int, last: tMap, m: tMap): tInterval {
  var iv: tInterval;
  var acting: seq[int];
  var p: int;
  acting = ActingSet(last);
  p = PrimaryOf(acting);
  iv = (first = sis, last = m.epoch - 1, acting = ToSet(acting), primary = p, rw = false);
  if (sizeof(acting) > 0 && p != -1 && sizeof(acting) >= last.minSize) {
    if (last.upThru[p] >= iv.first && last.upFrom[p] <= iv.first) {
      iv.rw = true;
    } else if (lec >= iv.first && lec <= iv.last) {
      iv.rw = true;
    }
  }
  return iv;
}

// PastIntervals::PriorSet::PriorSet, with PeeringState::build_prior's osd state function
fun BuildPrior(pi: tPI, les: int, up: seq[int], acting: seq[int], m: tMap): tPrior {
  var o: int;
  var pr: tPrior;
  var i: int;
  var iv: tInterval;
  var upNow: set[int];
  var cand: map[int, int];
  var anyDown: bool;
  foreach (o in acting) { pr.probe += (o); }
  foreach (o in up) { pr.probe += (o); }
  foreach (o in pi.all) {
    if (m.up[o]) {
      pr.probe += (o);
    } else {
      pr.down += (o);
    }
  }
  i = sizeof(pi.ivs) - 1;
  while (i >= 0) {
    iv = pi.ivs[i];
    if (iv.last < les) {
      i = -1;
    } else {
      upNow = default(set[int]);
      cand = default(map[int, int]);
      anyDown = false;
      foreach (o in iv.acting) {
        if (m.up[o]) {
          upNow += (o);
        } else if (m.lostAt[o] > iv.first) {
          upNow += (o);
        } else {
          cand[o] = m.lostAt[o];
          anyDown = true;
        }
      }
      // replicated pools: recoverable with any one member
      if (sizeof(upNow) == 0 && anyDown) {
        pr.pgDown = true;
        foreach (o in keys(cand)) {
          pr.blockedBy[o] = cand[o];
        }
      }
      i = i - 1;
    }
  }
  return pr;
}

// PastIntervals::PriorSet::affected_by_map
fun AffectedByMap(pr: tPrior, m: tMap): bool {
  var o: int;
  foreach (o in pr.probe) {
    if (!m.up[o] && !(o in pr.down)) {
      return true;
    }
    if (o in pr.blockedBy && m.lostAt[o] != pr.blockedBy[o]) {
      return true;
    }
  }
  foreach (o in pr.down) {
    if (m.up[o]) {
      return true;
    }
    if (o in pr.blockedBy && m.lostAt[o] != pr.blockedBy[o]) {
      return true;
    }
  }
  return false;
}

// PeeringState::find_best_info (replicated: newest last_update, then the
// current primary). -1 if none qualifies.
fun FindBestInfo(infos: map[int, tInfo], n: int, restrict: bool, up: seq[int], acting: seq[int],
                 me: int, historyLes: bool): int {
  var maxLes: int;
  var minLua: int;
  var best: int;
  var o: int;
  var i: tInfo;
  minLua = -1;
  o = 0;
  while (o < n) {
    if (o in infos) {
      i = infos[o];
      if (historyLes && maxLes < i.h.les) {
        maxLes = i.h.les;
      }
      if (maxLes < i.les) {
        maxLes = i.les;
      }
    }
    o = o + 1;
  }
  o = 0;
  while (o < n) {
    if (o in infos) {
      i = infos[o];
      if (maxLes <= i.les && (minLua == -1 || i.lu < minLua)) {
        minLua = i.lu;
      }
    }
    o = o + 1;
  }
  if (minLua == -1) {
    return -1;
  }
  best = -1;
  o = 0;
  while (o < n) {
    if (o in infos) {
      i = infos[o];
      if ((!restrict || Contains(up, o) || Contains(acting, o)) && i.lu >= minLua && i.les >= maxLes) {
        if (best == -1 || i.lu > infos[best].lu || (i.lu == infos[best].lu && o == me)) {
          best = o;
        }
      }
    }
    o = o + 1;
  }
  return best;
}

// PeeringState::calc_replicated_acting with no log trimming: every peer is
// contiguous with the authoritative log, so none needs backfill
fun CalcReplicatedActing(primary: int, size: int, acting: seq[int], up: seq[int],
                         infos: map[int, tInfo], n: int, restrict: bool): seq[int] {
  var want: seq[int];
  var cands: seq[int];
  var i: int;
  var o: int;
  want += (0, primary);
  i = 0;
  while (i < sizeof(up)) {
    if (up[i] != primary) {
      want += (sizeof(want), up[i]);
    }
    i = i + 1;
  }
  if (sizeof(want) >= size) {
    return want;
  }
  i = 0;
  while (i < sizeof(acting)) {
    if (acting[i] != primary && !Contains(up, acting[i])) {
      cands += (sizeof(cands), acting[i]);
    }
    i = i + 1;
  }
  want = AppendByLastUpdate(want, cands, infos, size);
  if (sizeof(want) >= size || restrict) {
    return want;
  }
  cands = default(seq[int]);
  o = 0;
  while (o < n) {
    if (o in infos && o != primary && !Contains(up, o) && !Contains(acting, o)) {
      cands += (sizeof(cands), o);
    }
    o = o + 1;
  }
  return AppendByLastUpdate(want, cands, infos, size);
}

// append candidates newest last_update first until want has `size` members
fun AppendByLastUpdate(want: seq[int], cands: seq[int], infos: map[int, tInfo], size: int): seq[int] {
  var best: int;
  var i: int;
  while (sizeof(want) < size && sizeof(cands) > 0) {
    best = 0;
    i = 1;
    while (i < sizeof(cands)) {
      if (infos[cands[i]].lu > infos[cands[best]].lu) {
        best = i;
      }
      i = i + 1;
    }
    want += (sizeof(want), cands[best]);
    cands -= (best);
  }
  return want;
}

fun Wids(log: seq[tEntry]): set[int] {
  var r: set[int];
  var i: int;
  while (i < sizeof(log)) {
    r += (log[i].wid);
    i = i + 1;
  }
  return r;
}

fun Head(log: seq[tEntry]): int {
  if (sizeof(log) == 0) {
    return 0;
  }
  return log[sizeof(log) - 1].ver;
}

fun HasEntry(log: seq[tEntry], e: tEntry): bool {
  var i: int;
  while (i < sizeof(log)) {
    if (log[i] == e) {
      return true;
    }
    i = i + 1;
  }
  return false;
}

// PGLog::merge_log: `olog` is authoritative, with head `ohead` and tail
// `otail` (pg_log_t::copy_after). If it is behind us, our entries after its
// head are divergent. If it is ahead, the cut point is the newest of its
// entries not beyond our head (or its tail); our entries after the cut are
// divergent and its entries after the cut are appended.
fun MergeLog(mine: seq[tEntry], olog: seq[tEntry], ohead: int, otail: int): seq[tEntry] {
  var lb: int;
  var r: seq[tEntry];
  var i: int;
  var head: int;
  head = Head(mine);
  if (ohead < head) {
    mine = RewindTo(mine, ohead);
    head = ohead;
  }
  if (ohead <= head) {
    return mine;
  }
  lb = otail;
  i = 0;
  while (i < sizeof(olog)) {
    if (olog[i].ver <= head && olog[i].ver > lb) {
      lb = olog[i].ver;
    }
    i = i + 1;
  }
  i = 0;
  while (i < sizeof(mine)) {
    if (mine[i].ver <= lb) {
      r += (sizeof(r), mine[i]);
    }
    i = i + 1;
  }
  i = 0;
  while (i < sizeof(olog)) {
    if (olog[i].ver > lb) {
      r += (sizeof(r), olog[i]);
    }
    i = i + 1;
  }
  return r;
}

// PGLog::rewind_divergent_log
fun RewindTo(log: seq[tEntry], upTo: int): seq[tEntry] {
  var r: seq[tEntry];
  var i: int;
  while (i < sizeof(log)) {
    if (log[i].ver <= upTo) {
      r += (sizeof(r), log[i]);
    }
    i = i + 1;
  }
  return r;
}

// PGLog::proc_replica_log: the newest entry the replica shares with us
fun CommonHead(mine: seq[tEntry], theirs: seq[tEntry]): int {
  var r: int;
  var i: int;
  while (i < sizeof(theirs)) {
    if (HasEntry(mine, theirs[i])) {
      r = theirs[i].ver;
    }
    i = i + 1;
  }
  return r;
}

fun Msg(kind: tKind, src: int, dst: int, toAddr: int, epSent: int, req: int): tMsg {
  return (kind = kind, src = src, dst = dst, toAddr = toAddr, epSent = epSent, req = req,
          info = EmptyInfo(), log = default(seq[tEntry]), pi = EmptyPI(),
          tail = 0, ent = (ver = 0, wid = 0));
}

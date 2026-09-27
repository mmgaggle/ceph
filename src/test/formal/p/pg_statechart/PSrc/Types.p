/*
 * The PeeringState statechart of one replicated PG, with the data it acts
 * on: objects, the PG log, missing sets, backfill cursors, and the local and
 * remote reservations that recovery and backfill take.
 *
 * The chart itself (states, outer states, initial states, reaction lists)
 * is generated from src/osd/PeeringState.h into Chart.p; Osd.p routes
 * events through it as boost::statechart does and implements every custom
 * reaction and every state's entry and exit.
 *
 * Encodings:
 *   epochs are small integers; an eversion (epoch, version) is
 *   epoch * 1000 + version, which orders the same way
 *   objects are 1 .. nObjects, in hobject order; a backfill cursor
 *   (last_backfill) is 0 for hobject_t() (nothing) and LB_MAX for
 *   hobject_t::get_max() (complete)
 */

type tCfg = (
  nOsds: int,
  crush: seq[int],
  size: int,
  minSize: int,
  nObjects: int,
  maxLog: int,              // osd_max_pg_log_entries: the log is trimmed to this many entries
  maxBackfills: int,        // osd_max_backfills: local and remote reserver slots
  maxRecoveryOps: int,      // recovery ops started per start_recovery_ops
  asyncMinCost: int,        // osd_async_recovery_min_cost
  writes: int,
  chaos: int,
  // what the environment may do
  crash: bool,
  falseDown: bool,
  outs: bool,
  lost: bool,
  minSizeChange: bool,
  batchMaps: bool,
  full: bool,               // an OSD may go (backfill/recovery) toofull and back
  preempt: bool,            // other PGs' reservations may preempt ours
  commands: bool,           // the mgr may send force-recovery/backfill and scrub requests
  // PGs per OSD
  maxPgs: int,              // mon_max_pg_per_osd * osd_max_pg_per_osd_hard_ratio (0: no limit)
  others: int,              // other PGs each OSD holds to begin with
  pgChurn: bool,            // other PGs come and go on the OSDs
  // the cluster network
  partitions: bool,         // links between OSDs may fail and heal
  minReporters: int,        // mon_osd_min_down_reporters (each OSD its own host)
  maxMarkdowns: int,        // osd_max_markdown_count (within osd_max_markdown_period)
  keepCuts: bool,           // links that are down stay down when the cluster settles
  script: seq[tStep],
  // design, as in Ceph
  upThruGate: bool,
  historyLes: bool,
  staleFilter: bool,
  // proposed fixes
  getInfoKeepsRequest: bool,
  advMapFullCheck: bool,    // register the AdvMap reactions of Wait{Local,Remote}RecoveryReserved (tracker 70670)
  resetIgnoresCommands: bool, // Reset discards SetForce*, UnsetForce* and RequestScrub, as Started does
  grantFromCheck: bool,     // a grant counts only for the request outstanding (the shard last asked, the
                            // reserver's latest request); Active discards others
  deferWhileWaiting: bool,  // WaitRemote*Reserved give up on a preemption; RepWaitRecoveryReserved on a release
  pendingKeepsUp: bool,     // consume_map keeps a withheld creation while the OSD is in up (not only acting)
  twiddleFix: bool,         // resume_creating_pg twiddles a one-OSD acting set with a second up OSD (as pg repeer)
  // checks beyond Ceph's own asserts
  slotCheck: bool           // recovery, backfill and deletion run holding their local reservation
);

enum tOp {
  OP_CRASH, OP_DETECT, OP_RESTART, OP_FALSE_DOWN, OP_OUT, OP_IN, OP_LOST, OP_MIN_SIZE, OP_WRITE,
  OP_FULL, OP_NOT_FULL, OP_PREEMPT, OP_FORCE, OP_SCRUB, OP_BUSY, OP_IDLE, OP_WAIT_STATE,
  OP_PG_ADD, OP_PG_DEL,     // another PG is created on / removed from osd
  OP_CUT, OP_CUT1, OP_HEAL, // the cluster link osd<->peer fails (OP_CUT1: osd->peer only), heals
  OP_ISOLATE,               // every cluster link of osd fails
  OP_WAIT_ACTIVE, OP_WAIT_UP, OP_WAIT_ACKED, OP_WAIT_CLEAN, OP_HOLD_MAPS, OP_RELEASE_MAPS
}
type tStep = (op: tOp, osd: int, st: tS, peer: int);   // st: OP_WAIT_STATE's state; peer: OP_CUT*

// ---------------------------------------------------------------- PG data

type tHistory = (created: int, les: int, lis: int, lec: int, sis: int);

type tInfo = (
  lu: int,                  // last_update
  lc: int,                  // last_complete
  tail: int,                // log_tail
  les: int,                 // last_epoch_started
  lis: int,                 // last_interval_started
  lb: int,                  // last_backfill
  h: tHistory
);

// a pg_log_entry_t (MODIFY): object oid moved from version prior to ver
type tEntry = (ver: int, oid: int, wid: int, prior: int);

// a pg_missing_t item
type tMiss = (need: int, have: int);

type tInterval = (first: int, last: int, acting: set[int], primary: int, rw: bool);
type tPI = (first: int, last: int, all: set[int], ivs: seq[tInterval]);
type tPrior = (probe: set[int], down: set[int], blockedBy: map[int, int], pgDown: bool);

type tMap = (
  epoch: int,
  crush: seq[int],
  up: map[int, bool],
  inn: map[int, bool],
  upFrom: map[int, int],
  addr: map[int, int],
  upThru: map[int, int],
  downAt: map[int, int],
  lostAt: map[int, int],
  fullOsds: set[int],       // OSDs with the FULL state bit (OSDMap::check_full)
  pgTemp: seq[int],
  size: int,
  minSize: int
);

fun LB_MAX(): int { return 1000; }

// ------------------------------------------------------------- messages

enum tKind {
  K_QUERY_INFO,             // MOSDPGQuery2 INFO
  K_QUERY_LOG,              // MOSDPGQuery2 LOG / FULLLOG
  K_NOTIFY,                 // MOSDPGNotify2
  K_LOG,                    // MOSDPGLog (info, log, missing)
  K_INFO,                   // MOSDPGInfo2
  K_TRIM,                   // MOSDPGTrim
  K_REPOP,                  // MOSDRepOp
  K_REPOP_REPLY,            // MOSDRepOpReply
  K_PUSH,                   // MOSDPGPush (recovery or backfill)
  K_PUSH_REPLY,             // MOSDPGPushReply
  K_PULL,                   // MOSDPGPull
  K_BACKFILL_PROGRESS,      // MOSDPGBackfill OP_BACKFILL_PROGRESS
  K_BACKFILL_FINISH,        // MOSDPGBackfill OP_BACKFILL_FINISH
  K_BACKFILL_FINISH_ACK,    // MOSDPGBackfill OP_BACKFILL_FINISH_ACK
  K_SCAN,                   // MOSDPGScan OP_SCAN_GET_DIGEST
  K_SCAN_DIGEST,            // MOSDPGScan OP_SCAN_DIGEST
  K_RESERVE,                // MRecoveryReserve / MBackfillReserve (see tRes)
  K_REMOVE,                 // MOSDPGRemove
  K_BACKFILL_REMOVE         // MOSDPGBackfillRemove
}

// MRecoveryReserve and MBackfillReserve ops
enum tRes {
  RES_NONE,
  RES_REQUEST, RES_GRANT, RES_RELEASE, RES_REVOKE,                        // recovery and backfill
  RES_REJECT_TOOFULL, RES_REVOKE_TOOFULL                                  // backfill
}

type tMsg = (
  kind: tKind,
  src: int,
  dst: int,
  toAddr: int,
  epSent: int,              // epoch_sent / map_epoch
  req: int,                 // query_epoch / min_epoch
  info: tInfo,
  log: seq[tEntry],
  tail: int,
  pi: tPI,
  missing: map[int, tMiss],
  ent: tEntry,              // repop entry
  logOnly: bool,            // repop: log entry only (object beyond last_backfill)
  trimTo: int,              // repop: pg_trim_to
  lcod: int,                // repop reply / MOSDPGTrim: last_complete_ondisk
  pull: bool,               // push: a reply to a pull
  digest: map[int, int],    // scan digest: object -> version
  oid: int,                 // push / pull
  obj: tObjv,
  objValid: bool,          // push: the object exists (else remove it)
  lb: int,                  // backfill progress
  pct: int,                 // repop: pg_committed_to
  pinc: int,                // (model) the sending PG's process incarnation, to its own OSD
  begin: int,               // scan: where the scan starts
  backfill: bool,           // reservation: backfill (MBackfillReserve) or recovery
  res: tRes,
  prio: int
);

// ------------------------------------------------------------- events

// a statechart event with its payload
type tEvt = (
  e: tE,
  m: tMsg,
  lastEpoch: int,           // AdvMap: lastmap, osdmap
  newEpoch: int,
  a: int,                   // Activate / ActivateCommitted: activation epoch; *Prio: priority
  fromResv: bool,           // an AsyncReserver callback (grant or preempt)
  rseq: int                 // ... for the PG's request with this number
);

// a client write waiting in the PG
type tW = (wid: int, oid: int, epoch: int);

// a backfill target's ReplicaBackfillInterval: what its last scan found
type tPbi = (begin: int, end: int, objs: map[int, int]);

// what a recovering object is waiting for
fun RC_PULL(): int { return 1; }
fun RC_PUSH(): int { return 2; }
fun RC_BACKFILL(): int { return 3; }

// ------------------------------------------------------------- reservations

// an OSD's AsyncReserver
fun PG_ITEM(): int { return 0; }        // this PG
fun OTHER_ITEM(): int { return 100; }   // some other PG
type tResvItem = (item: int, prio: int, grant: tEvt, preempt: tEvt, hasPreempt: bool, epoch: int, rseq: int);
type tReserver = (max: int, queue: seq[tResvItem], inProgress: map[int, tResvItem]);
enum tRq { RQ_REQUEST, RQ_CANCEL, RQ_PRIO }
type tResvReq = (op: tRq, item: int, local: bool, prio: int, grant: tEvt, preempt: tEvt, hasPreempt: bool, epoch: int, rseq: int,
                 inc: int);   // inc: the PG's process incarnation (-1: the OSD's own)
event eResv: tResvReq;

// ------------------------------------------------------------- P events

event ePeer: tMsg;
event eMaps: seq[tMap];
event eBoot: (osd: int, have: int, addr: int);
event eAlive: (osd: int, addr: int, want: int, version: int);
event ePgTemp: (osd: int, addr: int, want: seq[int], epoch: int, forced: bool);
event eDied: (osd: int, addr: int);
event eSetup: (mon: machine, osds: map[int, machine], client: machine, env: machine);
event eCrash;
event eRestart;
event eEnvDown: set[int];
event eEnvOut: (osd: int, out: bool);
event eEnvLost: int;
event eEnvMinSize: int;
event eStep;
event eIssueWrite: int;
event eWrite: (wid: int, oid: int, epoch: int);
event eWriteAck: int;
event eNoteActive: int;
event eNoteClean: int;
event eNoteUp: int;
event eNoteAcked: int;
event eHoldMaps: (osd: int, hold: bool);
// a PGPeeringEvent the PG or OSD queued for itself, with its epochs
event eQueued: (evt: tEvt, es: int, er: int, inc: int);   // a PGPeeringEvent: epoch_sent, epoch_requested
// an OSD's reservers grant, preempt or reject asynchronously
event eFull: bool;
event ePreempt;
event eEnvFull: (osd: int, full: bool);
event eCommand: tE;         // the mgr: SetForce*, UnsetForce*, RequestScrub
event eBusy: bool;          // another PG takes (true) or gives back an OSD's local and remote slots
event eOthers: int;         // other PGs on the OSD: +1 or -1
event ePgOthers: int;       // the OSD to its PG: how many other PGs it holds
event eLink: (peer: int, outCut: bool, inCut: bool);   // the cluster link to and from peer
event eHbGrace: (peer: int, gen: int);                 // the OSD to itself: heartbeat grace expired
event eFailure: (reporter: int, raddr: int, target: int, taddr: int, epoch: int, alive: bool);  // MOSDFailure
event eNoteExit: int;       // an OSD shut itself down (marked down too often)
event eNoteState: (osd: int, st: tS);

// monitor-only
event mMap: (epoch: int, sis: int);
event mSettled;
event mLost: int;
event mHolds: int;
event mIssued: int;
event mAcked: (wid: int, oid: int, ver: int, ep: int, osd: int);
event mActive: (osd: int, sis: int);
event mClean: (osd: int, sis: int, acting: seq[int]);
event mObj: (osd: int, oid: int, obj: tObjv, valid: bool);   // an OSD's copy of an object changed
event mCrashed: (osd: int, where: string, ev: tE);
event mDeleted: int;
event mReserve: (osd: int, pgOsd: int, local: bool, held: bool);

// ------------------------------------------------------------- helpers

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
  return (lu = 0, lc = 0, tail = 0, les = 0, lis = 0, lb = LB_MAX(), h = EmptyHistory());
}

fun EmptyPI(): tPI {
  return (first = 0, last = 0, all = default(set[int]), ivs = default(seq[tInterval]));
}

fun MergeHistory(h: tHistory, o: tHistory): tHistory {
  if (o.created > h.created) { h.created = o.created; }
  if (o.les > h.les) { h.les = o.les; }
  if (o.lis > h.lis) { h.lis = o.lis; }
  if (o.lec > h.lec) { h.lec = o.lec; }
  return h;
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

fun PiAdd(pi: tPI, iv: tInterval): tPI {
  var i: int;
  var o: int;
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

fun BuildPrior(pi: tPI, les: int, up: seq[int], acting: seq[int], m: tMap): tPrior {
  var pr: tPrior;
  var i: int;
  var o: int;
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

fun Incomplete(i: tInfo): bool {
  return i.lb != LB_MAX();
}

// PeeringState::calculate_maxles_and_minlua + find_best_info (replicated)
fun FindBestInfo(infos: map[int, tInfo], n: int, restrict: bool, up: seq[int], acting: seq[int],
                 me: int, historyLes: bool): int {
  var maxLes: int;
  var minLua: int;
  var best: int;
  var o: int;
  var i: tInfo;
  var b: tInfo;
  minLua = -1;
  o = 0;
  while (o < n) {
    if (o in infos) {
      i = infos[o];
      if (historyLes && maxLes < i.h.les) {
        maxLes = i.h.les;
      }
      if (!Incomplete(i) && maxLes < i.les) {
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
      if ((!restrict || Contains(up, o) || Contains(acting, o)) && i.lu >= minLua &&
          i.les >= maxLes && !Incomplete(i)) {
        if (best == -1) {
          best = o;
        } else {
          b = infos[best];
          if (i.lu > b.lu) {
            best = o;
          } else if (i.lu == b.lu) {
            // a longer tail, then no missing, then the current primary
            if (i.tail < b.tail) {
              best = o;
            } else if (i.tail == b.tail) {
              if (i.lc == i.lu && b.lc != b.lu) {
                best = o;
              } else if ((i.lc == i.lu) == (b.lc == b.lu) && o == me) {
                best = o;
              }
            }
          }
        }
      }
    }
    o = o + 1;
  }
  return best;
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

fun Head(log: seq[tEntry], tail: int): int {
  if (sizeof(log) == 0) {
    return tail;
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

fun EntryAt(log: seq[tEntry], v: int): tEntry {
  var i: int;
  while (i < sizeof(log)) {
    if (log[i].ver == v) {
      return log[i];
    }
    i = i + 1;
  }
  return (ver = 0, oid = 0, wid = 0, prior = 0);
}

fun After(log: seq[tEntry], v: int): seq[tEntry] {
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

fun UpTo(log: seq[tEntry], v: int): seq[tEntry] {
  var r: seq[tEntry];
  var i: int;
  while (i < sizeof(log)) {
    if (log[i].ver <= v) {
      r += (sizeof(r), log[i]);
    }
    i = i + 1;
  }
  return r;
}

// pg_log_t::copy_after's tail: the newest entry at or before v, or the tail
fun TailFor(log: seq[tEntry], tail: int, v: int): int {
  var t: int;
  var i: int;
  t = tail;
  while (i < sizeof(log)) {
    if (log[i].ver <= v) {
      t = log[i].ver;
    }
    i = i + 1;
  }
  return t;
}

// the newest version of oid in log up to v, or -1
fun NewestFor(log: seq[tEntry], oid: int, v: int): int {
  var r: int;
  var i: int;
  r = -1;
  while (i < sizeof(log)) {
    if (log[i].oid == oid && log[i].ver <= v) {
      r = log[i].ver;
    }
    i = i + 1;
  }
  return r;
}

fun Msg(kind: tKind, src: int, dst: int, epSent: int, req: int): tMsg {
  var m: tMsg;
  m.kind = kind;
  m.src = src;
  m.dst = dst;
  m.epSent = epSent;
  m.req = req;
  m.info = EmptyInfo();
  m.pi = EmptyPI();
  return m;
}

fun Evt(e: tE): tEvt {
  var ev: tEvt;
  ev.e = e;
  return ev;
}

fun EvtMsg(e: tE, m: tMsg): tEvt {
  var ev: tEvt;
  ev.e = e;
  ev.m = m;
  return ev;
}

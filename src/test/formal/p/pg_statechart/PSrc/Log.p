/*
 * The PG log and missing set of one OSD (PGLog, pg_missing_t) and the object
 * data it describes. A replicated pool's entries cannot be rolled back.
 *
 * Objects above last_backfill (lb) are not tracked: backfill brings them.
 */

// an OSD's object data: the version and every write it holds
type tObjv = (ver: int, wids: set[int]);

type tStore = (
  log: seq[tEntry],
  tail: int,
  lu: int,                        // last_update (the log head)
  missing: map[int, tMiss],
  objs: map[int, tObjv],
  failed: string                  // a ceph_assert that failed, or ""
);

// pg_missing_set::add_next_event
fun AddNextEvent(ms: map[int, tMiss], e: tEntry): map[int, tMiss] {
  var it: tMiss;
  if (e.prior == 0) {
    ms[e.oid] = (need = e.ver, have = 0);
  } else if (e.oid in ms) {
    it = ms[e.oid];
    it.need = e.ver;              // leave have
    ms[e.oid] = it;
  } else {
    ms[e.oid] = (need = e.ver, have = e.prior);
  }
  return ms;
}

// pg_missing_set::revise_need
fun ReviseNeed(ms: map[int, tMiss], oid: int, need: int): map[int, tMiss] {
  var it: tMiss;
  if (oid in ms) {
    it = ms[oid];
    it.need = need;
    ms[oid] = it;
  } else {
    ms[oid] = (need = need, have = 0);
  }
  return ms;
}

fun NewestIn(log: seq[tEntry], oid: int): int {
  var r: int;
  var i: int;
  r = -1;
  while (i < sizeof(log)) {
    if (log[i].oid == oid) {
      r = log[i].ver;
    }
    i = i + 1;
  }
  return r;
}

// PGLog::_merge_object_divergent_entries for a replicated pool: `ents` are the
// divergent entries for one object, oldest first; st.log is the log they are
// merged against. rm: remove the object (the rollbacker; false for a
// peer whose missing set is all we adjust)
fun MergeObjectDivergent(st: tStore, lb: int, oid: int, ents: seq[tEntry], rm: bool): tStore {
  var prior: int;
  var newest: int;
  var it: tMiss;
  if (oid > lb || sizeof(ents) == 0) {
    return st;
  }
  prior = ents[0].prior;
  newest = NewestIn(st.log, oid);
  if (newest != -1 && newest >= ents[0].ver) {
    // a newer authoritative entry: its add_next_event made the object missing
    if (!(oid in st.missing) || st.missing[oid].need != newest) {
      st.failed = format("_merge_object_divergent_entries: object {0} not missing at {1}", oid, newest);
      return st;
    }
    it = st.missing[oid];
    it.have = 0;
    st.missing[oid] = it;
    if (rm) {
      st.objs -= (oid);
    }
    return st;
  }
  if (prior == 0) {
    // the divergent entries created the object
    if (oid in st.missing) {
      st.missing -= (oid);
    }
    if (rm) {
      st.objs -= (oid);
    }
    return st;
  }
  if (oid in st.missing) {
    if (st.missing[oid].have == prior) {
      st.missing -= (oid);
    } else {
      st.missing = ReviseNeed(st.missing, oid, prior);
    }
    return st;
  }
  // cannot roll back: remove it and fetch the prior version
  if (rm) {
    st.objs -= (oid);
  }
  st.missing[oid] = (need = prior, have = 0);
  return st;
}

// PGLog::_merge_divergent_entries
fun MergeDivergent(st: tStore, lb: int, div: seq[tEntry], rm: bool): tStore {
  var oids: set[int];
  var oid: int;
  var ents: seq[tEntry];
  var i: int;
  while (i < sizeof(div)) {
    oids += (div[i].oid);
    i = i + 1;
  }
  foreach (oid in oids) {
    ents = default(seq[tEntry]);
    i = 0;
    while (i < sizeof(div)) {
      if (div[i].oid == oid) {
        ents += (sizeof(ents), div[i]);
      }
      i = i + 1;
    }
    st = MergeObjectDivergent(st, lb, oid, ents, rm);
  }
  return st;
}

// PGLog::rewind_divergent_log
fun RewindDivergent(st: tStore, lb: int, head: int): tStore {
  var div: seq[tEntry];
  div = After(st.log, head);
  st.log = UpTo(st.log, head);
  st.lu = head;
  return MergeDivergent(st, lb, div, true);
}

// PGLog::merge_log: `olog` is authoritative, with tail otail and head ohead
fun MergeLog(st: tStore, lb: int, olog: seq[tEntry], otail: int, ohead: int): tStore {
  var cut: int;
  var i: int;
  var div: seq[tEntry];
  var added: seq[tEntry];
  if (sizeof(st.log) > 0 || st.lu > st.tail) {
    if (!(st.lu >= otail && ohead >= st.tail)) {
      st.failed = format("merge_log: logs do not overlap (ours ({0},{1}], theirs ({2},{3}])", st.tail, st.lu, otail, ohead);
      return st;
    }
  }
  // extend on tail: fill in older history
  if (otail < st.tail) {
    added = default(seq[tEntry]);
    i = 0;
    while (i < sizeof(olog)) {
      if (olog[i].ver <= st.tail) {
        added += (sizeof(added), olog[i]);
      }
      i = i + 1;
    }
    i = 0;
    while (i < sizeof(st.log)) {
      added += (sizeof(added), st.log[i]);
      i = i + 1;
    }
    st.log = added;
    st.tail = otail;
  }
  if (ohead < st.lu) {
    st = RewindDivergent(st, lb, ohead);
  }
  if (ohead > st.lu) {
    cut = otail;
    if (st.tail > cut) {
      cut = st.tail;
    }
    i = 0;
    while (i < sizeof(olog)) {
      if (olog[i].ver <= st.lu && olog[i].ver > cut) {
        cut = olog[i].ver;
      }
      i = i + 1;
    }
    div = After(st.log, cut);
    st.log = UpTo(st.log, cut);
    // append the new entries, updating missing (append_log_entries_update_missing)
    i = 0;
    while (i < sizeof(olog)) {
      if (olog[i].ver > cut) {
        st.log += (sizeof(st.log), olog[i]);
        if (olog[i].oid <= lb) {
          st.missing = AddNextEvent(st.missing, olog[i]);
        }
      }
      i = i + 1;
    }
    st = MergeDivergent(st, lb, div, true);
    st.lu = ohead;
  }
  return st;
}

// PGLog::proc_replica_log: the primary's view of a peer. Its last_update
// becomes the newest entry of ours at or before its head (or the larger
// tail); its entries after that are divergent and adjust its missing set.
fun ProcReplicaLogLu(log: seq[tEntry], tail: int, ohead: int, otail: int): int {
  var lu: int;
  var i: int;
  var limit: int;
  limit = otail;
  if (tail > limit) {
    limit = tail;
  }
  lu = -1;
  i = 0;
  while (i < sizeof(log)) {
    if (log[i].ver <= ohead) {
      lu = log[i].ver;
    }
    i = i + 1;
  }
  if (lu == -1 || lu < limit) {
    return limit;
  }
  return lu;
}

// last_complete: everything up to it is on disk. With missing objects, the
// entry just before the oldest one needed
fun LastComplete(log: seq[tEntry], tail: int, lu: int, ms: map[int, tMiss]): int {
  var oldest: int;
  var oid: int;
  var lc: int;
  var i: int;
  if (sizeof(ms) == 0) {
    return lu;
  }
  oldest = -1;
  foreach (oid in keys(ms)) {
    if (oldest == -1 || ms[oid].need < oldest) {
      oldest = ms[oid].need;
    }
  }
  lc = tail;
  while (i < sizeof(log)) {
    if (log[i].ver < oldest) {
      lc = log[i].ver;
    }
    i = i + 1;
  }
  return lc;
}

// PGLog::trim: drop entries up to trim_to
fun TrimLog(st: tStore, trimTo: int): tStore {
  if (trimTo > st.tail) {
    st.log = After(st.log, trimTo);
    st.tail = trimTo;
  }
  return st;
}

/*
 * PeeringState::calc_replicated_acting: the acting set, backfill targets and
 * acting_recovery_backfill. Prefer up, then acting, then any peer; a peer
 * that is incomplete or behind the oldest authoritative log entry cannot be
 * log-recovered: in up it becomes a backfill target, elsewhere it is passed
 * over.
 */
type tActing = (want: seq[int], backfill: set[int], arb: set[int]);

fun CalcReplicatedActing(prim: int, oldest: int, size: int, acting: seq[int], up: seq[int],
                         all: map[int, tInfo], n: int, restrict: bool): tActing {
  var r: tActing;
  var cands: seq[int];
  var i: int;
  var o: int;
  var ci: tInfo;
  r.want += (0, prim);
  r.arb += (prim);
  i = 0;
  while (i < sizeof(up)) {
    if (up[i] != prim) {
      ci = all[up[i]];
      if (Incomplete(ci) || ci.lu < oldest) {
        r.backfill += (up[i]);
        r.arb += (up[i]);
      } else {
        r.want += (sizeof(r.want), up[i]);
        r.arb += (up[i]);
      }
    }
    i = i + 1;
  }
  if (sizeof(r.want) >= size) {
    return r;
  }
  i = 0;
  while (i < sizeof(acting)) {
    o = acting[i];
    if (o != prim && !Contains(up, o)) {
      ci = all[o];
      if (!Incomplete(ci) && ci.lu >= oldest) {
        cands += (sizeof(cands), o);
      }
    }
    i = i + 1;
  }
  r = AppendByLastUpdate(r, cands, all, size);
  if (sizeof(r.want) >= size || restrict) {
    return r;
  }
  cands = default(seq[int]);
  o = 0;
  while (o < n) {
    if (o in all && o != prim && !Contains(up, o) && !Contains(acting, o)) {
      ci = all[o];
      if (!Incomplete(ci) && ci.lu >= oldest) {
        cands += (sizeof(cands), o);
      }
    }
    o = o + 1;
  }
  return AppendByLastUpdate(r, cands, all, size);
}

// candidates newest last_update first, until want has `size` members
fun AppendByLastUpdate(r: tActing, cands: seq[int], all: map[int, tInfo], size: int): tActing {
  var best: int;
  var i: int;
  while (sizeof(r.want) < size && sizeof(cands) > 0) {
    best = 0;
    i = 1;
    while (i < sizeof(cands)) {
      if (all[cands[i]].lu > all[cands[best]].lu) {
        best = i;
      }
      i = i + 1;
    }
    r.want += (sizeof(r.want), cands[best]);
    r.arb += (cands[best]);
    cands -= (best);
  }
  return r;
}

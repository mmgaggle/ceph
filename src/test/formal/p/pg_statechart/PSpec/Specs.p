// A write acked to the client is on every copy of its object once the PG
// is clean: the primary acks only after every OSD in
// acting_recovery_backfill has it, and recovery and backfill bring every
// other copy up to date before the PG is clean. That holds for a clean
// interval no older than the write's (a lagging primary may still go clean
// in an interval the cluster has left). An OSD marked lost may take writes
// acked before with it.
spec AckedWritesDurable observes mAcked, mObj, mDeleted, mClean, mLost {
  var acked: map[int, (oid: int, ep: int)];  // wid -> object, the acking primary's activation epoch
  var lost: set[int];
  var copies: map[int, map[int, tObjv]];     // osd -> object -> its copy

  start state Watch {
    on mLost do (o: int) {
      lost += (o);
      acked = default(map[int, (oid: int, ep: int)]);
    }
    on mAcked do (a: (wid: int, oid: int, ver: int, ep: int, osd: int)) {
      if (!(a.osd in lost)) {
        acked[a.wid] = (oid = a.oid, ep = a.ep);
      }
    }
    on mObj do (c: (osd: int, oid: int, obj: tObjv, valid: bool)) {
      var m: map[int, tObjv];
      if (c.osd in copies) {
        m = copies[c.osd];
      }
      if (c.valid) {
        m[c.oid] = c.obj;
      } else if (c.oid in m) {
        m -= (c.oid);
      }
      copies[c.osd] = m;
    }
    on mDeleted do (o: int) {
      if (o in copies) {
        copies -= (o);
      }
    }
    on mClean do (c: (osd: int, sis: int, acting: seq[int])) {
      var w: int;
      var i: int;
      var o: int;
      foreach (w in keys(acked)) {
        i = 0;
        while (acked[w].ep <= c.sis && i < sizeof(c.acting)) {
          o = c.acting[i];
          assert o in copies && acked[w].oid in copies[o] && w in copies[o][acked[w].oid].wids,
            format("the PG went clean in interval {0} (acting {1}) but osd.{2}'s copy of object {3} lacks write {4}, acked in epoch {5}",
                   c.sis, c.acting, o, acked[w].oid, w, acked[w].ep);
          i = i + 1;
        }
      }
    }
  }
}

// Once the environment settles, the PG goes active in its current interval,
// unless every OSD that ever had the PG has been marked lost.
spec PgGoesActive observes mMap, mActive, mSettled, mHolds, mLost {
  var sis: int;
  var activeSis: set[int];
  var holders: set[int];
  var lost: set[int];

  start cold state Unsettled {
    on mMap do (m: (epoch: int, sis: int)) { sis = m.sis; }
    on mActive do (a: (osd: int, sis: int)) { activeSis += (a.sis); }
    on mHolds do (o: int) { holders += (o); }
    on mLost do (o: int) { lost += (o); }
    on mSettled do { Check(); }
  }
  hot state Inactive {
    on mMap do (m: (epoch: int, sis: int)) { sis = m.sis; Check(); }
    on mActive do (a: (osd: int, sis: int)) { activeSis += (a.sis); Check(); }
    on mHolds do (o: int) { holders += (o); Check(); }
    on mLost do (o: int) { lost += (o); Check(); }
  }
  cold state Active {
    on mMap do (m: (epoch: int, sis: int)) { sis = m.sis; Check(); }
    on mActive do (a: (osd: int, sis: int)) { activeSis += (a.sis); Check(); }
    on mHolds do (o: int) { holders += (o); Check(); }
    on mLost do (o: int) { lost += (o); Check(); }
  }
  cold state AllLost {
    ignore mMap, mActive, mHolds, mLost;
  }

  fun Check() {
    var o: int;
    var gone: bool;
    gone = true;
    foreach (o in holders) {
      if (!(o in lost)) {
        gone = false;
      }
    }
    if (gone) {
      goto AllLost;
    } else if (sis in activeSis) {
      goto Active;
    } else {
      goto Inactive;
    }
  }
}

// Once the environment settles (no OSD lost, none full), the PG goes
// active+clean in its current interval: recovery and backfill finish.
spec PgGoesClean observes mMap, mClean, mSettled {
  var sis: int;
  var cleanSis: set[int];

  start cold state Unsettled {
    on mMap do (m: (epoch: int, sis: int)) { sis = m.sis; }
    on mClean do (c: (osd: int, sis: int, acting: seq[int])) { cleanSis += (c.sis); }
    on mSettled do { Check(); }
  }
  hot state NotClean {
    on mMap do (m: (epoch: int, sis: int)) { sis = m.sis; Check(); }
    on mClean do (c: (osd: int, sis: int, acting: seq[int])) { cleanSis += (c.sis); Check(); }
  }
  cold state Clean {
    on mMap do (m: (epoch: int, sis: int)) { sis = m.sis; Check(); }
    on mClean do (c: (osd: int, sis: int, acting: seq[int])) { cleanSis += (c.sis); Check(); }
  }

  fun Check() {
    if (sis in cleanSis) {
      goto Clean;
    } else {
      goto NotClean;
    }
  }
}

// Once the environment settles, every write is acked (unless every OSD
// that had the PG was marked lost).
spec WritesComplete observes mIssued, mAcked, mSettled, mHolds, mLost {
  var pending: set[int];
  var holders: set[int];
  var lost: set[int];

  start cold state Unsettled {
    on mIssued do (w: int) { pending += (w); }
    on mAcked do (a: (wid: int, oid: int, ver: int, ep: int, osd: int)) { pending -= (a.wid); }
    on mHolds do (o: int) { holders += (o); }
    on mLost do (o: int) { lost += (o); }
    on mSettled do { Check(); }
  }
  hot state Waiting {
    on mAcked do (a: (wid: int, oid: int, ver: int, ep: int, osd: int)) { pending -= (a.wid); Check(); }
    on mHolds do (o: int) { holders += (o); Check(); }
    on mLost do (o: int) { lost += (o); Check(); }
  }
  cold state Done {
    ignore mAcked, mHolds, mLost;
  }

  fun Check() {
    var o: int;
    var gone: bool;
    gone = true;
    foreach (o in holders) {
      if (!(o in lost)) {
        gone = false;
      }
    }
    if (gone || sizeof(pending) == 0) {
      goto Done;
    }
    goto Waiting;
  }
}

// Once the environment settles, the maps stop changing.
spec MapsSettle observes mMap, mSettled {
  var after: int;
  var settled: bool;

  start state Watch {
    on mSettled do { settled = true; }
    on mMap do (m: (epoch: int, sis: int)) {
      if (settled) {
        after = after + 1;
        assert after <= 40, format("{0} epochs since the cluster settled (now {1})", after, m.epoch);
      }
    }
  }
}

// Once the environment settles, the PG gives back every reservation it
// requested, local and remote, granted or queued: a leaked one holds a
// slot of osd_max_backfills for good, and the PG's next request for it
// trips AsyncReserver's duplicate-request assert.
spec ReservationsReleased observes mReserve, mSettled {
  var held: set[(osd: int, local: bool)];

  start cold state Unsettled {
    on mReserve do (r: (osd: int, pgOsd: int, local: bool, held: bool)) { Note(r.osd, r.local, r.held); }
    on mSettled do { Check(); }
  }
  hot state Held {
    on mReserve do (r: (osd: int, pgOsd: int, local: bool, held: bool)) { Note(r.osd, r.local, r.held); Check(); }
  }
  cold state Released {
    on mReserve do (r: (osd: int, pgOsd: int, local: bool, held: bool)) { Note(r.osd, r.local, r.held); Check(); }
  }

  fun Note(o: int, isLocal: bool, h: bool) {
    if (h) {
      held += ((osd = o, local = isLocal));
    } else {
      held -= ((osd = o, local = isLocal));
    }
  }

  fun Check() {
    if (sizeof(held) == 0) {
      goto Released;
    } else {
      goto Held;
    }
  }
}

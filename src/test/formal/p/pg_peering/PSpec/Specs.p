// A write acked to the client is in every authoritative log from then on:
// a write acked by a primary that activated in epoch A is in the log of
// every primary that activates a writeable acting set in an epoch after A,
// whether that happens before the ack (a stale primary acking) or after
// it. (A retried write can be acked from an entry an earlier interval
// wrote, so the entry's own epoch does not bound the ack.) An OSD marked
// lost may take writes acked before with it, and one marked lost while
// still running guarantees nothing it acks.
spec AckedWritesSurvive observes mAcked, mActivated, mLost {
  var acked: map[int, int];                  // wid -> the acking primary's activation epoch
  var acts: seq[(osd: int, epoch: int, wids: set[int])];
  var lost: set[int];

  start state Watch {
    on mLost do (o: int) {
      lost += (o);
      acked = default(map[int, int]);
    }
    on mAcked do (a: (wid: int, ep: int, osd: int)) {
      var i: int;
      if (a.wid in acked || a.osd in lost) {
        return;
      }
      acked[a.wid] = a.ep;
      while (i < sizeof(acts)) {
        assert acts[i].epoch <= a.ep || a.wid in acts[i].wids,
          format("write {0} acked in epoch {1}, but osd.{2} activated in epoch {3} without it",
                 a.wid, a.ep, acts[i].osd, acts[i].epoch);
        i = i + 1;
      }
    }
    on mActivated do (a: (osd: int, epoch: int, wids: set[int])) {
      var w: int;
      foreach (w in keys(acked)) {
        assert acked[w] >= a.epoch || w in a.wids,
          format("osd.{0} activated in epoch {1} without write {2}, acked in epoch {3}",
                 a.osd, a.epoch, w, acked[w]);
      }
      acts += (sizeof(acts), a);
    }
  }
}

// Once the environment settles, the PG goes active in its current interval.
// Unless every OSD that ever had the PG has been marked lost: then only
// `ceph osd force-create-pg` brings it back.
spec PgGoesActive observes mMap, mActive, mSettled, mHolds, mLost {
  var sis: int;
  var activeSis: set[int];         // intervals a primary went active in (a lagging one may be old)
  var settled: bool;
  var holders: set[int];
  var lost: set[int];

  start cold state Unsettled {
    on mMap do (m: (epoch: int, sis: int)) { sis = m.sis; }
    on mActive do (a: (osd: int, sis: int)) { activeSis += (a.sis); }
    on mHolds do (o: int) { holders += (o); }
    on mLost do (o: int) { lost += (o); }
    on mSettled do {
      settled = true;
      Check();
    }
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

// Once the environment settles, every write is acked (unless every OSD
// that had the PG was marked lost).
spec WritesComplete observes mIssued, mAcked, mSettled, mHolds, mLost {
  var pending: set[int];
  var holders: set[int];
  var lost: set[int];

  start cold state Unsettled {
    on mIssued do (w: int) { pending += (w); }
    on mAcked do (a: (wid: int, ep: int, osd: int)) { pending -= (a.wid); }
    on mHolds do (o: int) { holders += (o); }
    on mLost do (o: int) { lost += (o); }
    on mSettled do {
      Check();
    }
  }
  hot state Waiting {
    on mAcked do (a: (wid: int, ep: int, osd: int)) { pending -= (a.wid); Check(); }
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

// Once the environment settles, the maps stop changing: a bounded number of
// epochs (boots, up_thru, pg_temp) and no churn.
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

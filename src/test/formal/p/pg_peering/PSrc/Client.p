/*
 * The client (Objecter): sends each write to the acting primary of its
 * newest map, and resends every unacked write whenever the PG's interval
 * changes in its map (Objecter::_calc_target). Duplicates are harmless:
 * the primary acks a write already in its log (the reqid dup check).
 */
machine Client {
  var osds: map[int, machine];
  var env: machine;
  var m: tMap;
  var have: bool;
  var pending: set[int];

  start state Init {
    on eSetup do (s: (mon: machine, osds: map[int, machine], client: machine, env: machine)) {
      osds = s.osds;
      env = s.env;
      goto Run;
    }
    defer eIssueWrite, eMaps;
  }

  state Run {
    on eMaps do (ms: seq[tMap]) {
      var w: int;
      var i: int;
      var changed: bool;
      i = 0;
      while (i < sizeof(ms)) {
        if (!have || ms[i].epoch > m.epoch) {
          // ops wait for the first map
          if (!have || NewInterval(m, ms[i])) {
            changed = true;
          }
          m = ms[i];
          have = true;
        }
        i = i + 1;
      }
      if (changed) {
        foreach (w in pending) {
          Send(w);
        }
      }
    }
    on eIssueWrite do (w: int) {
      pending += (w);
      Send(w);
    }
    on eWriteAck do (w: int) {
      if (w in pending) {
        pending -= (w);
        send env, eNoteAcked, w;
      }
    }
  }

  fun Send(w: int) {
    var p: int;
    if (!have) {
      return;
    }
    p = PrimaryOf(ActingSet(m));
    if (p >= 0) {
      send osds[p], eWrite, (wid = w, epoch = m.epoch);
    }
  }
}

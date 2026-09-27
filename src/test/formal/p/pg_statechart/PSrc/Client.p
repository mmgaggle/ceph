/*
 * The client (Objecter): each write goes to one of the PG's objects. It is
 * sent to the acting primary of the newest map, and resent on every
 * interval change until acked; the primary acks a write already in its log
 * (the reqid dup check).
 */
machine Client {
  var cfg: tCfg;
  var osds: map[int, machine];
  var env: machine;
  var m: tMap;
  var have: bool;
  var pending: map[int, int];     // wid -> object

  start state Init {
    entry (c: tCfg) {
      cfg = c;
    }
    on eSetup do (s: (mon: machine, osds: map[int, machine], client: machine, env: machine)) {
      osds = s.osds;
      env = s.env;
      goto Run;
    }
    defer eIssueWrite, eMaps;
  }

  state Run {
    on eMaps do (ms: seq[tMap]) {
      var i: int;
      var w: int;
      var changed: bool;
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
        foreach (w in keys(pending)) {
          Send(w);
        }
      }
    }
    on eIssueWrite do (w: int) {
      pending[w] = 1 + choose(cfg.nObjects);
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
      send osds[p], eWrite, (wid = w, oid = pending[w], epoch = m.epoch);
    }
  }
}

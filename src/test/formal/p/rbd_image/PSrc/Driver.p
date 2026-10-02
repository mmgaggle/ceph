/*
 * The clients: one per script, all opened at once; each runs its actions
 * in order. Once every action is answered, the specs see the final state.
 */
machine Driver {
  var store: machine;
  var total: int;
  var done: int;
  var clients: map[int, machine];
  var dead: set[int];
  var draining: set[int];

  start state Run {
    entry (p: (cfg: tCfg, init: tInit, scripts: seq[tScript])) {
      var i: int;
      var s: tScript;
      store = new Store((cfg = p.cfg, init = p.init));
      i = 1;
      foreach (s in p.scripts) {
        clients[i] = new Client((cfg = p.cfg, id = i, store = store, driver = this, script = s));
        total = total + sizeof(s.actions);
        i = i + 1;
      }
      if (total == 0) {
        Drain();
      }
    }

    on eActionDone do (d: (client: int, rc: tRc)) {
      done = done + 1;
      if (done == total) {
        Drain();
      }
    }

    on eCrashed do (c: (client: int, remaining: int)) {
      total = total - c.remaining;
      dead += (c.client);
      draining -= (c.client);
      if (done == total) {
        Drain();
      }
      if (sizeof(draining) == 0 && done == total) {
        send store, eQuiesce, this;
      }
    }

    on eDrained do (c: int) {
      draining -= (c);
      if (sizeof(draining) == 0) {
        send store, eQuiesce, this;
      }
    }

    ignore eQuiesced;
  }

  // every action is answered: wait until no live client has anything in
  // flight, then let the specs see the final state
  fun Drain() {
    var c: int;
    foreach (c in keys(clients)) {
      if (!(c in dead)) {
        draining += (c);
        send clients[c], eDrain, this;
      }
    }
    if (sizeof(draining) == 0) {
      send store, eQuiesce, this;
    }
  }
}

/*
 * The clients: one per script, all opened at once; each runs its actions
 * in order. Once every action is answered, the specs see the final state.
 */
machine Driver {
  var store: machine;
  var total: int;
  var done: int;

  start state Run {
    entry (p: (cfg: tCfg, init: tInit, scripts: seq[tScript])) {
      var i: int;
      var s: tScript;
      store = new Store((cfg = p.cfg, init = p.init));
      i = 1;
      foreach (s in p.scripts) {
        new Client((cfg = p.cfg, id = i, store = store, driver = this, script = s));
        total = total + sizeof(s.actions);
        i = i + 1;
      }
      if (total == 0) {
        send store, eQuiesce, this;
      }
    }

    on eActionDone do (d: (client: int, rc: tRc)) {
      done = done + 1;
      if (done == total) {
        send store, eQuiesce, this;
      }
    }

    ignore eQuiesced;
  }
}

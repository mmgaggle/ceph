/*
 * The monitor (OSDMonitor): owns the OSDMap history and makes each change
 * a new epoch.
 *
 * - boot (prepare_boot): an OSD the map still shows up is first marked
 *   down; the booted OSD's up_from is the new epoch, its up_thru unchanged.
 * - MOSDAlive (preprocess_alive, prepare_alive): ignored from an OSD that
 *   is down or from an old incarnation; otherwise up_thru becomes the map
 *   epoch the OSD had when it asked.
 * - MOSDPGTemp (preprocess_pgtemp, prepare_pgtemp): ignored unless the
 *   sender is the acting primary now; also sets the sender's up_thru to
 *   the epoch it asked in.
 * - Every new map drops a pg_temp that is redundant or all down
 *   (OSDMap::clean_temps).
 * - The environment marks OSDs down, out and lost, and changes min_size.
 *
 * Maps go to every OSD and the client in order. With batchMaps the monitor
 * may hold a subscriber's maps back and deliver several at once, as when an
 * OSD falls behind.
 */
machine Mon {
  var cfg: tCfg;
  var osds: map[int, machine];
  var client: machine;
  var maps: seq[tMap];            // maps[e] is epoch e; maps[0] is unused
  var sentTo: map[machine, int];    // newest epoch sent to each subscriber
  var sis: int;                   // the PG's interval start as of the newest epoch
  var flushQueued: bool;
  var deadAddr: map[int, int];    // addresses up to this one belong to dead processes
  var env: machine;
  var held: set[machine];         // subscribers whose maps are held back (scripted runs)

  start state Init {
    entry (c: tCfg) {
      var m: tMap;
      var o: int;
      cfg = c;
      m = (epoch = 1, crush = c.crush, up = default(map[int, bool]), inn = default(map[int, bool]),
           upFrom = default(map[int, int]), addr = default(map[int, int]), upThru = default(map[int, int]),
           downAt = default(map[int, int]), lostAt = default(map[int, int]),
           pgTemp = default(seq[int]), size = c.size, minSize = c.minSize);
      while (o < c.nOsds) {
        m.up[o] = true;
        m.inn[o] = true;
        m.upFrom[o] = 1;
        m.addr[o] = 1;
        m.upThru[o] = 1;
        m.downAt[o] = 0;
        m.lostAt[o] = 0;
        o = o + 1;
      }
      maps += (0, m);
      maps += (1, m);
      sis = 1;
      announce mMap, (epoch = 1, sis = 1);
    }
    on eSetup do (s: (mon: machine, osds: map[int, machine], client: machine, env: machine)) {
      var o: int;
      osds = s.osds;
      client = s.client;
      env = s.env;
      foreach (o in keys(osds)) {
        sentTo[osds[o]] = 0;
      }
      sentTo[client] = 0;
      FlushAll();
      goto Serve;
    }
    defer eBoot, eAlive, ePgTemp, eDied, eEnvDown, eEnvOut, eEnvLost, eEnvMinSize, eStep, eHoldMaps;
  }

  state Serve {
    on eBoot do (b: (osd: int, have: int, addr: int)) {
      var m: tMap;
      if (sentTo[osds[b.osd]] > b.have) {
        sentTo[osds[b.osd]] = b.have;
      }
      m = Latest();
      if (m.up[b.osd]) {
        // still up from before it crashed: down first
        m.up[b.osd] = false;
        m.downAt[b.osd] = Next();
        Publish(m);
        m = Latest();
      }
      m.up[b.osd] = true;
      m.upFrom[b.osd] = Next();
      m.addr[b.osd] = b.addr;
      Publish(m);
      send env, eNoteUp, b.osd;
      // C_Booted: the reply carries the maps up to the one that marks it up
      Flush(osds[b.osd]);
      if (b.osd in deadAddr && b.addr <= deadAddr[b.osd]) {
        MarkDown(b.osd);
      }
    }
    on eDied do (d: (osd: int, addr: int)) {
      var m: tMap;
      deadAddr[d.osd] = d.addr;
      m = Latest();
      if (m.up[d.osd] && m.addr[d.osd] <= d.addr) {
        MarkDown(d.osd);
      }
    }
    on eAlive do (a: (osd: int, addr: int, want: int, version: int)) {
      var m: tMap;
      m = Latest();
      if (!m.up[a.osd] || m.addr[a.osd] != a.addr) {
        return;
      }
      if (m.upThru[a.osd] >= a.want) {
        Flush(osds[a.osd]);
        return;
      }
      if (a.version > m.upThru[a.osd]) {
        m.upThru[a.osd] = a.version;
      }
      Publish(m);
    }
    on ePgTemp do (t: (osd: int, addr: int, want: seq[int], epoch: int)) {
      var m: tMap;
      m = Latest();
      if (!m.up[t.osd] || m.addr[t.osd] != t.addr) {
        return;
      }
      if (PrimaryOf(ActingSet(m)) != t.osd) {
        return;
      }
      if (sizeof(t.want) == 0 && sizeof(m.pgTemp) == 0) {
        Flush(osds[t.osd]);
        return;
      }
      if (sizeof(t.want) > 0 && sizeof(m.pgTemp) > 0 && ActingSet(m) == t.want) {
        Flush(osds[t.osd]);
        return;
      }
      m.pgTemp = t.want;
      if (t.epoch > m.upThru[t.osd]) {
        m.upThru[t.osd] = t.epoch;
      }
      Publish(m);
    }
    on eEnvDown do (s: set[int]) {
      var o: int;
      var m: tMap;
      var changed: bool;
      m = Latest();
      foreach (o in s) {
        if (m.up[o]) {
          m.up[o] = false;
          m.downAt[o] = Next();
          changed = true;
        }
      }
      if (changed) {
        Publish(m);
      }
    }
    on eEnvOut do (x: (osd: int, out: bool)) {
      var m: tMap;
      m = Latest();
      m.inn[x.osd] = !x.out;
      Publish(m);
    }
    on eEnvLost do (o: int) {
      var m: tMap;
      m = Latest();
      // "osd.N is not down"
      if (m.up[o]) {
        return;
      }
      m.lostAt[o] = m.downAt[o];
      announce mLost, o;
      Publish(m);
    }
    on eEnvMinSize do (k: int) {
      var m: tMap;
      m = Latest();
      m.minSize = k;
      Publish(m);
    }
    on eStep do {
      flushQueued = false;
      FlushAll();
    }
    on eHoldMaps do (h: (osd: int, hold: bool)) {
      if (h.hold) {
        held += (osds[h.osd]);
      } else {
        held -= (osds[h.osd]);
        Flush(osds[h.osd]);
      }
    }
  }

  fun MarkDown(o: int) {
    var m: tMap;
    m = Latest();
    m.up[o] = false;
    m.downAt[o] = Next();
    Publish(m);
  }

  fun Latest(): tMap {
    return maps[sizeof(maps) - 1];
  }

  fun Next(): int {
    return sizeof(maps);
  }

  fun Publish(m: tMap) {
    var s: machine;
    var last: tMap;
    var up: seq[int];
    last = Latest();
    m.epoch = Next();
    // OSDMap::clean_temps
    if (sizeof(m.pgTemp) > 0) {
      up = UpSet(m);
      if (sizeof(ActingSetOfTemp(m)) == 0 || m.pgTemp == up || sizeof(m.pgTemp) > m.size) {
        m.pgTemp = default(seq[int]);
      }
    }
    maps += (m.epoch, m);
    if (NewInterval(last, m)) {
      sis = m.epoch;
    }
    announce mMap, (epoch = m.epoch, sis = sis);
    if (!cfg.batchMaps) {
      FlushAll();
      return;
    }
    foreach (s in keys(sentTo)) {
      if ($) {
        Flush(s);
      }
    }
    if (!flushQueued) {
      flushQueued = true;
      send this, eStep;
    }
  }

  // pg_temp members that are up
  fun ActingSetOfTemp(m: tMap): seq[int] {
    var r: seq[int];
    var i: int;
    while (i < sizeof(m.pgTemp)) {
      if (m.up[m.pgTemp[i]]) {
        r += (sizeof(r), m.pgTemp[i]);
      }
      i = i + 1;
    }
    return r;
  }

  fun Flush(s: machine) {
    var ms: seq[tMap];
    var e: int;
    if (s in held) {
      return;
    }
    e = sentTo[s] + 1;
    while (e < sizeof(maps)) {
      ms += (sizeof(ms), maps[e]);
      e = e + 1;
    }
    if (sizeof(ms) > 0) {
      sentTo[s] = sizeof(maps) - 1;
      send s, eMaps, ms;
    }
  }

  fun FlushAll() {
    var s: machine;
    foreach (s in keys(sentTo)) {
      Flush(s);
    }
  }
}

/*
 * The file data, reduced to one integer version.
 *
 * Store is the oracle that assigns globally ordered versions to writes.
 * It stands in for the OSDs: a sync write lands immediately, a buffered
 * write takes a ticket when the application's write completes and lands
 * later when the client flushes it. The last landed write wins, exactly as
 * an object overwrite does.
 *
 * The store remembers which tickets have not landed. When a client is
 * blocklisted, those versions can never land, so the store reports them lost
 * at that moment, which is when the data is lost in the real system.
 */

type tStoreWrite = (from: machine, ver: int);
type tBlocklist = (from: machine, client: machine);

/* ver == 0: sync write, assign and store a new version; else: flush of a buffered write */
event eStoreWrite     : tStoreWrite;
event eStoreWriteAck  : int;
event eStoreRead      : machine;
event eStoreReadResp  : int;
/* take the next version number without writing (buffered write completion) */
event eStoreTicket     : machine;
event eStoreTicketResp : int;
/* the OSDs reject I/O from a blocklisted client */
event eStoreWriteRejected : int;
event eStoreReadRejected;
/* the MDS blocklists a client and waits until the OSDs apply it */
event eBlocklist    : tBlocklist;
event eBlocklistAck;

machine Store {
  var content: int;
  var counter: int;
  var blocklist: set[machine];
  var unlanded: map[machine, set[int]];   // tickets handed out that have not landed

  start state Serving {
    on eBlocklist do (b: tBlocklist) {
      var v: int;
      blocklist += (b.client);
      if (b.client in unlanded) {
        foreach (v in unlanded[b.client]) {
          announce eWriteLost, v;
        }
        unlanded -= (b.client);
      }
      send b.from, eBlocklistAck;
    }
    on eStoreWrite do (w: tStoreWrite) {
      var v: int;
      if (w.from in blocklist) {
        send w.from, eStoreWriteRejected, w.ver;
        return;
      }
      if (w.ver == 0) {
        counter = counter + 1;
        v = counter;
      } else {
        v = w.ver;
        if ((w.from in unlanded) && (v in unlanded[w.from])) {
          unlanded[w.from] -= (v);
        }
      }
      content = v;
      send w.from, eStoreWriteAck, v;
    }
    on eStoreRead do (m: machine) {
      if (m in blocklist) {
        send m, eStoreReadRejected;
        return;
      }
      send m, eStoreReadResp, content;
    }
    on eStoreTicket do (m: machine) {
      var s: set[int];
      counter = counter + 1;
      if (m in blocklist) {
        // the data enters a cache that will be purged
        announce eWriteLost, counter;
      } else {
        if (m in unlanded) { s = unlanded[m]; }
        s += (counter);
        unlanded[m] = s;
      }
      send m, eStoreTicketResp, counter;
    }
  }
}

/*
 * The file data, reduced to one integer version.
 *
 * Store is the oracle that assigns globally ordered versions to writes.
 * It stands in for the OSDs: a sync write lands immediately, a buffered
 * write takes a ticket when the application's write completes and lands
 * later when the client flushes it. The last landed write wins, exactly as
 * an object overwrite does.
 */

type tStoreWrite = (from: machine, ver: int);

/* ver == 0: sync write, assign and store a new version; else: flush of a buffered write */
event eStoreWrite     : tStoreWrite;
event eStoreWriteAck  : int;
event eStoreRead      : machine;
event eStoreReadResp  : int;
/* take the next version number without writing (buffered write completion) */
event eStoreTicket     : machine;
event eStoreTicketResp : int;

machine Store {
  var content: int;
  var counter: int;

  start state Serving {
    on eStoreWrite do (w: tStoreWrite) {
      var v: int;
      if (w.ver == 0) {
        counter = counter + 1;
        v = counter;
      } else {
        v = w.ver;
      }
      content = v;
      send w.from, eStoreWriteAck, v;
    }
    on eStoreRead do (m: machine) {
      send m, eStoreReadResp, content;
    }
    on eStoreTicket do (m: machine) {
      counter = counter + 1;
      send m, eStoreTicketResp, counter;
    }
  }
}

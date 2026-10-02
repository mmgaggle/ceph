/*
 * The RADOS state. Each handler is one atomic op on one object, or one
 * monitor command.
 *
 * - Each image's header object (rbd_header.<id>): its cls_lock entry
 *   (lock_obj, unlock, break_lock, set_cookie, get_info, cls_lock.cc),
 *   its watchers (a watch handle per client), its snapshots as cls_rbd
 *   keeps them (snapshot_add, snapshot_remove, snapshot_trash_add,
 *   snapshot_get, set_protection_status, child_attach, child_detach,
 *   children_list, cls_rbd.cc), its parent (set_parent, remove_parent),
 *   and the snap_seq. A refresh reads the header in one op
 *   (get_mutable_metadata).
 * - The rbd_children object of clone v1 (add_child, remove_child,
 *   get_children): one pool.
 * - The monitor: the pool's self-managed snapshot ids
 *   (selfmanaged_snap_create, selfmanaged_snap_remove), and the blocklist
 *   (osd blocklist add). Blocklisting a client removes its watches
 *   (check_blocklisted_watchers) and fails its later OSD ops with
 *   -EBLOCKLISTED (PrimaryLogPG::do_op). A break's wait for the latest
 *   OSD map is folded into the command: every OSD fences at once.
 * - The val objects, written as PrimaryLogPG::make_writeable writes
 *   them: the op's snap context, filtered of removed snapshots
 *   (filter_snapc), clones the object when its newest snapshot is newer
 *   than the object's snapset.sq; an older snap context is applied with
 *   no clone, as librbd never sets ORDERSNAP. A removed snapshot's clones
 *   are trimmed. A read at a snapshot answers the clone that covers it,
 *   the head if the object was not yet cloned past it, or -ENOENT.
 * - Notifies: a notify goes to every watcher of the header, and completes
 *   once each has acked or lost its watch (a lost watcher is a missing
 *   ack, as the OSD's notify timeout treats it).
 * - The OSD may drop a live client's watch (a watch timeout under load):
 *   the client learns of it when its next watch check fails
 *   (ENOTCONN), and re-watches.
 */
// how many store ops a watch drop picks its op from
fun DROP_STEPS(): int { return 60; }

machine Store {
  var cfg: tCfg;
  var hdrs: map[int, tHeader];
  var watchers: map[int, map[int, int]];   // image -> client -> handle
  var nextHandle: int;
  var blocklist: set[int];
  var rbdChildren: map[tParent, set[int]];
  var poolSnaps: set[int];
  var poolSeq: int;
  var objs: map[int, map[int, tObj]];      // image -> obj -> object
  var clients: map[int, machine];
  // pending notifies: who waits, which watchers still owe an ack, the acks
  var nFrom: map[int, machine];
  var nTag: map[int, int];
  var nWaiting: map[int, set[int]];
  var nAcks: map[int, seq[tAck]];
  var nextNid: int;
  var drops: int;
  var dropAt: int;
  var dropStep: int;

  start state Serve {
    entry (p: (cfg: tCfg, init: tInit)) {
      var o: int;
      var h: tHeader;
      var s: tSnap;
      var os: map[int, tObj];
      cfg = p.cfg;
      drops = cfg.watchDrops;
      if (drops > 0) {
        dropAt = 1 + choose(DROP_STEPS());
      }
      // image 1, with each object written once (val 1)
      h = default(tHeader);
      h.present = true;
      os = default(map[int, tObj]);
      o = 1;
      while (o <= NOBJ()) {
        os[o] = (present = true, head = 1, sq = 0, clones = default(map[int, tClone]));
        o = o + 1;
      }
      // snapshot 1 (name 1), if the scenario starts with one
      if (p.init.snap) {
        poolSeq = 1;
        poolSnaps += (1);
        s = default(tSnap);
        s.name = 1;
        if (p.init.protected) {
          s.protection = PROTECTED();
        }
        h.snaps[1] = s;
        h.snapSeq = 1;
      }
      hdrs[1] = h;
      objs[1] = os;
      watchers[1] = default(map[int, int]);
      announce mImage, (image = 1, present = true);
      // image 2 cloned from it already, if the scenario says so
      if (p.init.children) {
        CreateImage(2);
        hdrs[2].parent = (image = 1, snap = 1);
        announce mParentSet, (child = 2, parent = (image = 1, snap = 1));
        announce mCloneDone, 2;
        if (cfg.cloneV2) {
          hdrs[1].snaps[1].children += (2);
          hdrs[1].snaps[1].childCount = 1;
        } else {
          rbdChildren[(image = 1, snap = 1)] = default(set[int]);
          rbdChildren[(image = 1, snap = 1)] += (2);
        }
      }
    }

    on eOp do (p: (from: machine, client: int, op: tOp)) {
      var op: tOp;
      var r: tRes;
      var c: int;
      op = p.op;
      clients[p.client] = p.from;
      MaybeDropWatch();
      r = default(tRes);
      r.tag = op.tag;
      r.rc = OK;
      if (op.kind == OP_BLOCKLIST) {
        // a monitor command: works for anyone, blocklisted or not
        Blocklist(op.owner);
      } else if (op.kind == OP_SNAP_ALLOC) {
        poolSeq = poolSeq + 1;
        poolSnaps += (poolSeq);
        r.n = poolSeq;
      } else if (op.kind == OP_SNAP_RELEASE) {
        if (op.snap in poolSnaps) {
          poolSnaps -= (op.snap);
          TrimSnap(op.snap);
        } else {
          r.rc = ENOENT;
        }
      } else if (p.client in blocklist) {
        r.rc = EBLOCKLISTED;
      } else if (op.kind == OP_WRITE || op.kind == OP_COPYUP) {
        r = DataWrite(p.client, op, r);
      } else if (op.kind == OP_READ) {
        r = DataRead(op, r);
      } else if (op.kind == OP_ADD_CHILD) {
        if (!(op.parent in rbdChildren)) {
          rbdChildren[op.parent] = default(set[int]);
        }
        if (op.child in rbdChildren[op.parent]) {
          r.rc = EEXIST;
        } else {
          rbdChildren[op.parent] += (op.child);
        }
      } else if (op.kind == OP_REMOVE_CHILD) {
        if (!(op.parent in rbdChildren) || !(op.child in rbdChildren[op.parent])) {
          r.rc = ENOENT;
        } else {
          rbdChildren[op.parent] -= (op.child);
          if (sizeof(rbdChildren[op.parent]) == 0) {
            rbdChildren -= (op.parent);
          }
        }
      } else if (op.kind == OP_GET_CHILDREN) {
        if (!(op.parent in rbdChildren)) {
          r.rc = ENOENT;
        } else {
          r.children = rbdChildren[op.parent];
        }
      } else if (op.kind == OP_CREATE_IMAGE) {
        if (op.image in hdrs && hdrs[op.image].present) {
          r.rc = EEXIST;
        } else {
          CreateImage(op.image);
        }
      } else if (!(op.image in hdrs) || !hdrs[op.image].present) {
        r.rc = ENOENT;
      } else {
        r = HeaderOp(p.client, op, r);
      }
      send p.from, eRes, r;
    }

    // a notify on an image's header: delivered to every watcher
    on eNotifyReq do (p: (from: machine, client: int, image: int, n: tNotify, tag: int)) {
      var nid: int;
      var c: int;
      var acks: seq[tAck];
      MaybeDropWatch();
      if (p.client in blocklist) {
        send p.from, eNotified, (tag = p.tag, acks = acks, rc = EBLOCKLISTED);
        return;
      }
      if (!(p.image in hdrs) || !hdrs[p.image].present || sizeof(watchers[p.image]) == 0) {
        send p.from, eNotified, (tag = p.tag, acks = acks, rc = OK);
        return;
      }
      nextNid = nextNid + 1;
      nid = nextNid;
      nFrom[nid] = p.from;
      nTag[nid] = p.tag;
      nWaiting[nid] = default(set[int]);
      nAcks[nid] = acks;
      foreach (c in keys(watchers[p.image])) {
        nWaiting[nid] += (c);
      }
      foreach (c in keys(watchers[p.image])) {
        send clients[c], eNotify, (nid = nid, image = p.image, n = p.n);
      }
    }

    on eAck do (a: (nid: int, client: int, ack: tAck)) {
      if (!(a.nid in nWaiting) || !(a.client in nWaiting[a.nid])) {
        return;  // the watcher's watch was lost first, or the notify already completed
      }
      nAcks[a.nid] += (sizeof(nAcks[a.nid]), a.ack);
      nWaiting[a.nid] -= (a.client);
      FinishNotifyIfDone(a.nid);
    }

    // a client died: its watches lapse (the OSD's watch timeout)
    on eCrash do (c: int) {
      var image: int;
      foreach (image in keys(watchers)) {
        if (c in watchers[image]) {
          DropWatch(image, c);
        }
      }
      clients -= (c);
    }

    on eQuiesce do (from: machine) {
      var hs: set[int];
      var dangling: set[tParent];
      var i: int;
      var s: int;
      foreach (i in keys(hdrs)) {
        if (hdrs[i].present) {
          foreach (s in keys(hdrs[i].snaps)) {
            hs += (s);
          }
          if (hdrs[i].parent.image != 0 && !SnapExists(hdrs[i].parent)) {
            dangling += ((image = i, snap = 0));
          }
          foreach (s in keys(hdrs[i].snaps)) {
            if (hdrs[i].snaps[s].parent.image != 0 && !SnapExists(hdrs[i].snaps[s].parent)) {
              dangling += ((image = i, snap = s));
            }
          }
        }
      }
      announce mFinal, (poolSnaps = poolSnaps, headerSnaps = hs, dangling = dangling);
      send from, eQuiesced;
    }
  }

  fun CreateImage(image: int) {
    var h: tHeader;
    h = default(tHeader);
    h.present = true;
    hdrs[image] = h;
    objs[image] = default(map[int, tObj]);
    watchers[image] = default(map[int, int]);
    announce mImage, (image = image, present = true);
  }

  // the OSD drops a live client's watch: a watch timeout. The op to
  // drop it at is chosen up front, so that every op is as likely.
  fun MaybeDropWatch() {
    var image: int;
    var c: int;
    var victims: seq[(image: int, client: int)];
    var v: (image: int, client: int);
    if (drops == 0) {
      return;
    }
    dropStep = dropStep + 1;
    if (dropStep != dropAt) {
      return;
    }
    foreach (image in keys(watchers)) {
      foreach (c in keys(watchers[image])) {
        victims += (sizeof(victims), (image = image, client = c));
      }
    }
    if (sizeof(victims) == 0) {
      dropAt = dropAt + 1;   // nobody to drop yet: the next op
      return;
    }
    drops = drops - 1;
    dropAt = dropStep + 1 + choose(DROP_STEPS());
    v = choose(victims);
    DropWatch(v.image, v.client);
    send clients[v.client], eWatchError, (image = v.image, rc = ENOENT);
  }

  fun DropWatch(image: int, c: int) {
    var nid: int;
    watchers[image] -= (c);
    // a pending notify no longer waits for it: a missing ack
    foreach (nid in keys(nWaiting)) {
      if (c in nWaiting[nid]) {
        nWaiting[nid] -= (c);
        FinishNotifyIfDone(nid);
      }
    }
  }

  fun FinishNotifyIfDone(nid: int) {
    if (sizeof(nWaiting[nid]) == 0) {
      send nFrom[nid], eNotified, (tag = nTag[nid], acks = nAcks[nid], rc = OK);
      nFrom -= (nid);
      nTag -= (nid);
      nWaiting -= (nid);
      nAcks -= (nid);
    }
  }

  fun Blocklist(c: int) {
    var image: int;
    blocklist += (c);
    foreach (image in keys(watchers)) {
      if (c in watchers[image]) {
        DropWatch(image, c);
        if (c in clients) {
          send clients[c], eWatchError, (image = image, rc = EBLOCKLISTED);
        }
      }
    }
  }

  // an op on image's header object
  fun HeaderOp(c: int, op: tOp, r: tRes): tRes {
    var h: tHeader;
    var s: tSnap;
    var k: int;
    var refs: set[tParent];
    h = hdrs[op.image];
    if (op.kind == OP_WATCH) {
      nextHandle = nextHandle + 1;
      watchers[op.image][c] = nextHandle;
      r.n = nextHandle;
    } else if (op.kind == OP_UNWATCH) {
      if (c in watchers[op.image] && watchers[op.image][c] == op.cookie) {
        DropWatch(op.image, c);
      }
    } else if (op.kind == OP_LIST_WATCHERS) {
      r.watchers = watchers[op.image];
    } else if (op.kind == OP_LOCK) {
      // lock_obj: an exclusive lock with no duration; the same holder and
      // cookie again is -EEXIST (no MAY_RENEW)
      if (h.lock.held && h.lock.owner == c && h.lock.cookie == op.cookie) {
        r.rc = EEXIST;
      } else if (h.lock.held) {
        r.rc = EBUSY;
      } else {
        h.lock = (held = true, owner = c, cookie = op.cookie);
      }
    } else if (op.kind == OP_GET_LOCK_INFO) {
      r.lock = h.lock;
    } else if (op.kind == OP_BREAK_LOCK) {
      // break_lock: removes the named holder's entry; no liveness check
      if (h.lock.held && h.lock.owner == op.owner && h.lock.cookie == op.cookie) {
        h.lock = default(tLock);
      } else {
        r.rc = ENOENT;
      }
    } else if (op.kind == OP_SET_COOKIE) {
      if (h.lock.held && h.lock.owner == c && h.lock.cookie == op.cookie) {
        h.lock.cookie = op.cookie2;
      } else {
        r.rc = EBUSY;
      }
    } else if (op.kind == OP_UNLOCK) {
      if (h.lock.held && h.lock.owner == c && h.lock.cookie == op.cookie) {
        h.lock = default(tLock);
      } else {
        r.rc = ENOENT;
      }
    } else if (op.kind == OP_READ_HEADER) {
      r.hdr = h;
    } else if (op.kind == OP_SNAP_ADD) {
      if (op.snap < h.snapSeq) {
        r.rc = ESTALE;
      } else if (op.snap in h.snaps || NameInUse(h, op.name)) {
        r.rc = EEXIST;
      } else {
        s = default(tSnap);
        s.name = op.name;
        s.parent = h.parent;   // the snapshot records the parent of the moment
        h.snaps[op.snap] = s;
        h.snapSeq = op.snap;
        announce mSnapCreated, (image = op.image, snap = op.snap, view = Views(op.image, op.snap));
      }
    } else if (op.kind == OP_SNAP_REMOVE) {
      if (!(op.snap in h.snaps)) {
        r.rc = ENOENT;
      } else if (h.snaps[op.snap].protection != UNPROTECTED()) {
        r.rc = EBUSY;
      } else if (h.snaps[op.snap].childCount > 0) {
        r.rc = EBUSY;
      } else {
        h.snaps -= (op.snap);
        hdrs[op.image] = h;
        announce mSnapRemoved, (image = op.image, snap = op.snap,
                                refs = ParentRefs((image = op.image, snap = op.snap)));
      }
    } else if (op.kind == OP_SNAP_TRASH_ADD) {
      if (!(op.snap in h.snaps)) {
        r.rc = ENOENT;
      } else if (h.snaps[op.snap].protection != UNPROTECTED()) {
        r.rc = EBUSY;
      } else if (h.snaps[op.snap].trash) {
        r.rc = EEXIST;
      } else {
        h.snaps[op.snap].trash = true;
        announce mSnapRemoveStarted, (image = op.image, snap = op.snap);
      }
    } else if (op.kind == OP_SNAP_GET) {
      if (!(op.snap in h.snaps)) {
        r.rc = ENOENT;
      } else {
        r.snap = h.snaps[op.snap];
      }
    } else if (op.kind == OP_SET_PROTECTION) {
      // a blind write on main; op.value is the status the writer saw
      if (!(op.snap in h.snaps)) {
        r.rc = ENOENT;
      } else if (cfg.protectCas && h.snaps[op.snap].protection != op.value) {
        r.rc = EBUSY;
      } else {
        h.snaps[op.snap].protection = op.status;
      }
    } else if (op.kind == OP_CHILD_ATTACH) {
      if (!(op.snap in h.snaps) || h.snaps[op.snap].trash) {
        r.rc = ENOENT;
      } else if (op.child in h.snaps[op.snap].children) {
        r.rc = EEXIST;
      } else {
        h.snaps[op.snap].children += (op.child);
        h.snaps[op.snap].childCount = h.snaps[op.snap].childCount + 1;
      }
    } else if (op.kind == OP_CHILD_DETACH) {
      if (!(op.snap in h.snaps) || !(op.child in h.snaps[op.snap].children)) {
        r.rc = ENOENT;
      } else {
        h.snaps[op.snap].children -= (op.child);
        h.snaps[op.snap].childCount = h.snaps[op.snap].childCount - 1;
      }
    } else if (op.kind == OP_CHILDREN_LIST) {
      if (!(op.snap in h.snaps) || sizeof(h.snaps[op.snap].children) == 0) {
        r.rc = ENOENT;
      } else {
        r.children = h.snaps[op.snap].children;
      }
    } else if (op.kind == OP_SET_PARENT) {
      if (h.parent.image != 0 && h.parent != op.parent) {
        r.rc = EEXIST;
      } else {
        h.parent = op.parent;
        announce mParentSet, (child = op.image, parent = op.parent);
      }
    } else if (op.kind == OP_REMOVE_PARENT) {
      if (h.parent.image == 0) {
        r.rc = ENOENT;
      } else {
        h.parent = default(tParent);
        if (op.value == 1) {
          // deep-flatten: the snapshots lose their parent too
          foreach (k in keys(h.snaps)) {
            h.snaps[k].parent = default(tParent);
          }
        }
        announce mParentSet, (child = op.image, parent = default(tParent));
      }
    } else if (op.kind == OP_REMOVE_IMAGE) {
      h.present = false;
      hdrs[op.image] = h;
      objs -= (op.image);
      foreach (k in keys(watchers[op.image])) {
        DropWatch(op.image, k);
      }
      announce mImageRemoved, (image = op.image, refs = ChildRefs(op.image));
      announce mImage, (image = op.image, present = false);
      return r;
    }
    hdrs[op.image] = h;
    return r;
  }

  fun SnapExists(p: tParent): bool {
    return p.image in hdrs && hdrs[p.image].present && p.snap in hdrs[p.image].snaps;
  }

  // whether image has every object of its own: a flattened child needs
  // its parent no more
  fun Complete(image: int): bool {
    var o: int;
    if (!(image in objs)) {
      return false;
    }
    o = 1;
    while (o <= NOBJ()) {
      if (!(o in objs[image]) || !objs[image][o].present) {
        return false;
      }
      o = o + 1;
    }
    return true;
  }

  fun NameInUse(h: tHeader, name: int): bool {
    var s: int;
    foreach (s in keys(h.snaps)) {
      if (!h.snaps[s].trash && h.snaps[s].name == name) {
        return true;
      }
    }
    return false;
  }

  // every parent link, from a head or a snapshot of an existing image
  // that still needs its parent, to p
  fun ParentRefs(p: tParent): set[tParent] {
    var i: int;
    var s: int;
    var refs: set[tParent];
    foreach (i in keys(hdrs)) {
      if (hdrs[i].present && !Complete(i)) {
        if (hdrs[i].parent == p) {
          refs += ((image = i, snap = 0));
        }
        foreach (s in keys(hdrs[i].snaps)) {
          if (hdrs[i].snaps[s].parent == p) {
            refs += ((image = i, snap = s));
          }
        }
      }
    }
    return refs;
  }

  // every such parent link to any snapshot of image
  fun ChildRefs(image: int): set[tParent] {
    var i: int;
    var s: int;
    var refs: set[tParent];
    foreach (i in keys(hdrs)) {
      if (hdrs[i].present && !Complete(i)) {
        if (hdrs[i].parent.image == image) {
          refs += ((image = i, snap = 0));
        }
        foreach (s in keys(hdrs[i].snaps)) {
          if (hdrs[i].snaps[s].parent.image == image) {
            refs += ((image = i, snap = s));
          }
        }
      }
    }
    return refs;
  }

  // PrimaryLogPG::make_writeable, for a write or a cls copyup
  fun DataWrite(c: int, op: tOp, r: tRes): tRes {
    var o: tObj;
    var snaps: seq[int];
    var cover: seq[int];
    var s: int;
    var k: int;
    var holder: int;
    if (!(op.image in objs)) {
      r.rc = ENOENT;
      return r;
    }
    o = default(tObj);
    if (op.obj in objs[op.image]) {
      o = objs[op.image][op.obj];
    }
    if (op.kind == OP_COPYUP && o.present) {
      return r;   // cls copyup writes only an object that does not exist
    }
    // filter_snapc: removed snapshots drop out of the op's snap context
    foreach (s in op.snapc.snaps) {
      if (s in poolSnaps) {
        snaps += (sizeof(snaps), s);
      }
    }
    if (o.present && sizeof(snaps) > 0 && snaps[0] > o.sq) {
      // clone: the snaps newer than the object's sq
      foreach (s in snaps) {
        if (s > o.sq) {
          cover += (sizeof(cover), s);
        }
      }
      o.clones[op.snapc.sq] = (snaps = cover, val = o.head);
    }
    o.present = true;
    o.head = op.value;
    if (op.snapc.sq > o.sq) {
      o.sq = op.snapc.sq;
    }
    objs[op.image][op.obj] = o;
    holder = 0;
    if (op.image in hdrs && hdrs[op.image].lock.held) {
      holder = hdrs[op.image].lock.owner;
    }
    announce mWrite, (image = op.image, obj = op.obj, client = c, holder = holder,
                      exclusiveLock = cfg.exclusiveLock);
    if (op.image in hdrs) {
      foreach (k in keys(hdrs[op.image].snaps)) {
        announce mSnapView, (image = op.image, snap = k, obj = op.obj, val = View(o, k));
      }
    }
    return r;
  }

  fun DataRead(op: tOp, r: tRes): tRes {
    var o: tObj;
    var v: int;
    if (!(op.image in objs) || !(op.obj in objs[op.image])) {
      r.rc = ENOENT;
      return r;
    }
    o = objs[op.image][op.obj];
    if (op.snap == 0) {
      if (!o.present) {
        r.rc = ENOENT;
      } else {
        r.n = o.head;
      }
      return r;
    }
    v = View(o, op.snap);
    if (v == 0) {
      r.rc = ENOENT;
    } else {
      r.n = v;
    }
    return r;
  }

  // what a read at snapshot snap answers: the first clone whose sq is at
  // least snap, if it covers snap; else the head, if the object has not
  // been cloned past snap; else nothing (0)
  fun View(o: tObj, snap: int): int {
    var k: int;
    var best: int;
    var s: int;
    best = 0;
    foreach (k in keys(o.clones)) {
      if (k >= snap && (best == 0 || k < best)) {
        best = k;
      }
    }
    if (best != 0) {
      foreach (s in o.clones[best].snaps) {
        if (s == snap) {
          return o.clones[best].val;
        }
      }
      return 0;
    }
    if (o.present && snap > o.sq) {
      return o.head;
    }
    return 0;
  }

  fun Views(image: int, snap: int): map[int, int] {
    var o: int;
    var v: map[int, int];
    foreach (o in keys(objs[image])) {
      v[o] = View(objs[image][o], snap);
    }
    return v;
  }

  // snap trimming: a removed snapshot leaves its clones, and a clone that
  // covers no snapshot any more is deleted
  fun TrimSnap(snap: int) {
    var i: int;
    var ob: int;
    var k: int;
    var o: tObj;
    var cl: tClone;
    var left: seq[int];
    var s: int;
    foreach (i in keys(objs)) {
      foreach (ob in keys(objs[i])) {
        o = objs[i][ob];
        foreach (k in keys(o.clones)) {
          cl = o.clones[k];
          left = default(seq[int]);
          foreach (s in cl.snaps) {
            if (s != snap) {
              left += (sizeof(left), s);
            }
          }
          if (sizeof(left) == 0) {
            o.clones -= (k);
          } else {
            cl.snaps = left;
            o.clones[k] = cl;
          }
        }
        objs[i][ob] = o;
      }
    }
  }
}

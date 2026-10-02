/*
 * RBD images shared by several librbd clients: the exclusive lock and the
 * writes it fences, snapshots (create, remove, protect, unprotect) and
 * layering (clone, flatten).
 *
 * The RADOS state is one Store machine: each image's header object (its
 * cls_lock, its watchers, its snapshots and their protection status and
 * clone v2 child counts, its parent), the rbd_children object of clone
 * v1, the pool's self-managed snapshot ids (the monitor), the blocklist,
 * and the val objects with their snapshot clones. Each Store handler is
 * one atomic RADOS op, or one monitor command. Each Client machine is one
 * librbd ImageCtx: its watch, its exclusive lock state machine
 * (ManagedLock, ExclusiveLock), its writes, and the operation it runs.
 */

type tCfg = (
  // the image
  exclusiveLock: bool,      // the exclusive-lock feature; without it, every client writes
  cloneV2: bool,            // clones attach with child_attach (v2), or rbd_children (v1)
  proxySnapRemove: bool,    // fast-diff or journaling: snap remove goes to the lock owner
                            // (Operations.cc:1050-1074); otherwise it runs locally, with no lock
  proxyProtect: bool,       // journaling: protect and unprotect go to the lock owner
                            // (Operations.cc:1250, 1343); otherwise local, with no lock
  // mechanisms on main (true is main)
  blocklistOnBreak: bool,   // rbd_blocklist_on_break_lock: a break blocklists the old owner first
  autoPolicy: bool,         // rbd_auto_exclusive_lock_until_manual_request: the owner releases
                            // the lock when a peer asks (AutomaticPolicy); false is StandardPolicy,
                            // which answers -EROFS
  cloneRechecks: bool,      // a clone v1 refreshes the parent after add_child and checks the
                            // snapshot is still PROTECTED (AttachChildRequest.cc:85-109)
  unprotectScans: bool,     // unprotect scans rbd_children before it writes UNPROTECTED
  deepFlatten: bool,        // the deep-flatten feature: a flatten detaches the child even when
                            // it has snapshots, and strips the parent from them
  // proposed (false is main)
  protectCas: bool,         // set_protection_status(UNPROTECTED) applies only over UNPROTECTING,
                            // and PROTECTED only over UNPROTECTED
  refreshOnAcquire: bool,   // PostAcquireRequest refreshes the header whether or not a
                            // HeaderUpdate was seen (main refreshes only if one was)
  // environment
  watchDrops: int,          // how many times the OSD may drop a live client's watch (a watch
                            // timeout: the client learns of it later and re-watches)
  crashes: int              // how many times each client may die, at any step; its lock
                            // entry stays until a peer breaks it, and its watches lapse
);

enum tRc { OK, EBUSY, EAGAIN, ENOENT, EEXIST, EROFS, EBLOCKLISTED, EINVAL, ESTALE, ETIMEDOUT,
           ERESTART, EOPNOTSUPP, ENOTEMPTY }

// a cls_lock entry on a header: the holder's entity (the client) and cookie
// (its watch handle); addr is the client too
type tLock = (held: bool, owner: int, cookie: int);

// protection status (rbd_types.h:153-156)
fun UNPROTECTED(): int { return 0; }
fun UNPROTECTING(): int { return 1; }
fun PROTECTED(): int { return 2; }

// a parent spec: (image, snap); image 0 is none
type tParent = (image: int, snap: int);

// a snapshot in a header: cls_rbd_snap, with the parent it recorded
type tSnap = (name: int, protection: int, trash: bool, childCount: int, children: set[int], parent: tParent);

// a client's snapshot context, and what refresh reads from the header
type tSnapc = (sq: int, snaps: seq[int]);   // snaps newest first
type tHeader = (present: bool, snapSeq: int, snaps: map[int, tSnap], parent: tParent, lock: tLock);

// a val object: its head, the snapset's sq, and its clones (by the
// clone's sq, the op's snapc.sq that made it) with the snaps each covers
type tClone = (snaps: seq[int], val: int);
type tObj = (present: bool, head: int, sq: int, clones: map[int, tClone]);

// what the store answers an op
type tRes = (rc: tRc, tag: int, n: int, lock: tLock, watchers: map[int, int], hdr: tHeader,
             children: set[int], snap: tSnap, obj: tObj);

enum tOpKind {
  // the header's watch, and notifies
  OP_WATCH, OP_UNWATCH, OP_LIST_WATCHERS,
  // cls_lock on the header
  OP_LOCK, OP_GET_LOCK_INFO, OP_BREAK_LOCK, OP_SET_COOKIE, OP_UNLOCK,
  // the monitor
  OP_BLOCKLIST, OP_SNAP_ALLOC, OP_SNAP_RELEASE,
  // cls_rbd on the header
  OP_READ_HEADER, OP_SNAP_ADD, OP_SNAP_REMOVE, OP_SNAP_TRASH_ADD, OP_SNAP_GET, OP_SET_PROTECTION,
  OP_CHILD_ATTACH, OP_CHILD_DETACH, OP_CHILDREN_LIST,
  OP_CREATE_IMAGE, OP_REMOVE_IMAGE, OP_SET_PARENT, OP_REMOVE_PARENT,
  // rbd_children (clone v1)
  OP_ADD_CHILD, OP_REMOVE_CHILD, OP_GET_CHILDREN,
  // val objects
  OP_WRITE, OP_READ, OP_COPYUP
}

type tOp = (kind: tOpKind, image: int, snap: int, obj: int, cookie: int, cookie2: int, owner: int,
            child: int, status: int, snapc: tSnapc, value: int, name: int, parent: tParent, tag: int);

fun Op(kind: tOpKind, image: int, tag: int): tOp {
  return (kind = kind, image = image, snap = 0, obj = 0, cookie = 0, cookie2 = 0, owner = 0, child = 0,
          status = 0, snapc = default(tSnapc), value = 0, name = 0, parent = default(tParent), tag = tag);
}

// notify payloads (WatchNotifyTypes.h)
enum tNotifyKind { N_ACQUIRED_LOCK, N_RELEASED_LOCK, N_REQUEST_LOCK, N_HEADER_UPDATE,
                   N_ASYNC_REQUEST, N_ASYNC_COMPLETE }
enum tReqKind { R_WRITE, R_SNAP_CREATE, R_SNAP_REMOVE, R_SNAP_PROTECT, R_SNAP_UNPROTECT, R_CLONE,
                R_FLATTEN, R_RELEASE_LOCK, R_ACQUIRE_LOCK, R_READ_CHILD, R_REMOVE_IMAGE }
type tNotify = (kind: tNotifyKind, from: int, req: tReqKind, asyncId: int, snapName: int, result: tRc);
// an ack: empty means the watcher sent no payload
type tAck = (empty: bool, result: tRc);

// a script action for a client, on its image
type tAction = (kind: tReqKind, obj: int, snapName: int, child: int);

fun Write(obj: int): tAction { return (kind = R_WRITE, obj = obj, snapName = 0, child = 0); }
fun ReadChild(obj: int): tAction { return (kind = R_READ_CHILD, obj = obj, snapName = 0, child = 0); }
fun SnapCreate(name: int): tAction { return (kind = R_SNAP_CREATE, obj = 0, snapName = name, child = 0); }
fun SnapRemove(name: int): tAction { return (kind = R_SNAP_REMOVE, obj = 0, snapName = name, child = 0); }
fun Protect(name: int): tAction { return (kind = R_SNAP_PROTECT, obj = 0, snapName = name, child = 0); }
fun Unprotect(name: int): tAction { return (kind = R_SNAP_UNPROTECT, obj = 0, snapName = name, child = 0); }
// clone the client's image at snapshot name into image child
fun Clone(name: int, child: int): tAction { return (kind = R_CLONE, obj = 0, snapName = name, child = child); }
fun Flatten(): tAction { return (kind = R_FLATTEN, obj = 0, snapName = 0, child = 0); }
fun ReleaseLock(): tAction { return (kind = R_RELEASE_LOCK, obj = 0, snapName = 0, child = 0); }
fun AcquireLock(): tAction { return (kind = R_ACQUIRE_LOCK, obj = 0, snapName = 0, child = 0); }
// remove the client's image (rbd rm: PreRemoveRequest, RemoveRequest)
fun RemoveImage(): tAction { return (kind = R_REMOVE_IMAGE, obj = 0, snapName = 0, child = 0); }

// a client's script: the image it opens, and its actions in order
type tScript = (image: int, actions: seq[tAction]);

fun Acts1(a: tAction): seq[tAction] {
  var s: seq[tAction];
  s += (0, a);
  return s;
}
fun Acts2(a: tAction, b: tAction): seq[tAction] {
  var s: seq[tAction];
  s += (0, a);
  s += (1, b);
  return s;
}
fun Acts3(a: tAction, b: tAction, c: tAction): seq[tAction] {
  var s: seq[tAction];
  s += (0, a);
  s += (1, b);
  s += (2, c);
  return s;
}
fun Acts4(a: tAction, b: tAction, c: tAction, d: tAction): seq[tAction] {
  var s: seq[tAction];
  s += (0, a);
  s += (1, b);
  s += (2, c);
  s += (3, d);
  return s;
}
fun On(image: int, acts: seq[tAction]): tScript { return (image = image, actions = acts); }

// the initial state: image 1 present with objects 1 and 2 written once; a
// snapshot named 1 present, protected, if the scenario says so
type tInit = (snap: bool, protected: bool, children: bool);

fun NOBJ(): int { return 2; }

/* events */

// client -> store, and the answer
event eOp: (from: machine, client: int, op: tOp);
event eRes: tRes;
// store -> watcher: a notify to ack; watcher -> store: the ack
event eNotify: (nid: int, image: int, n: tNotify);
event eAck: (nid: int, client: int, ack: tAck);
// client -> store: a notify on an image's header; store -> client: every ack in
event eNotifyReq: (from: machine, client: int, image: int, n: tNotify, tag: int);
event eNotified: (tag: int, acks: seq[tAck], rc: tRc);
// store -> client: the OSD dropped the watch, or the client was blocklisted
event eWatchError: (image: int, rc: tRc);
// a client's own request timer (rbd_request_timed_out_seconds): the
// generation of the request it was armed for
event eRequestTimer: int;
// a client's own retry timer for its lock request (schedule_request_lock)
event eLockRetryTimer: int;
// a client died: client -> store (its watches lapse), client -> driver
// (its remaining actions will not be answered)
event eCrash: int;
event eCrashed: (client: int, remaining: int);
// driver -> client: every action is answered; answer once nothing is in
// flight (an op run for a peer, a write, a lock action)
event eDrain: machine;
event eDrained: int;
// driver
event eActionDone: (client: int, rc: tRc);
event eQuiesce: machine;
event eQuiesced;

/* what the specs observe */

// a write applied to image.obj by a client, while the header's lock was
// held by holder (0: nobody)
event mWrite: (image: int, obj: int, client: int, holder: int, exclusiveLock: bool);
// snapshot snap of image answered created; the val each object shows at it
event mSnapCreated: (image: int, snap: int, view: map[int, int]);
// after a write: what each snapshot of the image shows for the object
event mSnapView: (image: int, snap: int, obj: int, val: int);
// snapshot snap of image removed from its header, and the parent links
// (a child's head, snap 0, or one of its snapshots) that still name it
// from a child that still needs it: one with an object not yet its own
event mSnapRemoved: (image: int, snap: int, refs: set[tParent]);
// an image removed, and such parent links to a snapshot of it
event mImageRemoved: (image: int, refs: set[tParent]);
// a child read an object from its parent (flatten, or a read through)
event mParentRead: (child: int, obj: int, rc: tRc);
// a clone completed: image child is a child from now on
event mCloneDone: int;
// a snapshot_add succeeded for the request with this async id (0: local)
event mSnapAddedFor: int;
// a child image's parent spec set, or cleared (image 0)
event mParentSet: (child: int, parent: tParent);
// an image created or removed
event mImage: (image: int, present: bool);
event mActionStarted: (client: int, kind: tReqKind, asyncId: int);
// a client died, with an action running or not
event mCrashed: (client: int, running: bool);
// a remove of snapshot snap of image began (snapshot_trash_add succeeded)
event mSnapRemoveStarted: (image: int, snap: int);
event mActionDone: (client: int, kind: tReqKind, asyncId: int, rc: tRc);
// at the end: the pool's snapshot ids, every header's, and the parent
// links (child image, child snap) that name a snapshot that is gone
event mFinal: (poolSnaps: set[int], headerSnaps: set[int], dangling: set[tParent]);

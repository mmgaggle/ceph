// With the exclusive-lock feature, every write applied to a val object
// comes from the client that holds the header's lock at that moment: a
// client that lost the lock is fenced before the new holder writes.
spec WritesFenced observes mWrite {
  start state Watch {
    on mWrite do (w: (image: int, obj: int, client: int, holder: int, exclusiveLock: bool)) {
      if (w.exclusiveLock) {
        assert w.holder == w.client,
          format("client {0} wrote object {1} of image {2} while client {3} held the exclusive lock",
                 w.client, w.obj, w.image, w.holder);
      }
    }
  }
}

// What a snapshot shows never changes after it is created: the val each
// object showed at the snapshot when snapshot_add committed is what any
// later read at the snapshot answers.
spec SnapImmutable observes mSnapCreated, mSnapView, mSnapRemoved {
  var views: map[(image: int, snap: int), map[int, int]];
  start state Watch {
    on mSnapCreated do (c: (image: int, snap: int, view: map[int, int])) {
      views[(image = c.image, snap = c.snap)] = c.view;
    }
    on mSnapView do (v: (image: int, snap: int, obj: int, val: int)) {
      var k: (image: int, snap: int);
      k = (image = v.image, snap = v.snap);
      if (k in views && v.obj in views[k]) {
        assert views[k][v.obj] == v.val,
          format("snapshot {0} of image {1} showed val {2} for object {3} when it was created, and shows {4} now",
                 v.snap, v.image, views[k][v.obj], v.obj, v.val);
      }
    }
    on mSnapRemoved do (s: (image: int, snap: int, refs: set[tParent])) {
      views -= ((image = s.image, snap = s.snap));
    }
  }
}

// A parent snapshot is never removed while a child's head, or a child's
// snapshot, still reads through it; nor is a parent image. A child is an
// image whose clone completed (one a clone still builds, and will remove
// if it fails, does not count).
spec ChildHasParent observes mSnapRemoved, mImageRemoved, mCloneDone {
  var children: set[int];
  start state Watch {
    on mCloneDone do (child: int) {
      children += (child);
    }
    on mSnapRemoved do (s: (image: int, snap: int, refs: set[tParent])) {
      var r: tParent;
      foreach (r in s.refs) {
        assert !(r.image in children),
          format("snapshot {0} of image {1} was removed while child {2} still reads through it (at its snapshot {3}; 0 is its head)",
                 s.snap, s.image, r.image, r.snap);
      }
    }
    on mImageRemoved do (s: (image: int, refs: set[tParent])) {
      var r: tParent;
      foreach (r in s.refs) {
        assert !(r.image in children),
          format("image {0} was removed while child {1} still reads through it", s.image, r.image);
      }
    }
  }
}

// A child always finds its parent's val: every object of the parent was
// written before the clone, so a read through to the parent never fails.
spec ParentReadable observes mParentRead {
  start state Watch {
    on mParentRead do (r: (child: int, obj: int, rc: tRc)) {
      assert r.rc == OK,
        format("child {0} read object {1} from its parent and got {2}: the parent snapshot's val is gone",
               r.child, r.obj, r.rc);
    }
  }
}

// A snapshot create answered -EEXIST created no snapshot itself: it is
// answered success once the snapshot it added exists.
spec CreateAnswered observes mSnapAddedFor, mActionDone {
  var added: set[int];
  start state Watch {
    on mSnapAddedFor do (asyncId: int) {
      added += (asyncId);
    }
    on mActionDone do (d: (client: int, kind: tReqKind, asyncId: int, rc: tRc)) {
      if (d.kind == R_SNAP_CREATE && d.rc == EEXIST) {
        assert !(d.asyncId in added),
          format("client {0}'s snapshot create was answered EEXIST, for the snapshot it created itself (request {1})",
                 d.client, d.asyncId);
      }
    }
  }
}

// Every action is answered (liveness).
spec AllAnswered observes mActionStarted, mActionDone {
  var outstanding: int;
  start state Idle {
    on mActionStarted do (s: (client: int, kind: tReqKind, asyncId: int)) {
      outstanding = 1;
      goto Waiting;
    }
  }
  hot state Waiting {
    on mActionStarted do (s: (client: int, kind: tReqKind, asyncId: int)) {
      outstanding = outstanding + 1;
    }
    on mActionDone do (d: (client: int, kind: tReqKind, asyncId: int, rc: tRc)) {
      outstanding = outstanding - 1;
      if (outstanding == 0) {
        goto Idle;
      }
    }
  }
}

// At the end, every snapshot id the pool holds belongs to a snapshot in
// some header: a snapshot create does not leak the ids it allocated.
spec NoLeakedSnapIds observes mFinal {
  start state Watch {
    on mFinal do (f: (poolSnaps: set[int], headerSnaps: set[int])) {
      var s: int;
      foreach (s in f.poolSnaps) {
        assert s in f.headerSnaps,
          format("pool snapshot id {0} belongs to no image's snapshot: leaked", s);
      }
    }
  }
}

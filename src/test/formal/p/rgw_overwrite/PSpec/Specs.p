// No data object is deleted while a key's head references it, and no head
// is written over an object already deleted: what each key holds stays
// readable. (With cls_refcount, an object sent to GC under one tag can
// rightly survive through another reference; only its deletion counts.)
spec HeadIntact observes mHead, mDeleted, mCreated {
  var heads: map[int, set[int]];
  var writers: map[int, int];
  var deleted: set[int];
  start state Watch {
    on mHead do (h: (key: int, writer: int, manifest: set[int])) {
      var o: int;
      foreach (o in h.manifest) {
        assert !(o in deleted),
          format("request {0} wrote the head of key {1} over object {2}, already deleted", h.writer, h.key, o);
      }
      heads[h.key] = h.manifest;
      writers[h.key] = h.writer;
    }
    on mDeleted do (o: int) {
      var k: int;
      foreach (k in keys(heads)) {
        assert !(o in heads[k]),
          format("object {0} was deleted while the head of key {1} (request {2}'s) references it", o, k, writers[k]);
      }
      deleted += (o);
    }
    on mCreated do (o: int) {
      deleted -= (o);
    }
  }
}

// At the end, once nothing is pending on a key's bucket index entry, the
// entry lists the object the key's head holds, or nothing if there is no
// head. (A pending entry would be checked against the head by the next
// listing.)
spec IndexMatchesHead observes mFinal {
  start state Watch {
    on mFinal do (f: tFinal) {
      var k: int;
      var e: tIx;
      var h: tHead;
      foreach (k in keys(f.heads)) {
        h = f.heads[k];
        e = f.ixs[k];
        if (sizeof(e.pending) == 0) {
          if (h.present) {
            assert e.present && e.listed && e.writer == h.writer,
              format("the bucket index lists request {0}'s object at key {1} (listed: {2}), but the head holds request {3}'s",
                     e.writer, k, e.present && e.listed, h.writer);
          } else {
            assert !(e.present && e.listed),
              format("the bucket index lists request {0}'s object at key {1}, but there is no head", e.writer, k);
          }
        }
      }
    }
  }
}

// At the end, after GC has run every queued chain, every data object is
// referenced by a head or by a live upload; and every multipart index
// entry belongs to a live upload.
spec NoOrphans observes mFinal {
  start state Watch {
    on mFinal do (f: tFinal) {
      var o: int;
      foreach (o in f.live) {
        assert o in f.referenced,
          format("object {0} leaked: nothing references it, and GC will never delete it", o);
      }
      foreach (o in f.mpIndex) {
        assert o in f.validKeys, format("index entry {0} orphaned in the multipart namespace", o);
      }
    }
  }
}

// a successful completion answers the object's ETag, never an empty one
spec CompletionEtag observes mCompleted {
  start state Watch {
    on mCompleted do (c: (rid: int, etag: int, want: int)) {
      assert c.etag == c.want,
        format("completion {0} answered success with ETag {1} (empty if 0), not {2}", c.rid, c.etag, c.want);
    }
  }
}

// every request is answered, unless its RGW dies
spec AllAnswered observes mStarted, mAnswered, mCrashed {
  var open: set[int];
  start cold state Idle {
    on mStarted do (r: int) {
      open += (r);
      goto Busy;
    }
    ignore mAnswered, mCrashed;
  }
  hot state Busy {
    on mStarted do (r: int) {
      open += (r);
    }
    on mAnswered do (a: (rid: int, ok: bool)) {
      open -= (a.rid);
      if (sizeof(open) == 0) {
        goto Idle;
      }
    }
    on mCrashed do (r: int) {
      open -= (r);
      if (sizeof(open) == 0) {
        goto Idle;
      }
    }
  }
}

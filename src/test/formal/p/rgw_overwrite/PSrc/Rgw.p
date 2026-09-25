/*
 * RGW serving one S3 request, one RADOS op at a time:
 * - PutObject (RGWPutObj, AtomicObjectProcessor);
 * - DeleteObject on a non-versioned bucket
 *   (RGWRados::Object::Delete::delete_obj);
 * - CopyObject within one pool (RGWRados::copy_obj), which shares the
 *   source's tail;
 * - UploadPart (RGWPutObj, MultipartObjectProcessor);
 * - CompleteMultipartUpload (RGWCompleteMultipart,
 *   RadosMultipartUpload::complete);
 * - AbortMultipartUpload (RGWAbortMultipart) and lifecycle's
 *   AbortIncompleteMultipartUpload (RGWLC::handle_multipart_expiration),
 *   both through RadosMultipartUpload::abort.
 * A request's tag, and the writer recorded in what it writes, is its rid.
 */
machine Rgw {
  var cfg: tCfg;
  var store: machine;
  var driver: machine;
  var rid: int;

  start state Serve {
    entry (p: (cfg: tCfg, store: machine, driver: machine, rid: int, req: tSpec)) {
      cfg = p.cfg;
      store = p.store;
      driver = p.driver;
      rid = p.rid;
      announce mStarted, rid;
      if (p.req.kind == R_PUT) {
        PutObject(p.req.key);
      } else if (p.req.kind == R_DELETE) {
        DeleteObject(p.req.key);
      } else if (p.req.kind == R_COPY) {
        CopyObject(p.req.src, p.req.key);
      } else if (p.req.kind == R_UPLOAD_PART) {
        UploadPart(p.req.upload, p.req.num, p.req.etag);
      } else if (p.req.kind == R_COMPLETE) {
        CompleteMultipart(p.req.upload, p.req.list);
      } else if (p.req.kind == R_ABORT) {
        Abort(p.req.upload, cfg.abortTakesLock);
      } else {
        Abort(p.req.upload, cfg.lcTakesLock);
      }
    }
  }

  // AtomicObjectProcessor: the tail first, then the head (holding the
  // first chunk) in write_meta
  fun PutObject(key: int) {
    var tail: set[int];
    tail += (TAIL(rid));
    WriteData(tail);
    if (WriteMeta(key, tail, rid, 0, default(set[int]), rid, false)) {
      // lost a race: ~RadosWriter removes the tail it wrote
      DeleteInline(tail);
    }
    Answer(true);
  }

  // RGWRados::Object::Delete::delete_obj on a non-versioned bucket
  fun DeleteObject(key: int) {
    var st: tHead;
    var r: (rc: tRc, epoch: int);
    st = ReadHead(key);
    if (!st.present) {
      Answer(true);  // -ENOENT, answered 204
      return;
    }
    IndexPrepare(key);
    // cls_rgw_remove_obj; on main without the ID tag check (55f5b762c67)
    r = HeadRemove(key, cfg.deleteGuard, st.tag);
    if (r.rc == OK || r.rc == ENOENT) {
      IndexComplete(key, IX_DEL, 1, r.epoch, default(set[int]));
      // complete_atomic_modification: the head it read goes to GC
      SendGc(st.tailTag, st.manifest);
      Answer(true);
      return;
    }
    IndexComplete(key, IX_CANCEL, -1, 0, default(set[int]));
    Answer(false);
  }

  // RGWRados::copy_obj within one pool. The destination shares the
  // source's tail: each tail object gets a reference under the new head's
  // tag, which is also the new head's tail tag.
  fun CopyObject(src: int, dst: int) {
    var s: tHead;
    var got: set[int];
    var o: int;
    var g: int;
    var rc: tRc;
    var canceled: bool;
    s = ReadHead(src);
    if (!s.present) {
      Answer(false);  // NoSuchKey
      return;
    }
    if (src == dst) {
      // copy_itself: the manifest read above, keep_tail, the tail tag
      // kept. The write reads the head afresh (the destination has its own
      // RGWObjectCtx), so it guards on whatever head is there by then.
      if (cfg.copySelfGuardsSource) {
        canceled = WriteHeadOver(dst, s, s.manifest, s.etag, s.upload, s.tailTag);
      } else {
        canceled = WriteMeta(dst, s.manifest, s.etag, s.upload, default(set[int]), s.tailTag, true);
      }
      Answer(true);
      return;
    }
    if (cfg.copyTakesRefs) {
      foreach (o in s.manifest) {
        rc = RefGet(o, rid);
        if (rc != OK) {
          // done_ret: drop the references taken
          foreach (g in got) {
            rc = RefPut(g, rid);
          }
          Answer(false);
          return;
        }
        got += (o);
      }
    }
    canceled = WriteMeta(dst, s.manifest, s.etag, 0, default(set[int]), rid, false);
    if (canceled && cfg.copyLoserDropsRefs) {
      foreach (g in got) {
        rc = RefPut(g, rid);
      }
    }
    Answer(true);
  }

  // RGWRados::Object::Write::write_meta and _do_write_meta. Returns true
  // if the write was canceled - it lost a race, which RGW answers as a
  // success.
  fun WriteMeta(key: int, manifest: set[int], etag: int, upload: int, removeKeys: set[int],
                tailTag: int, keepTail: bool): bool {
    var st: tHead;
    var nh: tHead;
    var r: (rc: tRc, epoch: int);
    var assumeNoent: bool;
    var prepared: bool;
    var attempts: int;
    var old: set[int];
    var o: int;
    nh = (present = true, tag = rid, tailTag = tailTag, manifest = manifest, writer = rid, etag = etag,
          upload = upload);
    // first without reading the head, as an exclusive create; on
    // -EEXIST, read it and replace it
    assumeNoent = true;
    while (attempts < 2) {
      attempts = attempts + 1;
      if (assumeNoent) {
        st = default(tHead);
      } else {
        st = ReadHead(key);
      }
      if (!prepared) {
        IndexPrepare(key);
        prepared = true;
      }
      // prepare_atomic_modification: cmpxattr on the ID tag read, and an
      // exclusive create if there was no head
      r = HeadWrite(key, st.present && cfg.idTagGuard, st.tag, !st.present, nh);
      if (!(r.rc == EEXIST && assumeNoent)) {
        attempts = 2;
      }
      assumeNoent = false;
    }
    if (r.rc != OK) {
      // done_cancel: -ECANCELED, -ENOENT or -EEXIST, answered as success
      IndexComplete(key, IX_CANCEL, -1, 0, removeKeys);
      return true;
    }
    // complete_atomic_modification: the replaced head's manifest goes to
    // GC under its tail tag, unless keep_tail
    if (st.present && !keepTail) {
      foreach (o in st.manifest) {
        if (!(cfg.gcSparesHead && o in manifest)) {
          old += (o);
        }
      }
      SendGc(st.tailTag, old);
    }
    IndexComplete(key, IX_ADD, 1, r.epoch, removeKeys);
    return false;
  }

  // proposed: a keep_tail rewrite guarded on the head it read before, so
  // it lands only over that head
  fun WriteHeadOver(key: int, st: tHead, manifest: set[int], etag: int, upload: int, tailTag: int): bool {
    var nh: tHead;
    var r: (rc: tRc, epoch: int);
    nh = (present = true, tag = rid, tailTag = tailTag, manifest = manifest, writer = rid, etag = etag,
          upload = upload);
    IndexPrepare(key);
    r = HeadWrite(key, true, st.tag, false, nh);
    if (r.rc != OK) {
      IndexComplete(key, IX_CANCEL, -1, 0, default(set[int]));
      return true;
    }
    IndexComplete(key, IX_ADD, 1, r.epoch, default(set[int]));
    return false;
  }

  // MultipartObjectProcessor
  fun UploadPart(u: int, num: int, etag: int) {
    var prefix: int;
    var obj: int;
    var rc: tRc;
    var written: set[int];
    // process_first_chunk: the part head is created exclusively; if the
    // part was uploaded before, under a random prefix
    prefix = u;
    rc = CreateExcl(OBJ(prefix, num));
    if (rc == EEXIST) {
      prefix = RANDPREFIX(rid);
      rc = CreateExcl(OBJ(prefix, num));
      assert rc == OK, "a random part prefix collided";
    }
    obj = OBJ(prefix, num);
    written += (obj);
    // the part head's write_meta: its entry in the multipart namespace
    MpIndexAdd(obj);
    rc = PartUpdate(u, num, (prefix = prefix, etag = etag, past = default(set[int])));
    if (rc != OK) {
      // -ERR_NO_SUCH_UPLOAD: ~RadosWriter removes the part head, through
      // the index
      DeleteInline(written);
      MpIndexDel(obj);
      Answer(false);
      return;
    }
    Answer(true);
  }

  // RGWCompleteMultipart::execute
  fun CompleteMultipart(u: int, list: map[int, int]) {
    var m: (rc: tRc, ver: int);
    var lp: (rc: tRc, parts: map[int, tPart]);
    var h: tHead;
    var ver: int;
    var num: int;
    var pt: tPart;
    var done: set[int];
    var processed: map[int, set[int]];
    var hist: (chain: set[int], ixKeys: set[int], done: set[int]);
    var manifest: set[int];
    var removeKeys: set[int];
    var chain: set[int];
    var canceled: bool;
    var i: int;
    var rc: tRc;
    var k: int;

    // the lock keeps racing completions and aborts off the parts
    m = TryLock(u);
    if (m.rc == ENOENT) {
      // check_previously_completed: the head's ETag against the list's
      h = ReadHead(MPKEY());
      if (h.present && h.etag == MPETAG(list)) {
        if (cfg.replayAnswersEtag) {
          announce mCompleted, (rid = rid, etag = h.etag, want = MPETAG(list));
        } else {
          announce mCompleted, (rid = rid, etag = 0, want = MPETAG(list));
        }
        Answer(true);
        return;
      }
      Answer(false);
      return;
    }
    if (m.rc != OK) {
      Answer(false);  // "This multipart completion is already in progress"
      return;
    }
    m = GetAttrs(u);
    if (m.rc != OK) {
      Finish(u, false);
      return;
    }
    ver = m.ver;
    if (!IsLocked(u)) {
      Finish(u, false);  // lock renewal failed
      return;
    }

    // RadosMultipartUpload::complete: the parts must be the list's
    lp = ListParts(u);
    if (lp.rc != OK) {
      Finish(u, false);
      return;
    }
    if (sizeof(lp.parts) != sizeof(list)) {
      Finish(u, false);
      return;
    }
    foreach (num in keys(list)) {
      if (!(num in lp.parts) || lp.parts[num].etag != list[num]) {
        Finish(u, false);  // -ERR_INVALID_PART
        return;
      }
    }
    foreach (num in keys(lp.parts)) {
      pt = lp.parts[num];
      manifest += (OBJ(pt.prefix, num));
      removeKeys += (OBJ(pt.prefix, num));
      done = default(set[int]);
      done += (pt.prefix);
      hist = History(num, pt, done);
      SendGc(UPLOADTAG(u), hist.chain);
      foreach (k in hist.ixKeys) {
        removeKeys += (k);
      }
      processed[num] = hist.done;
    }
    canceled = WriteMeta(MPKEY(), manifest, MPETAG(list), u, removeKeys, rid, false);
    if (canceled && cfg.loserGcsParts) {
      SendGc(UPLOADTAG(u), manifest);
    }
    if (cfg.completeMayCrash && $) {
      Crash();
      return;
    }

    // delete the meta object, which releases the lock. A part upload
    // that raced the completion makes that -ECANCELED: GC its part and
    // try again.
    removeKeys = default(set[int]);
    i = 0;
    while (i < 3) {
      if (cfg.metaDeleteMayFail && $) {
        rc = EIO;
      } else {
        rc = MetaDelete(u, VerCheck(ver), removeKeys);
      }
      if (rc != ECANCELED || i == 2) {
        break;  // any error is only logged
      }
      m = GetAttrs(u);
      if (m.rc != OK) {
        break;
      }
      ver = m.ver;
      // cleanup_orphaned_parts
      lp = ListParts(u);
      if (lp.rc == OK) {
        chain = default(set[int]);
        foreach (num in keys(lp.parts)) {
          pt = lp.parts[num];
          done = default(set[int]);
          if (num in processed) {
            done = processed[num];
          }
          if (!(pt.prefix in done)) {
            chain += (OBJ(pt.prefix, num));
            removeKeys += (OBJ(pt.prefix, num));
          }
          hist = History(num, pt, done);
          SendGc(UPLOADTAG(u), hist.chain);
          foreach (k in hist.ixKeys) {
            removeKeys += (k);
          }
          processed[num] = hist.done;
        }
        SendGc(UPLOADTAG(u), chain);
      }
      i = i + 1;
    }
    // the ETag set on the object's attrs by upload->complete()
    announce mCompleted, (rid = rid, etag = MPETAG(list), want = MPETAG(list));
    Finish(u, true);
  }

  // cleanup_part_history: a part's past prefixes go to GC and their
  // entries out of the index, skipping those processed already
  fun History(num: int, pt: tPart, done: set[int]): (chain: set[int], ixKeys: set[int], done: set[int]) {
    var r: (chain: set[int], ixKeys: set[int], done: set[int]);
    var pp: int;
    r.done = done;
    foreach (pp in pt.past) {
      if (!(pp in r.done) || !cfg.historySkipsProcessed) {
        r.done += (pp);
        r.ixKeys += (OBJ(pp, num));
        r.chain += (OBJ(pp, num));
      }
    }
    return r;
  }

  // RGWAbortMultipart::execute and the lifecycle's abort, around
  // RadosMultipartUpload::abort
  fun Abort(u: int, takeLock: bool) {
    var m: (rc: tRc, ver: int);
    var lp: (rc: tRc, parts: map[int, tPart]);
    var h: tHead;
    var num: int;
    var pt: tPart;
    var done: set[int];
    var processed: map[int, set[int]];
    var hist: (chain: set[int], ixKeys: set[int], done: set[int]);
    var removeKeys: set[int];
    var chain: set[int];
    var i: int;
    var rc: tRc;
    var k: int;
    if (takeLock) {
      m = TryLock(u);
      if (m.rc != OK) {
        Answer(false);
        return;
      }
    }
    rc = ENOENT;
    i = 0;
    while (i < 3) {
      m = GetAttrs(u);
      if (m.rc != OK) {
        break;
      }
      lp = ListParts(u);
      if (lp.rc != OK) {
        break;
      }
      if (cfg.gcSparesHead) {
        h = ReadHead(MPKEY());
      }
      chain = default(set[int]);
      foreach (num in keys(lp.parts)) {
        pt = lp.parts[num];
        done = default(set[int]);
        if (num in processed) {
          done = processed[num];
        }
        if (!(pt.prefix in done)) {
          done += (pt.prefix);
          if (!(cfg.gcSparesHead && h.present && OBJ(pt.prefix, num) in h.manifest)) {
            chain += (OBJ(pt.prefix, num));
          }
          removeKeys += (OBJ(pt.prefix, num));
          hist = History(num, pt, done);
          SendGc(UPLOADTAG(u), hist.chain);
          foreach (k in hist.ixKeys) {
            removeKeys += (k);
          }
          done = hist.done;
        }
        processed[num] = done;
      }
      SendGc(UPLOADTAG(u), chain);
      rc = MetaDelete(u, VerCheck(m.ver), removeKeys);
      if (rc != ECANCELED) {
        break;
      }
      i = i + 1;
    }
    if (takeLock) {
      Unlock(u);
    }
    Answer(rc == OK);
  }

  fun VerCheck(ver: int): int {
    if (cfg.metaVersionCheck) {
      return ver;
    }
    return -1;
  }

  // RGWCompleteMultipart::complete: unlock if still held, and answer
  fun Finish(u: int, ok: bool) {
    Unlock(u);
    Answer(ok);
  }

  fun Answer(ok: bool) {
    announce mAnswered, (rid = rid, ok = ok);
    send driver, eDone, (rid = rid, crashed = false);
  }

  // RGW dies: no answer, and the lock it holds expires
  fun Crash() {
    send store, eCrashed, rid;
    announce mCrashed, rid;
    send driver, eDone, (rid = rid, crashed = true);
  }

  // RADOS ops

  fun ReadHead(key: int): tHead {
    var h: tHead;
    send store, eReadHead, (from = this, key = key);
    receive {
      case eHeadRead: (x: tHead) { h = x; }
    }
    return h;
  }

  fun HeadWrite(key: int, guard: bool, expectTag: int, excl: bool, nh: tHead): (rc: tRc, epoch: int) {
    var r: (rc: tRc, epoch: int);
    send store, eHeadWrite, (from = this, key = key, guard = guard, expectTag = expectTag, excl = excl, head = nh);
    receive {
      case eHeadWritten: (x: (rc: tRc, epoch: int)) { r = x; }
    }
    return r;
  }

  fun HeadRemove(key: int, guard: bool, expectTag: int): (rc: tRc, epoch: int) {
    var r: (rc: tRc, epoch: int);
    send store, eHeadRemove, (from = this, key = key, guard = guard, expectTag = expectTag);
    receive {
      case eHeadWritten: (x: (rc: tRc, epoch: int)) { r = x; }
    }
    return r;
  }

  fun WriteData(objs: set[int]) {
    send store, eWriteData, (from = this, objs = objs);
    receive {
      case eDataDone: (rc: tRc) { }
    }
  }

  fun CreateExcl(obj: int): tRc {
    var r: tRc;
    send store, eCreateExcl, (from = this, obj = obj);
    receive {
      case eDataDone: (rc: tRc) { r = rc; }
    }
    return r;
  }

  fun DeleteInline(objs: set[int]) {
    send store, eDeleteInline, (from = this, objs = objs);
    receive {
      case eDataDone: (rc: tRc) { }
    }
  }

  fun SendGc(tag: int, objs: set[int]) {
    if (sizeof(objs) == 0) {
      return;
    }
    send store, eSendGc, (from = this, tag = tag, objs = objs);
    receive {
      case eDataDone: (rc: tRc) { }
    }
  }

  fun RefGet(obj: int, tag: int): tRc {
    var r: tRc;
    send store, eRefGet, (from = this, obj = obj, tag = tag);
    receive {
      case eDataDone: (rc: tRc) { r = rc; }
    }
    return r;
  }

  fun RefPut(obj: int, tag: int): tRc {
    var r: tRc;
    send store, eRefPut, (from = this, obj = obj, tag = tag);
    receive {
      case eDataDone: (rc: tRc) { r = rc; }
    }
    return r;
  }

  fun IndexPrepare(key: int) {
    send store, eIndexPrepare, (from = this, key = key, tag = rid);
    receive {
      case eIndexDone: (rc: tRc) { }
    }
  }

  fun IndexComplete(key: int, op: tIxOp, pool: int, epoch: int, removeKeys: set[int]) {
    send store, eIndexComplete, (from = this, key = key, op = op, tag = rid, pool = pool, epoch = epoch,
                                 writer = rid, removeKeys = removeKeys);
    receive {
      case eIndexDone: (rc: tRc) { }
    }
  }

  fun MpIndexAdd(key: int) {
    send store, eMpIndexAdd, (from = this, key = key);
    receive {
      case eIndexDone: (rc: tRc) { }
    }
  }

  fun MpIndexDel(key: int) {
    send store, eMpIndexDel, (from = this, key = key);
    receive {
      case eIndexDone: (rc: tRc) { }
    }
  }

  fun TryLock(u: int): (rc: tRc, ver: int) {
    var r: (rc: tRc, ver: int);
    send store, eTryLock, (from = this, upload = u, rid = rid);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { r = x; }
    }
    return r;
  }

  fun Unlock(u: int) {
    send store, eUnlock, (from = this, upload = u, rid = rid);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { }
    }
  }

  fun IsLocked(u: int): bool {
    var r: bool;
    send store, eIsLocked, (from = this, upload = u, rid = rid);
    receive {
      case eLocked: (x: bool) { r = x; }
    }
    return r;
  }

  fun GetAttrs(u: int): (rc: tRc, ver: int) {
    var r: (rc: tRc, ver: int);
    send store, eGetAttrs, (from = this, upload = u);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { r = x; }
    }
    return r;
  }

  fun ListParts(u: int): (rc: tRc, parts: map[int, tPart]) {
    var r: (rc: tRc, parts: map[int, tPart]);
    send store, eListParts, (from = this, upload = u);
    receive {
      case eParts: (x: (rc: tRc, parts: map[int, tPart])) { r = x; }
    }
    return r;
  }

  fun PartUpdate(u: int, num: int, part: tPart): tRc {
    var r: tRc;
    send store, ePartUpdate, (from = this, upload = u, num = num, part = part);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { r = x.rc; }
    }
    return r;
  }

  fun MetaDelete(u: int, checkVer: int, removeKeys: set[int]): tRc {
    var r: tRc;
    send store, eMetaDelete, (from = this, upload = u, checkVer = checkVer, removeKeys = removeKeys);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { r = x.rc; }
    }
    return r;
  }
}

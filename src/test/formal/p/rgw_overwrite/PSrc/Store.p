/*
 * The RADOS state. Each handler is one atomic op.
 *
 * - Each key's head object: a replace guarded by cmpxattr on the ID tag
 *   that was read, or an exclusive create; a removal (cls_rgw_remove_obj),
 *   guarded or not. Every head write or removal takes the PG's next
 *   version; a removal that finds no head answers the PG's last version,
 *   the floor the OSD sets on -ENOENT.
 * - Each key's bucket index entry, kept as cls_rgw keeps it: prepare adds
 *   a pending tag; complete (rgw_bucket_complete_op) drops it and applies
 *   the op unless its epoch is not newer than the entry's. The entry of a
 *   part head or meta object lives in the multipart namespace, removed
 *   through remove_objs.
 * - The data objects that manifests name, with their cls_refcount
 *   references: none recorded means the implicit one. GC queues chains
 *   under a tag, and later drops that tag's reference on each object
 *   (cls_refcount_put, implicit_ref); an object with none left is deleted.
 *   GC may run a queued chain before any op, in any order; at the end it
 *   runs every chain.
 * - Each upload's meta object: its parts
 *   (cls_rgw_mp_upload_part_info_update, which bumps the cls_version),
 *   and the completion lock, which lapses once its holder is dead.
 */
machine Store {
  var cfg: tCfg;
  var heads: map[int, tHead];
  var epoch: int;
  var ixs: map[int, tIx];
  var mpIndex: set[int];
  var live: set[int];
  var gone: set[int];
  var refs: map[int, set[int]];
  var retired: map[int, set[int]];
  var gcTags: seq[int];
  var gcObjs: seq[set[int]];
  var metas: set[int];
  var metaVer: map[int, int];
  var parts: map[int, map[int, tPart]];
  var lockOwner: map[int, int];
  var dead: set[int];

  start state Serve {
    entry (p: (cfg: tCfg, objects: set[int], uploads: set[int])) {
      var k: int;
      var u: int;
      var num: int;
      var h: tHead;
      var ps: map[int, tPart];
      cfg = p.cfg;
      k = 1;
      while (k <= 2) {
        heads[k] = default(tHead);
        ixs[k] = default(tIx);
        k = k + 1;
      }
      // an object at each of these keys, PUT earlier
      foreach (k in p.objects) {
        epoch = epoch + 1;
        h = (present = true, tag = OLDWRITER(k), tailTag = OLDWRITER(k), manifest = default(set[int]),
             writer = OLDWRITER(k), etag = OLDWRITER(k), upload = 0);
        h.manifest += (OLDTAIL(k));
        heads[k] = h;
        live += (OLDTAIL(k));
        ixs[k] = (present = true, listed = true, writer = OLDWRITER(k), pool = 1, epoch = epoch,
                  pending = default(set[int]));
        announce mHead, (key = k, writer = h.writer, manifest = h.manifest);
      }
      // uploads to key 1 with parts 1 and 2 uploaded once, at the base prefix
      foreach (u in p.uploads) {
        ps = default(map[int, tPart]);
        num = 1;
        while (num <= 2) {
          ps[num] = (prefix = u, etag = PARTETAG(u, num), past = default(set[int]));
          live += (OBJ(u, num));
          mpIndex += (OBJ(u, num));
          num = num + 1;
        }
        parts[u] = ps;
        metas += (u);
        metaVer[u] = 1;
        lockOwner[u] = 0;
        mpIndex += (METAKEY(u));
      }
    }

    on eReadHead do (p: (from: machine, key: int)) {
      MaybeGc();
      send p.from, eHeadRead, heads[p.key];
    }

    on eHeadWrite do (w: (from: machine, key: int, guard: bool, expectTag: int, excl: bool, head: tHead)) {
      var h: tHead;
      MaybeGc();
      h = heads[w.key];
      if (w.guard && !h.present) {
        send w.from, eHeadWritten, (rc = ENOENT, epoch = 0);
        return;
      }
      if (w.guard && h.tag != w.expectTag) {
        send w.from, eHeadWritten, (rc = ECANCELED, epoch = 0);
        return;
      }
      if (w.excl && h.present) {
        send w.from, eHeadWritten, (rc = EEXIST, epoch = 0);
        return;
      }
      heads[w.key] = w.head;
      epoch = epoch + 1;
      announce mHead, (key = w.key, writer = w.head.writer, manifest = w.head.manifest);
      send w.from, eHeadWritten, (rc = OK, epoch = epoch);
    }

    on eHeadRemove do (w: (from: machine, key: int, guard: bool, expectTag: int)) {
      var h: tHead;
      MaybeGc();
      h = heads[w.key];
      if (!h.present) {
        send w.from, eHeadWritten, (rc = ENOENT, epoch = epoch);
        return;
      }
      if (w.guard && h.tag != w.expectTag) {
        send w.from, eHeadWritten, (rc = ECANCELED, epoch = 0);
        return;
      }
      heads[w.key] = default(tHead);
      epoch = epoch + 1;
      announce mHead, (key = w.key, writer = 0, manifest = default(set[int]));
      send w.from, eHeadWritten, (rc = OK, epoch = epoch);
    }

    on eWriteData do (p: (from: machine, objs: set[int])) {
      var o: int;
      MaybeGc();
      foreach (o in p.objs) {
        live += (o);
      }
      send p.from, eDataDone, OK;
    }

    on eCreateExcl do (p: (from: machine, obj: int)) {
      MaybeGc();
      if (p.obj in live) {
        send p.from, eDataDone, EEXIST;
        return;
      }
      live += (p.obj);
      if (p.obj in gone) {
        gone -= (p.obj);
        announce mCreated, p.obj;
      }
      send p.from, eDataDone, OK;
    }

    on eDeleteInline do (p: (from: machine, objs: set[int])) {
      var o: int;
      MaybeGc();
      foreach (o in p.objs) {
        Remove(o);
      }
      send p.from, eDataDone, OK;
    }

    on eSendGc do (p: (from: machine, tag: int, objs: set[int])) {
      MaybeGc();
      gcTags += (sizeof(gcTags), p.tag);
      gcObjs += (sizeof(gcObjs), p.objs);
      send p.from, eDataDone, OK;
    }

    // cls_refcount_get with implicit_ref: fails on a missing object
    on eRefGet do (p: (from: machine, obj: int, tag: int)) {
      var r: set[int];
      MaybeGc();
      if (!(p.obj in live)) {
        send p.from, eDataDone, ENOENT;
        return;
      }
      r = RefsOf(p.obj);
      r += (p.tag);
      refs[p.obj] = r;
      send p.from, eDataDone, OK;
    }

    on eRefPut do (p: (from: machine, obj: int, tag: int)) {
      var rc: tRc;
      MaybeGc();
      rc = Put(p.obj, p.tag);
      send p.from, eDataDone, rc;
    }

    // rgw_bucket_prepare_op
    on eIndexPrepare do (p: (from: machine, key: int, tag: int)) {
      var e: tIx;
      MaybeGc();
      e = ixs[p.key];
      if (!e.present) {
        e = (present = true, listed = false, writer = 0, pool = -1, epoch = 0, pending = default(set[int]));
      }
      e.pending += (p.tag);
      ixs[p.key] = e;
      send p.from, eIndexDone, OK;
    }

    // rgw_bucket_complete_op
    on eIndexComplete do (c: (from: machine, key: int, op: tIxOp, tag: int, pool: int, epoch: int,
                              writer: int, removeKeys: set[int])) {
      var e: tIx;
      var op: tIxOp;
      var k: int;
      MaybeGc();
      e = ixs[c.key];
      if (!e.present || !(c.tag in e.pending)) {
        send c.from, eIndexDone, EINVAL;
        return;
      }
      e.pending -= (c.tag);
      op = c.op;
      if (op != IX_CANCEL && c.pool == e.pool && c.epoch != 0 && c.epoch <= e.epoch) {
        op = IX_CANCEL;  // "skipping request, old epoch"
      }
      // entry.ver = op.ver - on main for a cancel too, whose ver is
      // {-1, 0}, or the stale op's
      if (op != IX_CANCEL || !cfg.cancelKeepsVer) {
        e.pool = c.pool;
        e.epoch = c.epoch;
      }
      if (op == IX_CANCEL) {
        if (!e.listed && sizeof(e.pending) == 0) {
          e.present = false;
        }
      } else if (op == IX_DEL) {
        if (sizeof(e.pending) == 0) {
          e.present = false;
        } else {
          e.listed = false;
        }
      } else {
        e.listed = true;
        e.writer = c.writer;
      }
      ixs[c.key] = e;
      if (op != IX_CANCEL || cfg.cancelRemovesObjs) {
        foreach (k in c.removeKeys) {
          mpIndex -= (k);
        }
      }
      send c.from, eIndexDone, OK;
    }

    on eMpIndexAdd do (p: (from: machine, key: int)) {
      MaybeGc();
      mpIndex += (p.key);
      send p.from, eIndexDone, OK;
    }

    on eMpIndexDel do (p: (from: machine, key: int)) {
      MaybeGc();
      mpIndex -= (p.key);
      send p.from, eIndexDone, OK;
    }

    on eTryLock do (p: (from: machine, upload: int, rid: int)) {
      var owner: int;
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      owner = lockOwner[p.upload];
      if (owner != 0 && owner != p.rid && !(owner in dead) && (cfg.lockHeld || $)) {
        send p.from, eMetaRc, (rc = EBUSY, ver = 0);
        return;
      }
      lockOwner[p.upload] = p.rid;
      send p.from, eMetaRc, (rc = OK, ver = 0);
    }

    on eUnlock do (p: (from: machine, upload: int, rid: int)) {
      MaybeGc();
      if (p.upload in metas && lockOwner[p.upload] == p.rid) {
        lockOwner[p.upload] = 0;
      }
      send p.from, eMetaRc, (rc = OK, ver = 0);
    }

    on eIsLocked do (p: (from: machine, upload: int, rid: int)) {
      MaybeGc();
      send p.from, eLocked, (p.upload in metas && lockOwner[p.upload] == p.rid);
    }

    on eGetAttrs do (p: (from: machine, upload: int)) {
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      send p.from, eMetaRc, (rc = OK, ver = metaVer[p.upload]);
    }

    on eListParts do (p: (from: machine, upload: int)) {
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eParts, (rc = ENOENT, parts = default(map[int, tPart]));
        return;
      }
      send p.from, eParts, (rc = OK, parts = parts[p.upload]);
    }

    // assert_exists + cls_rgw_mp_upload_part_info_update + cls_version_inc
    on ePartUpdate do (p: (from: machine, upload: int, num: int, part: tPart)) {
      var info: tPart;
      var stored: tPart;
      var ps: map[int, tPart];
      var x: int;
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      info = p.part;
      ps = parts[p.upload];
      if (p.num in ps) {
        // carry the stored part's prefixes forward
        stored = ps[p.num];
        info.past += (stored.prefix);
        foreach (x in stored.past) {
          info.past += (x);
        }
      }
      if (info.prefix in info.past) {
        send p.from, eMetaRc, (rc = EEXIST, ver = 0);
        return;
      }
      ps[p.num] = info;
      parts[p.upload] = ps;
      metaVer[p.upload] = metaVer[p.upload] + 1;
      send p.from, eMetaRc, (rc = OK, ver = 0);
    }

    // the meta object's delete_obj, with cls_version_check if checkVer >= 0
    on eMetaDelete do (p: (from: machine, upload: int, checkVer: int, removeKeys: set[int])) {
      var k: int;
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      if (p.checkVer >= 0 && metaVer[p.upload] != p.checkVer) {
        // the index transaction is canceled; the cancel applies remove_objs
        if (cfg.cancelRemovesObjs) {
          foreach (k in p.removeKeys) {
            mpIndex -= (k);
          }
        }
        send p.from, eMetaRc, (rc = ECANCELED, ver = 0);
        return;
      }
      metas -= (p.upload);
      lockOwner[p.upload] = 0;
      mpIndex -= (METAKEY(p.upload));
      foreach (k in p.removeKeys) {
        mpIndex -= (k);
      }
      send p.from, eMetaRc, (rc = OK, ver = 0);
    }

    on eCrashed do (rid: int) {
      MaybeGc();
      dead += (rid);
    }

    // radosgw-admin gc process --include-all, then the specs look
    on eQuiesce do (from: machine) {
      var k: int;
      var u: int;
      var num: int;
      var pp: int;
      var pt: tPart;
      var o: int;
      var referenced: set[int];
      var validKeys: set[int];
      while (sizeof(gcTags) > 0) {
        GcEntry(0);
      }
      foreach (k in keys(heads)) {
        if (heads[k].present) {
          foreach (o in heads[k].manifest) {
            referenced += (o);
          }
        }
      }
      // a live upload's parts, current and past, and their index entries
      foreach (u in metas) {
        validKeys += (METAKEY(u));
        foreach (num in keys(parts[u])) {
          pt = parts[u][num];
          referenced += (OBJ(pt.prefix, num));
          validKeys += (OBJ(pt.prefix, num));
          foreach (pp in pt.past) {
            referenced += (OBJ(pp, num));
            validKeys += (OBJ(pp, num));
          }
        }
      }
      announce mFinal, (heads = heads, ixs = ixs, live = live, referenced = referenced,
                        mpIndex = mpIndex, validKeys = validKeys);
      send from, eQuiesced;
    }
  }

  // an object's cls_refcount references: the implicit one if none recorded
  fun RefsOf(o: int): set[int] {
    var r: set[int];
    if (o in refs) {
      return refs[o];
    }
    r += (WILD());
    return r;
  }

  // cls_refcount_put with implicit_ref
  fun Put(o: int, tag: int): tRc {
    var r: set[int];
    var ret: set[int];
    var found: int;
    if (!(o in live)) {
      return ENOENT;
    }
    r = RefsOf(o);
    if (o in retired) {
      ret = retired[o];
    }
    if (sizeof(r) == 0) {
      return EINVAL;
    }
    if (tag in r) {
      found = tag;
    } else if (WILD() in r) {
      found = WILD();
    } else {
      return OK;
    }
    if (tag in ret) {
      return OK;
    }
    ret += (tag);
    r -= (found);
    if (sizeof(r) == 0) {
      Remove(o);
      return OK;
    }
    refs[o] = r;
    retired[o] = ret;
    return OK;
  }

  fun Remove(o: int) {
    if (!(o in live)) {
      return;
    }
    live -= (o);
    if (o in refs) {
      refs -= (o);
    }
    if (o in retired) {
      retired -= (o);
    }
    gone += (o);
    announce mDeleted, o;
  }

  // RGWGC::process for one queued chain
  fun GcEntry(i: int) {
    var tag: int;
    var objs: set[int];
    var o: int;
    var rc: tRc;
    tag = gcTags[i];
    objs = gcObjs[i];
    gcTags -= (i);
    gcObjs -= (i);
    foreach (o in objs) {
      rc = Put(o, tag);
    }
  }

  // GC may run queued chains, in any order, before any op
  fun MaybeGc() {
    while (sizeof(gcTags) > 0 && $) {
      GcEntry(choose(sizeof(gcTags)));
    }
  }
}

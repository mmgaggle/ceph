// the code on main
fun Main(): tCfg {
  return (idTagGuard = true, cancelRemovesObjs = true, metaVersionCheck = true,
          historySkipsProcessed = true, abortTakesLock = true, replayAnswersEtag = true,
          copyTakesRefs = true, reshardLogs = true, reshardCheckExisting = true, oldShardsBlocked = true,
          cancelKeepsVer = false, lcTakesLock = false, loserGcsParts = false, gcSparesHead = false,
          deleteGuard = false, copyLoserDropsRefs = false, copySelfGuardsSource = false,
          ixFailKeepsWrite = false,
          completeMayCrash = false, metaDeleteMayFail = false, ixCompleteMayFail = false,
          lockHeld = true, writersPrompt = true);
}

fun Req(kind: tKind, key: int, src: int, upload: int): tSpec {
  return (kind = kind, key = key, src = src, upload = upload, num = 0, etag = 0, list = default(map[int, int]));
}
fun Put(key: int): tSpec { return Req(R_PUT, key, 0, 0); }
fun Del(key: int): tSpec { return Req(R_DELETE, key, 0, 0); }
fun Copy(src: int, dst: int): tSpec { return Req(R_COPY, dst, src, 0); }
fun List(): tSpec { return Req(R_LIST, 0, 0, 0); }
fun Dedup(src: int, tgt: int): tSpec { return Req(R_DEDUP, tgt, src, 0); }
fun Reshard(): tSpec { return Req(R_RESHARD, 0, 0, 0); }
fun AbortMpu(u: int): tSpec { return Req(R_ABORT, MPKEY(), 0, u); }
fun LcAbortMpu(u: int): tSpec { return Req(R_LC_ABORT, MPKEY(), 0, u); }
// complete upload u with the ETags its parts were first uploaded with
fun Complete(u: int): tSpec {
  var l: map[int, int];
  l[1] = PARTETAG(u, 1);
  l[2] = PARTETAG(u, 2);
  return (kind = R_COMPLETE, key = MPKEY(), src = 0, upload = u, num = 0, etag = 0, list = l);
}
fun Reupload(u: int, num: int, etag: int): tSpec {
  return (kind = R_UPLOAD_PART, key = MPKEY(), src = 0, upload = u, num = num, etag = etag,
          list = default(map[int, int]));
}
fun One(a: tSpec): seq[tSpec] {
  var s: seq[tSpec];
  s += (0, a);
  return s;
}
fun Two(a: tSpec, b: tSpec): seq[tSpec] {
  var s: seq[tSpec];
  s += (0, a);
  s += (1, b);
  return s;
}
fun Three(a: tSpec, b: tSpec, c: tSpec): seq[tSpec] {
  var s: seq[tSpec];
  s += (0, a);
  s += (1, b);
  s += (2, c);
  return s;
}

enum tScenario {
  SC_PUTS,              // three PutObjects over an existing object
  SC_PUT_VS_COMPLETE,   // a PutObject and a completion over it
  SC_COMPLETES,         // two uploads' completions over it
  SC_SAME_COMPLETES,    // three concurrent completions of one upload
  SC_REUPLOAD,          // a completion, and a re-upload of part 1 (same or other bytes)
  SC_ABORT,             // a completion and an AbortMultipartUpload
  SC_LC_ABORT,          // a completion and lifecycle's abort of the upload
  SC_RETRY,             // a completion, then the client retries it
  SC_THEN_ABORT,        // a completion, then an abort of the upload
  SC_PUT_THEN_RETRY,    // a completion, then a PutObject, then a retry of the completion
  SC_DEL_VS_PUT,        // a DeleteObject and a PutObject on the key
  SC_DELS_AND_PUT,      // two DeleteObjects and a PutObject on the key
  SC_DEL_VS_COMPLETE,   // a DeleteObject and a completion on the key
  SC_COPY_VS_PUT_SRC,   // a copy of key 1 to key 2, and a PutObject over key 1
  SC_COPY_VS_DEL_SRC,   // a copy of key 1 to key 2, and a DeleteObject of key 1
  SC_COPY_VS_PUT_DST,   // a copy of key 1 to key 2 and a PutObject over key 2; then key 1 deleted
  SC_COPY_THEN_DELETES, // a copy of key 1 to key 2; then both keys deleted at once
  SC_COPY_SELF_VS_PUT,  // a copy of key 1 onto itself, and a PutObject over key 1
  SC_COPY_MPU,          // a completion; a copy to key 2 and a PutObject over key 1; key 2 deleted
  SC_CRASH_COPY_RETRY,  // a completion; a copy to key 2; a retry of the completion; key 2 deleted
  SC_PUT_ONE,           // one PutObject over an existing object
  SC_COPY_ONE,          // a copy of key 1 to key 2; then key 1 deleted
  SC_LIST_VS_PUT,       // a PutObject and a bucket listing
  SC_LIST_VS_DEL,       // a DeleteObject and a bucket listing
  SC_LIST_VS_COMPLETE,  // a completion and a bucket listing
  SC_DEDUP_THEN_DELETES, // keys 1 and 2 hold the same bytes: dedup of 2 onto 1; then both deleted
  SC_DEDUP_VS_PUT_TGT,  // dedup of 2 onto 1 and a PutObject over key 2; then key 1 deleted
  SC_DEDUP_VS_DEL_TGT,  // dedup of 2 onto 1 and a DeleteObject of key 2; then key 1 deleted
  SC_DEDUP_VS_PUT_SRC,  // dedup of 2 onto 1 and a PutObject over key 1; then key 2 deleted
  SC_DEDUP_VS_COPY_SELF, // dedup of 2 onto 1 and a copy of key 2 onto itself; then key 1 deleted
  SC_RESHARD_VS_PUTS,   // a reshard, and two PutObjects over key 1
  SC_RESHARD_VS_DEL,    // a reshard, a DeleteObject and a PutObject on key 1
  SC_RESHARD_VS_MPU     // a reshard, a completion, and a re-upload of part 1
}

// Key 1 starts with an object, and key 2 too in the copy scenarios.
// Uploads 1 and 2 (to key 1) have parts 1 and 2 uploaded.
machine Scenario {
  start state Init {
    entry (p: (cfg: tCfg, sc: tScenario)) {
      var script: seq[seq[tSpec]];
      var objects: set[int];
      var uploads: set[int];
      var etag: int;
      var twins: bool;
      objects += (1);
      if (p.sc == SC_PUTS) {
        script += (0, Three(Put(1), Put(1), Put(1)));
      } else if (p.sc == SC_PUT_VS_COMPLETE) {
        uploads += (1);
        script += (0, Two(Put(1), Complete(1)));
      } else if (p.sc == SC_COMPLETES) {
        uploads += (1);
        uploads += (2);
        script += (0, Two(Complete(1), Complete(2)));
      } else if (p.sc == SC_SAME_COMPLETES) {
        uploads += (1);
        script += (0, Three(Complete(1), Complete(1), Complete(1)));
      } else if (p.sc == SC_REUPLOAD) {
        uploads += (1);
        etag = PARTETAG(1, 1);
        if ($) {
          etag = 19;
        }
        script += (0, Two(Complete(1), Reupload(1, 1, etag)));
      } else if (p.sc == SC_ABORT) {
        uploads += (1);
        script += (0, Two(Complete(1), AbortMpu(1)));
      } else if (p.sc == SC_LC_ABORT) {
        uploads += (1);
        script += (0, Two(Complete(1), LcAbortMpu(1)));
      } else if (p.sc == SC_RETRY) {
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(Complete(1)));
      } else if (p.sc == SC_THEN_ABORT) {
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(AbortMpu(1)));
      } else if (p.sc == SC_PUT_THEN_RETRY) {
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(Put(1)));
        script += (2, One(Complete(1)));
      } else if (p.sc == SC_DEL_VS_PUT) {
        script += (0, Two(Del(1), Put(1)));
      } else if (p.sc == SC_DELS_AND_PUT) {
        script += (0, Three(Del(1), Del(1), Put(1)));
      } else if (p.sc == SC_DEL_VS_COMPLETE) {
        uploads += (1);
        script += (0, Two(Del(1), Complete(1)));
      } else if (p.sc == SC_COPY_VS_PUT_SRC) {
        objects += (2);
        script += (0, Two(Copy(1, 2), Put(1)));
      } else if (p.sc == SC_COPY_VS_DEL_SRC) {
        objects += (2);
        script += (0, Two(Copy(1, 2), Del(1)));
      } else if (p.sc == SC_COPY_VS_PUT_DST) {
        objects += (2);
        script += (0, Two(Copy(1, 2), Put(2)));
        script += (1, One(Del(1)));
      } else if (p.sc == SC_COPY_THEN_DELETES) {
        objects += (2);
        script += (0, One(Copy(1, 2)));
        script += (1, Two(Del(1), Del(2)));
      } else if (p.sc == SC_COPY_SELF_VS_PUT) {
        script += (0, Two(Copy(1, 1), Put(1)));
      } else if (p.sc == SC_COPY_MPU) {
        objects += (2);
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, Two(Copy(1, 2), Put(1)));
        script += (2, One(Del(2)));
      } else if (p.sc == SC_CRASH_COPY_RETRY) {
        objects += (2);
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(Copy(1, 2)));
        script += (2, One(Complete(1)));
        script += (3, One(Del(2)));
      } else if (p.sc == SC_PUT_ONE) {
        script += (0, One(Put(1)));
      } else if (p.sc == SC_COPY_ONE) {
        objects += (2);
        script += (0, One(Copy(1, 2)));
        script += (1, One(Del(1)));
      } else if (p.sc == SC_LIST_VS_PUT) {
        script += (0, Two(Put(1), List()));
      } else if (p.sc == SC_LIST_VS_DEL) {
        script += (0, Two(Del(1), List()));
      } else if (p.sc == SC_LIST_VS_COMPLETE) {
        uploads += (1);
        script += (0, Two(Complete(1), List()));
      } else if (p.sc == SC_RESHARD_VS_PUTS) {
        script += (0, Three(Reshard(), Put(1), Put(1)));
      } else if (p.sc == SC_RESHARD_VS_DEL) {
        script += (0, Three(Reshard(), Del(1), Put(1)));
      } else if (p.sc == SC_RESHARD_VS_MPU) {
        uploads += (1);
        script += (0, Three(Reshard(), Complete(1), Reupload(1, 1, PARTETAG(1, 1))));
      } else {
        objects += (2);
        twins = true;
        if (p.sc == SC_DEDUP_THEN_DELETES) {
          script += (0, One(Dedup(1, 2)));
          script += (1, Two(Del(1), Del(2)));
        } else if (p.sc == SC_DEDUP_VS_PUT_TGT) {
          script += (0, Two(Dedup(1, 2), Put(2)));
          script += (1, One(Del(1)));
        } else if (p.sc == SC_DEDUP_VS_DEL_TGT) {
          script += (0, Two(Dedup(1, 2), Del(2)));
          script += (1, One(Del(1)));
        } else if (p.sc == SC_DEDUP_VS_PUT_SRC) {
          script += (0, Two(Dedup(1, 2), Put(1)));
          script += (1, One(Del(2)));
        } else {
          script += (0, Two(Dedup(1, 2), Copy(2, 2)));
          script += (1, One(Del(1)));
        }
      }
      new Driver((cfg = p.cfg, objects = objects, twins = twins, uploads = uploads, script = script));
    }
  }
}

// PutObject over PutObject
machine TestPuts { start state Init { entry { new Scenario((cfg = Main(), sc = SC_PUTS)); } } }
machine TestPutsCancelKeepsVer {
  start state Init { entry { var c: tCfg; c = Main(); c.cancelKeepsVer = true; new Scenario((cfg = c, sc = SC_PUTS)); } }
}
machine TestPutsNoIdTagGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.idTagGuard = false; new Scenario((cfg = c, sc = SC_PUTS)); } }
}

// a completion over the key, racing PutObject or another completion
machine TestPutVsComplete { start state Init { entry { new Scenario((cfg = Main(), sc = SC_PUT_VS_COMPLETE)); } } }
machine TestPutVsCompleteLoserGc {
  start state Init { entry { var c: tCfg; c = Main(); c.loserGcsParts = true; new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestPutVsCompleteCancelSkipsRemoveObjs {
  start state Init { entry { var c: tCfg; c = Main(); c.loserGcsParts = true; c.cancelRemovesObjs = false; new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestCompletes { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COMPLETES)); } } }
machine TestCompletesLoserGc {
  start state Init { entry { var c: tCfg; c = Main(); c.loserGcsParts = true; new Scenario((cfg = c, sc = SC_COMPLETES)); } }
}

// one upload completed three times at once
machine TestSameCompletes { start state Init { entry { new Scenario((cfg = Main(), sc = SC_SAME_COMPLETES)); } } }
machine TestSameCompletesLockLapses {
  start state Init { entry { var c: tCfg; c = Main(); c.lockHeld = false; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestSameCompletesReplayNoEtag {
  start state Init { entry { var c: tCfg; c = Main(); c.replayAnswersEtag = false; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}

// a part re-uploaded during the completion
machine TestReupload { start state Init { entry { new Scenario((cfg = Main(), sc = SC_REUPLOAD)); } } }
machine TestReuploadNoMetaVersionCheck {
  start state Init { entry { var c: tCfg; c = Main(); c.metaVersionCheck = false; new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}
machine TestReuploadHistoryNoSkip {
  start state Init { entry { var c: tCfg; c = Main(); c.historySkipsProcessed = false; new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}

// an abort during the completion
machine TestAbort { start state Init { entry { new Scenario((cfg = Main(), sc = SC_ABORT)); } } }
machine TestAbortNoLock {
  start state Init { entry { var c: tCfg; c = Main(); c.abortTakesLock = false; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestAbortLockLapses {
  start state Init { entry { var c: tCfg; c = Main(); c.lockHeld = false; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestLcAbort { start state Init { entry { new Scenario((cfg = Main(), sc = SC_LC_ABORT)); } } }
machine TestLcAbortTakesLock {
  start state Init { entry { var c: tCfg; c = Main(); c.lcTakesLock = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}

// a completion that leaves its meta object behind, then a retry or an abort
machine TestCrashThenRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestMetaDeleteFailsThenRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestMetaDeleteFailsThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestCrashThenRetrySparesHead {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; c.gcSparesHead = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestCrashThenAbortSparesHead {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; c.gcSparesHead = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestCrashPutThenRetrySparesHead {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; c.gcSparesHead = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}

// DeleteObject
machine TestDelVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEL_VS_PUT)); } } }
machine TestDelVsPutGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; new Scenario((cfg = c, sc = SC_DEL_VS_PUT)); } }
}
machine TestDelsAndPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DELS_AND_PUT)); } } }
machine TestDelsAndPutCancelKeepsVer {
  start state Init { entry { var c: tCfg; c = Main(); c.cancelKeepsVer = true; new Scenario((cfg = c, sc = SC_DELS_AND_PUT)); } }
}
machine TestDelVsComplete { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEL_VS_COMPLETE)); } } }
machine TestDelVsCompleteGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; c.loserGcsParts = true; new Scenario((cfg = c, sc = SC_DEL_VS_COMPLETE)); } }
}

// CopyObject sharing the source's tail
machine TestCopyVsPutSrc { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_VS_PUT_SRC)); } } }
machine TestCopyVsPutSrcNoRefs {
  start state Init { entry { var c: tCfg; c = Main(); c.copyTakesRefs = false; new Scenario((cfg = c, sc = SC_COPY_VS_PUT_SRC)); } }
}
machine TestCopyVsDelSrc { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_VS_DEL_SRC)); } } }
machine TestCopyVsPutDst { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_VS_PUT_DST)); } } }
machine TestCopyVsPutDstDropRefs {
  start state Init { entry { var c: tCfg; c = Main(); c.copyLoserDropsRefs = true; new Scenario((cfg = c, sc = SC_COPY_VS_PUT_DST)); } }
}
machine TestCopyThenDeletes { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_THEN_DELETES)); } } }
machine TestCopySelfVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_SELF_VS_PUT)); } } }
machine TestCopySelfVsPutGuarded {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; new Scenario((cfg = c, sc = SC_COPY_SELF_VS_PUT)); } }
}
machine TestCopyMpu { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_MPU)); } } }
machine TestCrashCopyRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_CRASH_COPY_RETRY)); } }
}

// an index completion that fails after the head write
machine TestIxFailPut {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_ONE)); } }
}
machine TestIxFailCopy {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_ONE)); } }
}
machine TestIxFailRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestIxKeepsWritePut {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; c.ixFailKeepsWrite = true; new Scenario((cfg = c, sc = SC_PUT_ONE)); } }
}
machine TestIxKeepsWriteCopy {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; c.ixFailKeepsWrite = true; new Scenario((cfg = c, sc = SC_COPY_ONE)); } }
}
machine TestIxKeepsWriteRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; c.ixFailKeepsWrite = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}

// a bucket listing's repair
machine TestListVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_LIST_VS_PUT)); } } }
machine TestListVsDel { start state Init { entry { new Scenario((cfg = Main(), sc = SC_LIST_VS_DEL)); } } }
machine TestListVsComplete { start state Init { entry { new Scenario((cfg = Main(), sc = SC_LIST_VS_COMPLETE)); } } }
machine TestListVsPutSlowWriter {
  start state Init { entry { var c: tCfg; c = Main(); c.writersPrompt = false; new Scenario((cfg = c, sc = SC_LIST_VS_PUT)); } }
}

// dedup of key 2's object onto key 1's, which holds the same bytes
machine TestDedupThenDeletes { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_THEN_DELETES)); } } }
machine TestDedupVsPutTgt { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_VS_PUT_TGT)); } } }
machine TestDedupVsDelTgt { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_VS_DEL_TGT)); } } }
machine TestDedupVsDelTgtGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; new Scenario((cfg = c, sc = SC_DEDUP_VS_DEL_TGT)); } }
}
machine TestDedupVsPutSrc { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_VS_PUT_SRC)); } } }
machine TestDedupVsCopySelf { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_VS_COPY_SELF)); } } }
machine TestDedupVsCopySelfGuarded {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; new Scenario((cfg = c, sc = SC_DEDUP_VS_COPY_SELF)); } }
}

// a bucket reshard racing writes
machine TestReshardVsPuts { start state Init { entry { new Scenario((cfg = Main(), sc = SC_RESHARD_VS_PUTS)); } } }
machine TestReshardVsDel { start state Init { entry { new Scenario((cfg = Main(), sc = SC_RESHARD_VS_DEL)); } } }
machine TestReshardVsMpu { start state Init { entry { new Scenario((cfg = Main(), sc = SC_RESHARD_VS_MPU)); } } }
machine TestReshardNoLog {
  start state Init { entry { var c: tCfg; c = Main(); c.reshardLogs = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestReshardNoCheckExisting {
  start state Init { entry { var c: tCfg; c = Main(); c.reshardCheckExisting = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestReshardOldShardsOpen {
  start state Init { entry { var c: tCfg; c = Main(); c.oldShardsBlocked = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}

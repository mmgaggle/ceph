module System = { Scenario, Driver, Store, Rgw };

// PutObject over PutObject
test tcPutsSafe [main=TestPuts]:
  assert HeadIntact, NoOrphans, AllAnswered, BucketStats in (union System, { TestPuts });
test tcPutsIndex [main=TestPuts]:
  assert IndexMatchesHead in (union System, { TestPuts });
test tcPutsCancelKeepsVer [main=TestPutsCancelKeepsVer]:
  assert IndexMatchesHead in (union System, { TestPutsCancelKeepsVer });
test tcBugNoIdTagGuard [main=TestPutsNoIdTagGuard]:
  assert NoOrphans in (union System, { TestPutsNoIdTagGuard });

// a completion over the key, racing PutObject or another completion
test tcPutVsCompleteSafe [main=TestPutVsComplete]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestPutVsComplete });
test tcPutVsCompleteLeak [main=TestPutVsComplete]:
  assert NoOrphans in (union System, { TestPutVsComplete });
test tcPutVsCompleteLoserGc [main=TestPutVsCompleteLoserGc]:
  assert NoOrphans in (union System, { TestPutVsCompleteLoserGc });
test tcBugCancelSkipsRemoveObjs [main=TestPutVsCompleteCancelSkipsRemoveObjs]:
  assert NoOrphans in (union System, { TestPutVsCompleteCancelSkipsRemoveObjs });
test tcCompletesSafe [main=TestCompletes]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCompletes });
test tcCompletesLeak [main=TestCompletes]:
  assert NoOrphans in (union System, { TestCompletes });
test tcCompletesLoserGc [main=TestCompletesLoserGc]:
  assert NoOrphans in (union System, { TestCompletesLoserGc });

// a part re-uploaded during the completion
test tcReupload [main=TestReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestReupload });
test tcBugNoMetaVersionCheck [main=TestReuploadNoMetaVersionCheck]:
  assert NoOrphans in (union System, { TestReuploadNoMetaVersionCheck });
test tcBugHistoryNoSkip [main=TestReuploadHistoryNoSkip]:
  assert HeadIntact in (union System, { TestReuploadHistoryNoSkip });

// an abort during the completion
test tcAbortVsComplete [main=TestAbort]:
  assert HeadIntact, NoOrphans, AllAnswered, BucketStats in (union System, { TestAbort });
test tcBugAbortNoLock [main=TestAbortNoLock]:
  assert HeadIntact in (union System, { TestAbortNoLock });
test tcAssumeLockHeld [main=TestAbortLockLapses]:
  assert HeadIntact in (union System, { TestAbortLockLapses });
test tcLcAbortVsComplete [main=TestLcAbort]:
  assert HeadIntact in (union System, { TestLcAbort });
test tcLcAbortTakesLock [main=TestLcAbortTakesLock]:
  assert HeadIntact, NoOrphans, AllAnswered in (union System, { TestLcAbortTakesLock });

// one upload completed three times at once
test tcSameCompletes [main=TestSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestSameCompletes });
test tcBugNoLockRenewal [main=TestSameCompletesLockLapses]:
  assert HeadIntact in (union System, { TestSameCompletesLockLapses });
test tcBugReplayNoEtag [main=TestSameCompletesReplayNoEtag]:
  assert CompletionEtag in (union System, { TestSameCompletesReplayNoEtag });

// a completion that leaves its meta object behind, then a retry or an abort
test tcCrashThenRetry [main=TestCrashThenRetry]:
  assert HeadIntact in (union System, { TestCrashThenRetry });
test tcCrashThenAbort [main=TestCrashThenAbort]:
  assert HeadIntact in (union System, { TestCrashThenAbort });
test tcMetaDeleteFailsThenRetry [main=TestMetaDeleteFailsThenRetry]:
  assert HeadIntact in (union System, { TestMetaDeleteFailsThenRetry });
test tcMetaDeleteFailsThenAbort [main=TestMetaDeleteFailsThenAbort]:
  assert HeadIntact in (union System, { TestMetaDeleteFailsThenAbort });
test tcSparesHeadCrashRetry [main=TestCrashThenRetrySparesHead]:
  assert HeadIntact, AllAnswered in (union System, { TestCrashThenRetrySparesHead });
test tcSparesHeadCrashAbort [main=TestCrashThenAbortSparesHead]:
  assert HeadIntact, AllAnswered in (union System, { TestCrashThenAbortSparesHead });
test tcSparesHeadCrashPutRetry [main=TestCrashPutThenRetrySparesHead]:
  assert HeadIntact in (union System, { TestCrashPutThenRetrySparesHead });

// DeleteObject on a non-versioned bucket
test tcDelVsPutSafe [main=TestDelVsPut]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestDelVsPut });
test tcDelVsPutLeak [main=TestDelVsPut]:
  assert NoOrphans in (union System, { TestDelVsPut });
test tcDelVsPutGuard [main=TestDelVsPutGuard]:
  assert HeadIntact, NoOrphans, IndexMatchesHead in (union System, { TestDelVsPutGuard });
test tcDelsAndPutSafe [main=TestDelsAndPut]:
  assert HeadIntact, AllAnswered, BucketStats in (union System, { TestDelsAndPut });
test tcDelsAndPutIndex [main=TestDelsAndPut]:
  assert IndexMatchesHead in (union System, { TestDelsAndPut });
test tcDelsCancelKeepsVer [main=TestDelsAndPutCancelKeepsVer]:
  assert IndexMatchesHead in (union System, { TestDelsAndPutCancelKeepsVer });
test tcDelVsCompleteSafe [main=TestDelVsComplete]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestDelVsComplete });
test tcDelVsCompleteLeak [main=TestDelVsComplete]:
  assert NoOrphans in (union System, { TestDelVsComplete });
test tcDelVsCompleteGuard [main=TestDelVsCompleteGuard]:
  assert HeadIntact, NoOrphans in (union System, { TestDelVsCompleteGuard });

// CopyObject sharing the source's tail
test tcCopyVsPutSrc [main=TestCopyVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyVsPutSrc });
test tcBugCopyNoRefs [main=TestCopyVsPutSrcNoRefs]:
  assert HeadIntact in (union System, { TestCopyVsPutSrcNoRefs });
test tcCopyVsDelSrc [main=TestCopyVsDelSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyVsDelSrc });
test tcCopyVsPutDstSafe [main=TestCopyVsPutDst]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyVsPutDst });
test tcCopyVsPutDstLeak [main=TestCopyVsPutDst]:
  assert NoOrphans in (union System, { TestCopyVsPutDst });
test tcCopyVsPutDstDropRefs [main=TestCopyVsPutDstDropRefs]:
  assert NoOrphans in (union System, { TestCopyVsPutDstDropRefs });
test tcCopyThenDeletes [main=TestCopyThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyThenDeletes });
test tcCopySelfVsPutLoss [main=TestCopySelfVsPut]:
  assert HeadIntact in (union System, { TestCopySelfVsPut });
test tcCopySelfGuarded [main=TestCopySelfVsPutGuarded]:
  assert HeadIntact, NoOrphans, IndexMatchesHead in (union System, { TestCopySelfVsPutGuarded });
test tcCopyMpu [main=TestCopyMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyMpu });
test tcCrashCopyRetry [main=TestCrashCopyRetry]:
  assert HeadIntact in (union System, { TestCrashCopyRetry });

// an index completion that fails after the head write
test tcIxFailPutLoss [main=TestIxFailPut]:
  assert HeadIntact in (union System, { TestIxFailPut });
test tcIxFailPutIndex [main=TestIxFailPut]:
  assert IndexMatchesHead in (union System, { TestIxFailPut });
test tcIxFailCopyLoss [main=TestIxFailCopy]:
  assert HeadIntact in (union System, { TestIxFailCopy });
test tcIxFailRetryLoss [main=TestIxFailRetry]:
  assert HeadIntact in (union System, { TestIxFailRetry });
test tcIxKeepsWritePut [main=TestIxKeepsWritePut]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats in (union System, { TestIxKeepsWritePut });
test tcIxKeepsWriteCopy [main=TestIxKeepsWriteCopy]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats in (union System, { TestIxKeepsWriteCopy });
test tcIxKeepsWriteRetry [main=TestIxKeepsWriteRetry]:
  assert HeadIntact, IndexMatchesHead in (union System, { TestIxKeepsWriteRetry });

// a bucket listing's repair
test tcListVsPut [main=TestListVsPut]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestListVsPut });
test tcListVsDel [main=TestListVsDel]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestListVsDel });
test tcListVsComplete [main=TestListVsComplete]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestListVsComplete });
test tcAssumeWritersPrompt [main=TestListVsPutSlowWriter]:
  assert IndexMatchesHead in (union System, { TestListVsPutSlowWriter });

// dedup of key 2's object onto key 1's
test tcDedupThenDeletes [main=TestDedupThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, BucketStats, AllAnswered in (union System, { TestDedupThenDeletes });
test tcDedupVsPutTgtSafe [main=TestDedupVsPutTgt]:
  assert HeadIntact, IndexMatchesHead, AllAnswered in (union System, { TestDedupVsPutTgt });
test tcDedupVsPutTgtLeak [main=TestDedupVsPutTgt]:
  assert NoOrphans in (union System, { TestDedupVsPutTgt });
test tcDedupVsDelTgtLeak [main=TestDedupVsDelTgt]:
  assert NoOrphans in (union System, { TestDedupVsDelTgt });
test tcDedupVsDelTgtGuarded [main=TestDedupVsDelTgtGuard]:
  assert NoOrphans in (union System, { TestDedupVsDelTgtGuard });
test tcDedupVsPutSrc [main=TestDedupVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, BucketStats, AllAnswered in (union System, { TestDedupVsPutSrc });
test tcDedupVsCopySelfLoss [main=TestDedupVsCopySelf]:
  assert HeadIntact in (union System, { TestDedupVsCopySelf });
test tcDedupVsCopySelfGuarded [main=TestDedupVsCopySelfGuarded]:
  assert HeadIntact in (union System, { TestDedupVsCopySelfGuarded });

// a bucket reshard racing writes
test tcReshardVsPuts [main=TestReshardVsPuts]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestReshardVsPuts });
test tcReshardVsDel [main=TestReshardVsDel]:
  assert HeadIntact, IndexMatchesHead, BucketStats, AllAnswered in (union System, { TestReshardVsDel });
test tcReshardVsMpu [main=TestReshardVsMpu]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestReshardVsMpu });
test tcBugReshardNoLog [main=TestReshardNoLog]:
  assert IndexMatchesHead in (union System, { TestReshardNoLog });
test tcBugReshardNoCheckExisting [main=TestReshardNoCheckExisting]:
  assert BucketStats in (union System, { TestReshardNoCheckExisting });
test tcBugOldShardsOpen [main=TestReshardOldShardsOpen]:
  assert IndexMatchesHead in (union System, { TestReshardOldShardsOpen });

module System = { Scenario, Driver, Store, Rgw };

// PutObject over PutObject
test tcPutsSafe [main=TestPuts]:
  assert HeadIntact, NoOrphans, AllAnswered in (union System, { TestPuts });
test tcPutsIndex [main=TestPuts]:
  assert IndexMatchesHead in (union System, { TestPuts });
test tcPutsCancelKeepsVer [main=TestPutsCancelKeepsVer]:
  assert IndexMatchesHead in (union System, { TestPutsCancelKeepsVer });
test tcBugNoIdTagGuard [main=TestPutsNoIdTagGuard]:
  assert NoOrphans in (union System, { TestPutsNoIdTagGuard });

// a completion over the key, racing PutObject or another completion
test tcPutVsCompleteSafe [main=TestPutVsComplete]:
  assert HeadIntact, IndexMatchesHead, AllAnswered in (union System, { TestPutVsComplete });
test tcPutVsCompleteLeak [main=TestPutVsComplete]:
  assert NoOrphans in (union System, { TestPutVsComplete });
test tcPutVsCompleteLoserGc [main=TestPutVsCompleteLoserGc]:
  assert NoOrphans in (union System, { TestPutVsCompleteLoserGc });
test tcBugCancelSkipsRemoveObjs [main=TestPutVsCompleteCancelSkipsRemoveObjs]:
  assert NoOrphans in (union System, { TestPutVsCompleteCancelSkipsRemoveObjs });
test tcCompletesSafe [main=TestCompletes]:
  assert HeadIntact, IndexMatchesHead, AllAnswered in (union System, { TestCompletes });
test tcCompletesLeak [main=TestCompletes]:
  assert NoOrphans in (union System, { TestCompletes });
test tcCompletesLoserGc [main=TestCompletesLoserGc]:
  assert NoOrphans in (union System, { TestCompletesLoserGc });

// a part re-uploaded during the completion
test tcReupload [main=TestReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered in (union System, { TestReupload });
test tcBugNoMetaVersionCheck [main=TestReuploadNoMetaVersionCheck]:
  assert NoOrphans in (union System, { TestReuploadNoMetaVersionCheck });
test tcBugHistoryNoSkip [main=TestReuploadHistoryNoSkip]:
  assert HeadIntact in (union System, { TestReuploadHistoryNoSkip });

// an abort during the completion
test tcAbortVsComplete [main=TestAbort]:
  assert HeadIntact, NoOrphans, AllAnswered in (union System, { TestAbort });
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
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered in (union System, { TestSameCompletes });
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
  assert HeadIntact, IndexMatchesHead, AllAnswered in (union System, { TestDelVsPut });
test tcDelVsPutLeak [main=TestDelVsPut]:
  assert NoOrphans in (union System, { TestDelVsPut });
test tcDelVsPutGuard [main=TestDelVsPutGuard]:
  assert HeadIntact, NoOrphans, IndexMatchesHead in (union System, { TestDelVsPutGuard });
test tcDelsAndPutSafe [main=TestDelsAndPut]:
  assert HeadIntact, AllAnswered in (union System, { TestDelsAndPut });
test tcDelsAndPutIndex [main=TestDelsAndPut]:
  assert IndexMatchesHead in (union System, { TestDelsAndPut });
test tcDelsCancelKeepsVer [main=TestDelsAndPutCancelKeepsVer]:
  assert IndexMatchesHead in (union System, { TestDelsAndPutCancelKeepsVer });
test tcDelVsCompleteSafe [main=TestDelVsComplete]:
  assert HeadIntact, IndexMatchesHead, AllAnswered in (union System, { TestDelVsComplete });
test tcDelVsCompleteLeak [main=TestDelVsComplete]:
  assert NoOrphans in (union System, { TestDelVsComplete });
test tcDelVsCompleteGuard [main=TestDelVsCompleteGuard]:
  assert HeadIntact, NoOrphans in (union System, { TestDelVsCompleteGuard });

// CopyObject sharing the source's tail
test tcCopyVsPutSrc [main=TestCopyVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered in (union System, { TestCopyVsPutSrc });
test tcBugCopyNoRefs [main=TestCopyVsPutSrcNoRefs]:
  assert HeadIntact in (union System, { TestCopyVsPutSrcNoRefs });
test tcCopyVsDelSrc [main=TestCopyVsDelSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered in (union System, { TestCopyVsDelSrc });
test tcCopyVsPutDstSafe [main=TestCopyVsPutDst]:
  assert HeadIntact, IndexMatchesHead, AllAnswered in (union System, { TestCopyVsPutDst });
test tcCopyVsPutDstLeak [main=TestCopyVsPutDst]:
  assert NoOrphans in (union System, { TestCopyVsPutDst });
test tcCopyVsPutDstDropRefs [main=TestCopyVsPutDstDropRefs]:
  assert NoOrphans in (union System, { TestCopyVsPutDstDropRefs });
test tcCopyThenDeletes [main=TestCopyThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered in (union System, { TestCopyThenDeletes });
test tcCopySelfVsPutLoss [main=TestCopySelfVsPut]:
  assert HeadIntact in (union System, { TestCopySelfVsPut });
test tcCopySelfGuarded [main=TestCopySelfVsPutGuarded]:
  assert HeadIntact, NoOrphans, IndexMatchesHead in (union System, { TestCopySelfVsPutGuarded });
test tcCopyMpu [main=TestCopyMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered in (union System, { TestCopyMpu });
test tcCrashCopyRetry [main=TestCrashCopyRetry]:
  assert HeadIntact in (union System, { TestCrashCopyRetry });

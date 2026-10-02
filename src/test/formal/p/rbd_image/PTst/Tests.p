module System = { Driver, Store, Client };

// the exclusive lock
test tcTwoWriters [main=TestTwoWriters]:
  assert WritesFenced, AllAnswered in (union System, { TestTwoWriters });
test tcLostWatchFenced [main=TestWriterLosesWatch]:
  assert WritesFenced, AllAnswered in (union System, { TestWriterLosesWatch });
test tcLostWatchNoBlocklist [main=TestWriterLosesWatchNoBlocklist]:
  assert WritesFenced in (union System, { TestWriterLosesWatchNoBlocklist });
test tcThreeWriters [main=TestThreeWriters]:
  assert WritesFenced, AllAnswered in (union System, { TestThreeWriters });

// snapshots
test tcCreateViaOwnerSafe [main=TestSnapCreateViaOwner]:
  assert WritesFenced, SnapImmutable, NoLeakedSnapIds, AllAnswered in (union System, { TestSnapCreateViaOwner });
test tcCreateViaOwnerAnswer [main=TestSnapCreateViaOwner]:
  assert CreateAnswered in (union System, { TestSnapCreateViaOwner });
test tcCreateViaOwnerLostWatchSafe [main=TestSnapCreateViaOwnerDrop]:
  assert WritesFenced, NoLeakedSnapIds, AllAnswered in (union System, { TestSnapCreateViaOwnerDrop });
test tcCreateViaOwnerLostWatchAnswer [main=TestSnapCreateViaOwnerDrop]:
  assert CreateAnswered in (union System, { TestSnapCreateViaOwnerDrop });
test tcOwnerLostWatchStale [main=TestOwnerLostWatch]:
  assert SnapImmutable in (union System, { TestOwnerLostWatch });
test tcOwnerLostWatchSafe [main=TestOwnerLostWatch]:
  assert WritesFenced, NoLeakedSnapIds, AllAnswered in (union System, { TestOwnerLostWatch });
test tcOwnerLostWatchRefresh [main=TestOwnerLostWatchRefresh]:
  assert WritesFenced, SnapImmutable, NoLeakedSnapIds, AllAnswered in (union System, { TestOwnerLostWatchRefresh });
test tcSnapCreateNoLock [main=TestSnapCreateNoLock]:
  assert SnapImmutable in (union System, { TestSnapCreateNoLock });
test tcTwoCreatesNoLock [main=TestTwoCreatesNoLock]:
  assert NoLeakedSnapIds in (union System, { TestTwoCreatesNoLock });
test tcCreateOwnerChangeAnswer [main=TestSnapCreateOwnerChange]:
  assert CreateAnswered in (union System, { TestSnapCreateOwnerChange });
test tcCreateOwnerChangeSafe [main=TestSnapCreateOwnerChange]:
  assert WritesFenced, SnapImmutable, NoLeakedSnapIds, AllAnswered in (union System, { TestSnapCreateOwnerChange });
test tcSnapRemoveVsCreate [main=TestSnapRemoveVsCreate]:
  assert WritesFenced, SnapImmutable, ChildHasParent, NoLeakedSnapIds, AllAnswered in (union System, { TestSnapRemoveVsCreate });

// layering
test tcCloneV2VsRemove [main=TestCloneV2VsRemove]:
  assert ChildHasParent, AllAnswered in (union System, { TestCloneV2VsRemove });
test tcCloneV1VsUnprotectOnly [main=TestCloneV1VsUnprotect]:
  assert ChildHasParent, AllAnswered in (union System, { TestCloneV1VsUnprotect });
test tcCloneV1NoRecheck [main=TestCloneV1NoRecheck]:
  assert ChildHasParent in (union System, { TestCloneV1NoRecheck });
test tcCloneV1VsProtectRace [main=TestCloneV1VsUnprotectProtect]:
  assert ChildHasParent in (union System, { TestCloneV1VsUnprotectProtect });
test tcCloneV1VsProtectRaceJournaling [main=TestCloneV1VsUnprotectProtectJournaling]:
  assert ChildHasParent, AllAnswered in (union System, { TestCloneV1VsUnprotectProtectJournaling });
test tcCloneV1VsProtectRaceCas [main=TestCloneV1VsUnprotectProtectCas]:
  assert ChildHasParent, AllAnswered in (union System, { TestCloneV1VsUnprotectProtectCas });
test tcFlattenVsRemove [main=TestFlattenVsRemove]:
  assert ChildHasParent, ParentReadable, AllAnswered in (union System, { TestFlattenVsRemove });
test tcChildReadVsRemove [main=TestChildReadVsRemove]:
  assert ChildHasParent, ParentReadable, AllAnswered in (union System, { TestChildReadVsRemove });
test tcFlattenVsChildSnapNoLock [main=TestFlattenVsChildSnapNoLock]:
  assert ChildHasParent in (union System, { TestFlattenVsChildSnapNoLock });

// crashes
test tcWritersCrash [main=TestWritersCrash]:
  assert WritesFenced, AllAnswered in (union System, { TestWritersCrash });
test tcOwnerCrashStale [main=TestOwnerCrashSnapCreate]:
  assert SnapImmutable in (union System, { TestOwnerCrashSnapCreate });
test tcOwnerCrashSafe [main=TestOwnerCrashSnapCreate]:
  assert WritesFenced, AllAnswered in (union System, { TestOwnerCrashSnapCreate });
test tcOwnerCrashRefresh [main=TestOwnerCrashSnapCreateRefresh]:
  assert WritesFenced, SnapImmutable, AllAnswered in (union System, { TestOwnerCrashSnapCreateRefresh });
test tcCreateViaOwnerCrashSafe [main=TestCreateViaOwnerCrash]:
  assert WritesFenced, AllAnswered in (union System, { TestCreateViaOwnerCrash });
test tcCreateViaOwnerCrashStale [main=TestCreateViaOwnerCrash]:
  assert SnapImmutable in (union System, { TestCreateViaOwnerCrash });
test tcFlattenCrashParent [main=TestFlattenCrash]:
  assert ChildHasParent in (union System, { TestFlattenCrash });
test tcFlattenCrashReads [main=TestFlattenCrash]:
  assert ParentReadable, AllAnswered in (union System, { TestFlattenCrash });
test tcCloneV1Crash [main=TestCloneV1Crash]:
  assert ChildHasParent, AllAnswered in (union System, { TestCloneV1Crash });

// image removal
test tcRemoveVsClone [main=TestRemoveVsClone]:
  assert ChildHasParent, AllAnswered in (union System, { TestRemoveVsClone });
test tcRemoveVsCloneNoLock [main=TestRemoveVsCloneNoLock]:
  assert ChildHasParent, AllAnswered in (union System, { TestRemoveVsCloneNoLock });
test tcRemoveParentVsFlatten [main=TestRemoveParentVsFlatten]:
  assert ChildHasParent, ParentReadable, AllAnswered in (union System, { TestRemoveParentVsFlatten });

// two children
test tcTwoChildren [main=TestTwoChildren]:
  assert ChildHasParent, ParentReadable, AllAnswered in (union System, { TestTwoChildren });

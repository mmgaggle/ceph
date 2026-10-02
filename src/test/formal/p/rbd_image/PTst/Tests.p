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
test tcCreateViaOwnerLostWatchSnap [main=TestSnapCreateViaOwnerDrop]:
  assert SnapImmutable in (union System, { TestSnapCreateViaOwnerDrop });
test tcCreateViaOwnerLostWatchRefresh [main=TestSnapCreateViaOwnerDropRefresh]:
  assert WritesFenced, SnapImmutable, NoLeakedSnapIds, AllAnswered in (union System, { TestSnapCreateViaOwnerDropRefresh });
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

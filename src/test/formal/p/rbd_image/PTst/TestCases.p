// the code on main, with the default features and options: exclusive-lock,
// fast-diff (snap remove goes to the lock owner), deep-flatten, clone v2,
// no journaling (protect and unprotect run with no lock)
fun Main(): tCfg {
  return (exclusiveLock = true, cloneV2 = true, proxySnapRemove = true, proxyProtect = false,
          blocklistOnBreak = true, autoPolicy = true, cloneRechecks = true, unprotectScans = true,
          deepFlatten = true, protectCas = false, refreshOnAcquire = false, watchDrops = 0,
          crashes = 0);
}
fun CloneV1(c: tCfg): tCfg {
  c.cloneV2 = false;
  return c;
}
fun NoLock(c: tCfg): tCfg {
  c.exclusiveLock = false;
  return c;
}
fun Journaling(c: tCfg): tCfg {
  c.proxyProtect = true;
  return c;
}
fun Drops(c: tCfg, n: int): tCfg {
  c.watchDrops = n;
  return c;
}
fun Crashes(c: tCfg, n: int): tCfg {
  c.crashes = n;
  return c;
}
fun NoBlocklist(c: tCfg): tCfg {
  c.blocklistOnBreak = false;
  return c;
}

fun Fresh(): tInit { return (snap = false, protected = false, children = false); }
fun WithSnap(): tInit { return (snap = true, protected = false, children = false); }
fun WithProtectedSnap(): tInit { return (snap = true, protected = true, children = false); }
// image 2 cloned (v2) from snapshot 1, which clone v2 leaves unprotected
fun WithChild(): tInit { return (snap = true, protected = false, children = true); }

fun Scripts1(a: tScript): seq[tScript] {
  var s: seq[tScript];
  s += (0, a);
  return s;
}
fun Scripts2(a: tScript, b: tScript): seq[tScript] {
  var s: seq[tScript];
  s += (0, a);
  s += (1, b);
  return s;
}
fun Scripts3(a: tScript, b: tScript, c: tScript): seq[tScript] {
  var s: seq[tScript];
  s += (0, a);
  s += (1, b);
  s += (2, c);
  return s;
}

/* the exclusive lock */

// two clients writing to one image: the lock moves between them
machine TestTwoWriters {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = Fresh(),
                  scripts = Scripts2(On(1, Acts2(Write(1), Write(2))), On(1, Acts2(Write(2), Write(1))))));
    }
  }
}
// the same, with the OSD dropping a watch once: the owner looks dead
// and its lock is broken
machine TestWriterLosesWatch {
  start state Init {
    entry {
      new Driver((cfg = Drops(Main(), 1), init = Fresh(),
                  scripts = Scripts2(On(1, Acts2(Write(1), Write(2))), On(1, Acts2(Write(2), Write(1))))));
    }
  }
}
// the same, without blocklisting on break
machine TestWriterLosesWatchNoBlocklist {
  start state Init {
    entry {
      new Driver((cfg = Drops(NoBlocklist(Main()), 1), init = Fresh(),
                  scripts = Scripts2(On(1, Acts2(Write(1), Write(2))), On(1, Acts2(Write(2), Write(1))))));
    }
  }
}
// three clients
machine TestThreeWriters {
  start state Init {
    entry {
      new Driver((cfg = Drops(Main(), 1), init = Fresh(),
                  scripts = Scripts3(On(1, Acts2(Write(1), Write(2))), On(1, Acts1(Write(2))),
                                     On(1, Acts1(Write(1))))));
    }
  }
}

/* snapshots */

// a snapshot created by a client that does not own the lock, while the
// owner writes
machine TestSnapCreateViaOwner {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = Fresh(),
                  scripts = Scripts2(On(1, Acts3(Write(1), Write(2), Write(1))), On(1, Acts2(SnapCreate(1), Write(2))))));
    }
  }
}
// the same, with the OSD dropping a watch once
machine TestSnapCreateViaOwnerDrop {
  start state Init {
    entry {
      new Driver((cfg = Drops(Main(), 1), init = Fresh(),
                  scripts = Scripts2(On(1, Acts3(Write(1), Write(2), Write(1))), On(1, Acts2(SnapCreate(1), Write(2))))));
    }
  }
}
// the owner takes the lock and creates a snapshot; the OSD drops a
// watch once; a peer writes
machine TestOwnerLostWatch {
  start state Init {
    entry {
      new Driver((cfg = Drops(Main(), 1), init = Fresh(),
                  scripts = Scripts2(On(1, Acts3(AcquireLock(), SnapCreate(1), Write(2))), On(1, Acts1(Write(1))))));
    }
  }
}
// the same, with the new owner refreshing the header as it acquires
machine TestOwnerLostWatchRefresh {
  start state Init {
    entry {
      var c: tCfg;
      c = Drops(Main(), 1);
      c.refreshOnAcquire = true;
      new Driver((cfg = c, init = Fresh(),
                  scripts = Scripts2(On(1, Acts3(AcquireLock(), SnapCreate(1), Write(2))), On(1, Acts1(Write(1))))));
    }
  }
}
// without the exclusive-lock feature: every client writes
machine TestSnapCreateNoLock {
  start state Init {
    entry {
      new Driver((cfg = NoLock(Main()), init = Fresh(),
                  scripts = Scripts2(On(1, Acts3(Write(1), Write(2), Write(1))), On(1, Acts2(SnapCreate(1), Write(2))))));
    }
  }
}
// two clients each create a snapshot, without the lock
machine TestTwoCreatesNoLock {
  start state Init {
    entry {
      new Driver((cfg = NoLock(Main()), init = Fresh(),
                  scripts = Scripts2(On(1, Acts1(SnapCreate(1))), On(1, Acts1(SnapCreate(2))))));
    }
  }
}
// a snapshot create sent to the owner while a third client takes the lock
machine TestSnapCreateOwnerChange {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = Fresh(),
                  scripts = Scripts3(On(1, Acts2(Write(1), Write(2))), On(1, Acts1(SnapCreate(1))),
                                     On(1, Acts1(Write(2))))));
    }
  }
}
// a snapshot removed while the owner writes, and a create of the same name
machine TestSnapRemoveVsCreate {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = WithSnap(),
                  scripts = Scripts2(On(1, Acts2(Write(1), Write(2))), On(1, Acts2(SnapRemove(1), SnapCreate(1))))));
    }
  }
}

/* layering */

// a clone v2 of snapshot 1 while another client removes the snapshot
machine TestCloneV2VsRemove {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = WithSnap(),
                  scripts = Scripts2(On(1, Acts1(Clone(1, 2))), On(1, Acts1(SnapRemove(1))))));
    }
  }
}
// a clone v1 of protected snapshot 1 while another client unprotects it
machine TestCloneV1VsUnprotect {
  start state Init {
    entry {
      new Driver((cfg = CloneV1(Main()), init = WithProtectedSnap(),
                  scripts = Scripts2(On(1, Acts1(Clone(1, 2))), On(1, Acts2(Unprotect(1), SnapRemove(1))))));
    }
  }
}
// the same, with the clone not re-checking the protection after add_child
machine TestCloneV1NoRecheck {
  start state Init {
    entry {
      var c: tCfg;
      c = CloneV1(Main());
      c.cloneRechecks = false;
      new Driver((cfg = c, init = WithProtectedSnap(),
                  scripts = Scripts2(On(1, Acts1(Clone(1, 2))), On(1, Acts2(Unprotect(1), SnapRemove(1))))));
    }
  }
}
// a clone v1, an unprotect, and a protect from a third client
machine TestCloneV1VsUnprotectProtect {
  start state Init {
    entry {
      new Driver((cfg = CloneV1(Main()), init = WithProtectedSnap(),
                  scripts = Scripts3(On(1, Acts1(Clone(1, 2))), On(1, Acts2(Unprotect(1), SnapRemove(1))),
                                     On(1, Acts1(Protect(1))))));
    }
  }
}
// the same, with protect and unprotect through the lock owner (journaling)
machine TestCloneV1VsUnprotectProtectJournaling {
  start state Init {
    entry {
      new Driver((cfg = Journaling(CloneV1(Main())), init = WithProtectedSnap(),
                  scripts = Scripts3(On(1, Acts1(Clone(1, 2))), On(1, Acts2(Unprotect(1), SnapRemove(1))),
                                     On(1, Acts1(Protect(1))))));
    }
  }
}
// the same, with set_protection_status checking the status it replaces
machine TestCloneV1VsUnprotectProtectCas {
  start state Init {
    entry {
      var c: tCfg;
      c = CloneV1(Main());
      c.protectCas = true;
      new Driver((cfg = c, init = WithProtectedSnap(),
                  scripts = Scripts3(On(1, Acts1(Clone(1, 2))), On(1, Acts2(Unprotect(1), SnapRemove(1))),
                                     On(1, Acts1(Protect(1))))));
    }
  }
}
// a child flattens while its parent's snapshot is removed
machine TestFlattenVsRemove {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = WithChild(),
                  scripts = Scripts2(On(2, Acts2(Flatten(), ReadChild(1))), On(1, Acts1(SnapRemove(1))))));
    }
  }
}
// a child reads through to the parent while the parent's snapshot is removed
machine TestChildReadVsRemove {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = WithChild(),
                  scripts = Scripts2(On(2, Acts2(ReadChild(1), ReadChild(2))), On(1, Acts1(SnapRemove(1))))));
    }
  }
}
// a child without the exclusive lock: a flatten while a snapshot of the
// child is created, without deep-flatten
machine TestFlattenVsChildSnapNoLock {
  start state Init {
    entry {
      var c: tCfg;
      c = NoLock(Main());
      c.deepFlatten = false;
      new Driver((cfg = c, init = WithChild(),
                  scripts = Scripts3(On(2, Acts1(Flatten())), On(2, Acts1(SnapCreate(1))),
                                     On(1, Acts2(Unprotect(1), SnapRemove(1))))));
    }
  }
}

/* crashes */

// two writers, each of which may die once
machine TestWritersCrash {
  start state Init {
    entry {
      new Driver((cfg = Crashes(Main(), 1), init = Fresh(),
                  scripts = Scripts2(On(1, Acts2(Write(1), Write(2))), On(1, Acts2(Write(2), Write(1))))));
    }
  }
}
// the owner creates a snapshot and may die; the peer writes
machine TestOwnerCrashSnapCreate {
  start state Init {
    entry {
      new Driver((cfg = Crashes(Main(), 1), init = Fresh(),
                  scripts = Scripts2(On(1, Acts3(Write(1), SnapCreate(1), Write(2))), On(1, Acts2(Write(2), Write(1))))));
    }
  }
}
// the same, with a refresh on every acquire
machine TestOwnerCrashSnapCreateRefresh {
  start state Init {
    entry {
      var c: tCfg;
      c = Crashes(Main(), 1);
      c.refreshOnAcquire = true;
      new Driver((cfg = c, init = Fresh(),
                  scripts = Scripts2(On(1, Acts3(Write(1), SnapCreate(1), Write(2))), On(1, Acts2(Write(2), Write(1))))));
    }
  }
}
// a snapshot created through the owner, which may die
machine TestCreateViaOwnerCrash {
  start state Init {
    entry {
      new Driver((cfg = Crashes(Main(), 1), init = Fresh(),
                  scripts = Scripts2(On(1, Acts3(Write(1), Write(2), Write(1))), On(1, Acts2(SnapCreate(1), Write(2))))));
    }
  }
}
// a child flattens and may die; the parent's snapshot is then removed
machine TestFlattenCrash {
  start state Init {
    entry {
      new Driver((cfg = Crashes(Main(), 1), init = WithChild(),
                  scripts = Scripts2(On(2, Acts1(Flatten())), On(1, Acts1(SnapRemove(1))))));
    }
  }
}
// a clone v1 may die; the snapshot is then unprotected and removed
machine TestCloneV1Crash {
  start state Init {
    entry {
      new Driver((cfg = Crashes(CloneV1(Main()), 1), init = WithProtectedSnap(),
                  scripts = Scripts2(On(1, Acts1(Clone(1, 2))), On(1, Acts2(Unprotect(1), SnapRemove(1))))));
    }
  }
}

/* image removal */

// rbd rm while a peer creates a snapshot and clones it
machine TestRemoveVsClone {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = Fresh(),
                  scripts = Scripts2(On(1, Acts1(RemoveImage())), On(1, Acts2(SnapCreate(1), Clone(1, 2))))));
    }
  }
}
// the same, without the exclusive-lock feature
machine TestRemoveVsCloneNoLock {
  start state Init {
    entry {
      new Driver((cfg = NoLock(Main()), init = Fresh(),
                  scripts = Scripts2(On(1, Acts1(RemoveImage())), On(1, Acts2(SnapCreate(1), Clone(1, 2))))));
    }
  }
}
// rbd rm of a parent whose child flattens, while a third client clones
machine TestRemoveParentVsFlatten {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = WithChild(),
                  scripts = Scripts3(On(1, Acts2(SnapRemove(1), RemoveImage())), On(2, Acts1(Flatten())),
                                     On(1, Acts1(Clone(1, 3))))));
    }
  }
}

/* two children */

// a second clone while the first flattens and the snapshot is removed
machine TestTwoChildren {
  start state Init {
    entry {
      new Driver((cfg = Main(), init = WithChild(),
                  scripts = Scripts3(On(1, Acts1(SnapRemove(1))), On(2, Acts2(Flatten(), ReadChild(1))),
                                     On(1, Acts1(Clone(1, 3))))));
    }
  }
}

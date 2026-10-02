# RBD exclusive lock, snapshots and layering: a P model

A model of librbd clients that share an image. Each client is one
ImageCtx: a watch on the header, the exclusive lock state machine, a
write path, and the operations it runs. The RADOS state is one store:
the headers with their cls_lock, snapshots and parent, the rbd_children
object, the pool's snapshot ids, the blocklist, and the data objects
with their snapshot clones.

The model follows the code on main as of `504b1a2b334`. Line numbers
below refer to that commit.

## The properties

**WritesFenced.** With the exclusive-lock feature, every write applied
to a data object comes from the client that holds the header's lock at
that moment. A client that lost the lock is fenced before the new holder
writes.

**SnapImmutable.** What a snapshot shows never changes after it is
created. When `snapshot_add` committed, each object showed some data.
Any later read at the snapshot answers that data.

**ChildHasParent.** A parent snapshot is never removed while a child's
head, or a child's snapshot, still reads through it. Nor is a parent
image. A child is an image whose clone completed. A child that has every
object of its own reads nothing through its parent. But at the end, no
child's head or snapshot names a parent snapshot that is gone, because
such a child cannot be opened.

**ParentReadable.** A child always finds its parent's data. Every object
of the parent was written before the clone, so a read through to the
parent never fails.

**CreateAnswered.** A snapshot create answered EEXIST created no snapshot
itself.

**AllAnswered** (liveness). Every action is answered.

**NoLeakedSnapIds.** At the end, every snapshot id the pool holds belongs
to a snapshot in some header.

## What is modelled

### The image

Image 1 has two objects, each written once. Some scenarios start with
snapshot 1 of it, protected or not, and with image 2 cloned from that
snapshot.

### `Store`: the RADOS state

Each handler is one atomic op on one object, or one monitor command.

- Each image's header. Its cls_lock entry: `lock_obj`, `unlock`,
  `break_lock`, `set_cookie` and `get_info` as `cls_lock.cc` keeps them,
  with no duration. Its watchers: one handle per client. Its snapshots
  as `cls_rbd` keeps them. `snapshot_add` has the `-ESTALE` check.
  `snapshot_remove` has the `-EBUSY` checks on protection and child
  count. `snapshot_trash_add`, `snapshot_get` and `child_detach` are as
  in the code. `set_protection_status` is a blind write. `child_attach`
  refuses a trashed snapshot. Its parent: `set_parent` and `remove_parent`, where
  deep-flatten strips the snapshots' parent too. Its `snap_seq`. A
  refresh reads the header in one op.
- The rbd_children object of clone v1: `add_child`, `remove_child`,
  `get_children`, in one pool.
- The monitor: the pool's self-managed snapshot ids, and the blocklist.
  A blocklisted client loses its watches (`check_blocklisted_watchers`).
  Its later OSD ops answer `-EBLOCKLISTED` (`PrimaryLogPG::do_op`). The
  break's wait for the latest OSD map is folded into the command: every
  OSD fences at once.
- The data objects, written as `PrimaryLogPG::make_writeable` writes
  them. Removed snapshots are filtered out of the op's snap context. If
  the context's newest snapshot is newer than the object's
  `snapset.seq`, the write clones the object first. If the context is
  older, the write is applied with no clone, because librbd never sets
  `ORDERSNAP`. A removed snapshot's clones are trimmed. A read at a
  snapshot answers the clone that covers it. If the object was not
  cloned past the snapshot, the read answers the head. Otherwise it
  answers `-ENOENT`.
- Notifies. A notify goes to every watcher of the header. It completes
  once each watcher acked it or lost its watch. A lost watcher is a
  missing ack, as the OSD's notify timeout treats it.
- A watch timeout. The OSD can drop a live client's watch, as many
  times as the scenario allows, at an op chosen up front out of 60. The
  client learns of it later and re-watches.
- A client's death. If the scenario allows it, a client picks a step at
  which it dies, out of its first 40. Its watches lapse (the OSD's
  watch timeout), nothing it had in flight completes, and its lock
  entry stays until a peer breaks it.

### `Client`: one ImageCtx

A client runs a script of actions one after another.

- The lock (`ManagedLock`, `ExclusiveLock`), with its action queue of
  TRY_LOCK, ACQUIRE_LOCK, RELEASE_LOCK and REACQUIRE_LOCK. An action
  already queued is merged into.
  - Acquire (`AcquireRequest`): `get_lock_info`, then `lock`. On
    `-EBUSY`, `BreakRequest` lists the watchers. If a watcher has the
    holder's address and the holder's cookie as handle, the holder is
    alive (`BreakRequest.cc:84-86`). Otherwise the request re-reads the
    lock, blocklists the holder (`rbd_blocklist_on_break_lock`), breaks
    the lock, and locks again. If the holder is alive, the acquire waits
    (WAITING_FOR_LOCK) and asks for the lock with a RequestLock notify.
    If no answer carries a payload, the result is `-ETIMEDOUT` and the
    acquire retries at once. If the answer is `-EROFS`, the acquire
    fails. Any other answer re-sends the request, as many times as the
    scenario allows, and then waits for a peer's ReleasedLock or
    AcquiredLock.
  - Post-acquire (`PostAcquireRequest`). If a HeaderUpdate was seen,
    refresh the header. Then send AcquiredLock to every watcher, and
    let writes go without the lock.
  - Release (`PreReleaseRequest`, `ReleaseRequest`): writes need the
    lock again, in-flight writes and ops finish, `unlock`,
    ReleasedLock. If a peer asks for the lock, the owner releases it
    (`AutomaticPolicy`) or answers `-EROFS` (`StandardPolicy`).
  - Re-acquire after a re-watch (`ReacquireRequest`): `set_cookie` to
    the new handle. If that fails, or there is no handle, the client
    releases and then acquires again (`release_acquire_lock`).
- The write path (`io::ImageDispatch`, `exclusive_lock::ImageDispatch`,
  `RefreshImageDispatch`). If a HeaderUpdate was seen, a write refreshes
  the header first. If writes need the lock, it waits for the lock. If
  writes are blocked (`block_writes`), it waits. It carries the client's
  snap context and cookie.
- Operations (`Operations::C_InvokeAsyncRequest`). Refresh first. If
  the op needs the lock, try to acquire it. As owner, run it locally.
  Otherwise send it to the owner (`notify_async_request`) and wait for
  its AsyncComplete. If the wait times out, or a lock owner announces
  itself (`schedule_cancel_async_requests`), the client retries. As owner, serve
  requests from peers (`handle_operation_request`), with the pending
  and completed request ids. Snapshot create and flatten always go
  through the owner. Snapshot remove does so with fast-diff or
  journaling (`Operations.cc:1050-1051`). Protect and unprotect do so
  only with journaling (`Operations.cc:1250, 1343`).
- Snapshot create (`SnapshotCreateRequest`): block writes and drain
  them, allocate a snap id, `snapshot_add`, update the snap context,
  unblock, HeaderUpdate. On `-ESTALE`, allocate again without releasing
  the stale id.
- Snapshot remove (`SnapshotRemoveRequest`): `snapshot_trash_add`, then
  `snapshot_get`. If children are attached, the snapshot stays in the
  trash and the op is done. Otherwise release the snap id, then
  `snapshot_remove`.
- Protect and unprotect (`SnapshotProtectRequest`,
  `SnapshotUnprotectRequest`). Protect writes PROTECTED. Unprotect
  writes UNPROTECTING, scans rbd_children, then writes UNPROTECTED. If
  the scan found a child, it writes PROTECTED back and answers `-EBUSY`.
- Clone (`image::CloneRequest`, `AttachChildRequest`), with this
  client's image as the parent. Check the snapshot (PROTECTED for v1).
  Create the child. Set its parent. Attach it: `add_child` for v1,
  `child_attach` for v2. For v1, refresh the parent and make sure that
  the snapshot is still PROTECTED. A failure removes the child.
- Flatten (`FlattenRequest`, `CopyupRequest`, `DetachChildRequest`,
  `DetachParentRequest`), on this client's image as the child. For each
  object missing from the child, read the parent at the snapshot and
  copy up. Then detach the child, unless it has snapshots and no
  deep-flatten. Remove a trashed parent snapshot left with no children.
  Detach the parent.
- A read through to the parent (`io::util::read_parent`). If a read of
  a child object answers `-ENOENT`, read the parent at the snapshot.
- Image removal (`image::PreRemoveRequest`, `image::RemoveRequest`), on
  this client's image. Acquire the lock, and keep it while removing
  (`StandardPolicy`). If a snapshot is listed, answer `-ENOTEMPTY`,
  after removing the trashed ones. If another client watches the
  header, answer `-EBUSY`. Detach the image from its parent. Check the
  listed snapshots once more. Close the image: release the lock,
  unregister the watch. Remove the header.

### `Driver`

It starts the clients, one per script. Once every action is answered,
it waits until no live client has anything in flight (an op run for a
peer, a write, a lock action), then it quiesces the store.

### What is not modelled

- Object maps, journals and the image cache. Reads other than the read
  through to the parent. More than one pool.
- Image trimming on removal. The data objects stay, so a clone made in
  the removal's window still finds data.
- The 600 s expiry of a completed request on the owner. The model folds
  it into one rule. If an owner is asked again for a request it
  completed with 0, it runs the request again. See finding 2. The
  request timer (`rbd_request_timed_out_seconds`) fires once per sent
  request, before or after its completion, as the scheduler decides.
  The retry timer of a lock request (`schedule_request_lock`) fires
  once per answer from the owner, likewise.
- A clone that dies half built. Its image stays in the pool, with a
  parent link nobody counts. It is not a child for ChildHasParent.
- A proxied op whose owner dies mid-way. When a child removes a trashed
  parent snapshot, it does not take the parent's lock.

## The scenarios

Every case runs on `Main()`: exclusive-lock, fast-diff, deep-flatten,
clone v2, no journaling, `rbd_blocklist_on_break_lock` on and
`AutomaticPolicy`, unless the case says otherwise.

| Case | Scenario | Properties | Expected |
|---|---|---|---|
| `tcTwoWriters` | two clients write two objects each | WritesFenced, AllAnswered | holds |
| `tcLostWatchFenced` | the same, with the OSD dropping one watch | WritesFenced, AllAnswered | holds |
| `tcLostWatchNoBlocklist` | the same, without blocklisting on break | WritesFenced | violated |
| `tcThreeWriters` | three writers, one dropped watch | WritesFenced, AllAnswered | holds |
| `tcCreateViaOwnerSafe` | the owner writes while a peer creates a snapshot through it | WritesFenced, SnapImmutable, NoLeakedSnapIds, AllAnswered | holds |
| `tcCreateViaOwnerAnswer` | the same | CreateAnswered | violated (finding 2) |
| `tcCreateViaOwnerLostWatchSafe` | the same, one dropped watch | WritesFenced, NoLeakedSnapIds, AllAnswered | holds |
| `tcCreateViaOwnerLostWatchAnswer` | the same | CreateAnswered | violated (finding 2) |
| `tcOwnerLostWatchStale` | the owner takes the lock and creates a snapshot, one dropped watch, a peer writes | SnapImmutable | violated (finding 1) |
| `tcOwnerLostWatchSafe` | the same | WritesFenced, NoLeakedSnapIds, AllAnswered | holds |
| `tcOwnerLostWatchRefresh` | the same, with a refresh on every acquire | all of the above | holds |
| `tcSnapCreateNoLock` | without the exclusive-lock feature | SnapImmutable | violated (finding 4) |
| `tcTwoCreatesNoLock` | two creates without the feature | NoLeakedSnapIds | violated (finding 4) |
| `tcCreateOwnerChangeAnswer` | a create through the owner while a third client takes the lock | CreateAnswered | violated (finding 2) |
| `tcCreateOwnerChangeSafe` | the same | WritesFenced, SnapImmutable, NoLeakedSnapIds, AllAnswered | holds |
| `tcSnapRemoveVsCreate` | a remove and a create of the same name through the owner | the safety properties, AllAnswered | holds |
| `tcCloneV2VsRemove` | a clone v2 while the snapshot is removed | ChildHasParent, AllAnswered | holds |
| `tcCloneV1VsUnprotectOnly` | a clone v1 while the snapshot is unprotected and removed | ChildHasParent, AllAnswered | holds |
| `tcCloneV1NoRecheck` | the same, without the clone's re-check | ChildHasParent | violated |
| `tcCloneV1VsProtectRace` | the same, with a third client protecting | ChildHasParent | violated (finding 3) |
| `tcCloneV1VsProtectRaceJournaling` | the same, with journaling | ChildHasParent, AllAnswered | holds |
| `tcCloneV1VsProtectRaceCas` | the same, with a compare-and-set | ChildHasParent, AllAnswered | holds |
| `tcFlattenVsRemove` | a child flattens while the parent snapshot is removed | ChildHasParent, ParentReadable, AllAnswered | holds |
| `tcChildReadVsRemove` | a child reads while the parent snapshot is removed | ChildHasParent, ParentReadable, AllAnswered | holds |
| `tcFlattenVsChildSnapNoLock` | a flatten and a snapshot of the child, no lock, no deep-flatten | ChildHasParent | violated (finding 4) |

`expect.txt` lists each case's expected outcome and a fragment of the
failure message. `tcCreateViaOwnerAnswer` needs more schedules than the others under PCT
with depth 3. `deep.sh` finds its counterexample under random, PCT with
depth 5 and POS in a few thousand schedules each.

The crash cases, each with `crashes = 1`:

| Case | Scenario | Properties | Expected |
|---|---|---|---|
| `tcWritersCrash` | two writers | WritesFenced, AllAnswered | holds |
| `tcOwnerCrashStale` | the owner creates a snapshot, a peer writes | SnapImmutable | violated (finding 1) |
| `tcOwnerCrashSafe` | the same | WritesFenced, AllAnswered | holds |
| `tcOwnerCrashRefresh` | the same, with a refresh on every acquire | WritesFenced, SnapImmutable, AllAnswered | holds |
| `tcCreateViaOwnerCrashSafe` | a snapshot through the owner, which may die | WritesFenced, AllAnswered | holds |
| `tcCreateViaOwnerCrashStale` | the same | SnapImmutable | violated (finding 1) |
| `tcFlattenCrashParent` | a child flattens, the parent snapshot is removed | ChildHasParent | violated (finding 5) |
| `tcFlattenCrashReads` | the same | ParentReadable, AllAnswered | holds |
| `tcCloneV1Crash` | a clone v1, then unprotect and remove | ChildHasParent, AllAnswered | holds |

The image removal and two-children cases:

| Case | Scenario | Properties | Expected |
|---|---|---|---|
| `tcRemoveVsClone` | `rbd rm` while a peer creates a snapshot and clones it | ChildHasParent, AllAnswered | violated (finding 6) |
| `tcRemoveVsCloneNoLock` | the same, without the exclusive-lock feature | ChildHasParent, AllAnswered | violated (finding 6) |
| `tcRemoveParentVsFlatten` | `rbd rm` of a parent after its snapshot, a child flattens, a third client clones | ChildHasParent, ParentReadable, AllAnswered | holds |
| `tcTwoChildren` | the snapshot is removed, a child flattens and reads, a second clone | ChildHasParent, ParentReadable, AllAnswered | holds |

## What the model found

### 1. A new lock owner writes with a stale snap context

If `is_refresh_required()` is false, `PostAcquireRequest::send_refresh`
skips the refresh (`PostAcquireRequest.cc:66`). It is true only after a
HeaderUpdate notify. The previous owner sends that notify after
`snapshot_add` and after it unblocks writes (`SnapshotCreateRequest.cc`,
`C_NotifyUpdate` in `Operations.cc:112-152`). If it is fenced in
between, nobody sends it.

The trace (`tcOwnerLostWatchStale`): client 2 owns the lock. The OSD
drops its watch. Client 2 runs a snapshot create: `snapshot_add` commits
snapshot 1. Client 1 tries the lock. The holder has no watcher with its cookie, so it looks
dead (`BreakRequest.cc:84-93`). Client 1 blocklists it, breaks the lock
and takes it. Client 1 saw no HeaderUpdate, so it does not refresh. It
writes object 1 with a snap context of `seq 0`. The OSD applies the
write to the head with no clone. Snapshot 1 now shows the new data for
object 1.

If the owner dies between `snapshot_add` and the notify, the same
happens, with no watch drop needed: `tcOwnerCrashStale` and
`tcCreateViaOwnerCrashStale` show it. `ORDERSNAP` cannot help: the
object was never cloned, so its `snapset.seq` is 0.

A fix: refresh in `PostAcquireRequest` whether or not a HeaderUpdate was
seen (`refreshOnAcquire` in the model). `tcOwnerLostWatchRefresh` and
`tcOwnerCrashRefresh` hold with it.

### 2. A snapshot create retried after a lock owner announced itself is answered EEXIST

`ImageWatcher::handle_payload(AcquiredLockPayload)` cancels every pending
request with `-ERESTART`, unless the payload names the owner it already
knew (`ImageWatcher.cc:986-1013`). `C_InvokeAsyncRequest` then
refreshes, tries the lock, and sends the request again with the same
id. The owner can complete the request in the meantime. Its
AsyncComplete then finds no pending request at the requester, and is
dropped. The owner answers the retry from `m_async_complete` with 0
(`ImageWatcher.cc:795-819`). The requester waits for an AsyncComplete
that never comes. It times out after `rbd_request_timed_out_seconds`
(30 s), retries, and gets 0 again. After 600 s the owner forgets the
request (`ImageWatcher.cc:678-706`), runs it again, and answers
`-EEXIST`.

A requester sees an unknown owner announce itself in two cases. It
opened the image after the owner acquired the lock, and the owner
re-acquires it after a re-watch (`notify_acquired_lock` from
`post_reacquire_lock_handler`). Or both opened the image at the same
time. `tcCreateViaOwnerLostWatchAnswer` shows the first,
`tcCreateViaOwnerAnswer` the second.

The model folds the timer and the expiry into one rule. If an owner is
asked again for a request it completed with 0, it runs the request
again. The outcome is the same, ten minutes earlier.

A fix: keep the request registered across the retry, so that a
completion that arrives in the window is kept and matched.

### 3. Protect and unprotect race each other and a clone v1

Without journaling, `snap_protect` and `snap_unprotect` run locally with
no lock (`Operations.cc:1250, 1343`). `set_protection_status` writes
the new status without reading the old one (`cls_rbd.cc:1149-1202`).
Unprotect writes UNPROTECTING, scans rbd_children, then writes
UNPROTECTED, or PROTECTED back on `-EBUSY`
(`SnapshotUnprotectRequest.cc:239-309`). A clone v1 adds its child, then
refreshes the parent and requires PROTECTED
(`AttachChildRequest.cc:47-109`).

The trace: client 2 writes UNPROTECTING and scans: no child. Client 3
protects: PROTECTED. Client 1 clones: `add_child`, refresh, PROTECTED,
done. Client 2 writes UNPROTECTED. Client 2 removes the snapshot.
`snapshot_trash_add` and `snapshot_remove` see UNPROTECTED and a child
count of 0, because v1 children are not counted. The child's parent
snapshot is gone.

With journaling, both ops go through the lock owner, and the case holds.
A fix that needs no journaling: `set_protection_status` compares the
status it replaces (`protectCas` in the model). The protect over
UNPROTECTING then fails with `-EBUSY`, and so does an unprotect's final
write over PROTECTED. `tcCloneV1VsProtectRaceCas` holds with it.

### 4. Without the exclusive-lock feature

- A snapshot is not a point in time. The creating client blocks and
  drains its own writes. Another client learns of the snapshot from the
  HeaderUpdate, and its writes until then carry the old snap context.
  The OSD applies them with no clone, so the snapshot shows them.
- Two clients that create snapshots at once can leak a pool snapshot
  id. `snapshot_add` answers `-ESTALE` to the older id. The create then
  allocates another id without releasing the first
  (`SnapshotCreateRequest.cc:249-251`).
- A flatten of a child without deep-flatten detaches the child only if
  it has no snapshots (`FlattenRequest.cc:163-192`). A snapshot created
  in the meantime copies the parent link (`cls_rbd.cc:2402-2411`). The
  child's snapshot then reads a parent snapshot that no longer counts
  it, and that snapshot can be removed.

These are known limits of the feature, but the model shows the exact
interleavings.

### 5. A flatten that dies leaves a child that cannot be opened

`FlattenRequest` detaches the child from its parent first
(`DetachChildRequest`, `FlattenRequest.cc:163-192`) and clears the
child's parent link second (`DetachParentRequest`, `:210-253`). The
detach removes the parent's trashed snapshot once no child counts on it.
If the client dies in between, the child's header still names the
parent snapshot, which is gone, or which a later remove takes away.
The child's next open fails with `-ENOENT` on its parent
(`RefreshRequest.cc:865-886`), although every object was copied up.

A fix: clear the parent link first, then detach the child. A crash then
leaves a child that counts on its parent for nothing, which the next
flatten or remove cleans up.

### 6. `rbd rm` removes the header after it let go of the lock

`RemoveRequest` closes the image, which releases the lock and
unregisters the watch, and then removes the header. Nothing guards the
header's removal. A peer that opens the image in between creates a
snapshot, clones it, and loses its parent when the header goes. With
the exclusive-lock feature the remover holds the lock through its
checks, but not through the removal. `tcRemoveVsClone` shows it.

The window is short, and the data objects were trimmed before it. A
guard on the header's removal, such as a cls check that the header has
no snapshot, would close it.

## Running

```
../run.sh .                           # every case, 20,000 schedules each
../deep.sh . tcCreateViaOwnerAnswer   # one case, under three schedulers
```

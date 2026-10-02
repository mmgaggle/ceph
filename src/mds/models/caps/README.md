# P model of the CephFS capability protocol

This directory holds a model of the CephFS capability ("caps") protocol
written in P. P is a language for modelling asynchronous state machines
and a checker that explores their interleavings. The model covers one auth
MDS, one regular file, two or three clients, the CEPH_LOCK_IFILE lock that
governs the file caps, the session cap generations that invalidate caps
when a session goes stale, and client eviction with the OSD blocklist. The
checker searches for schedules that violate the properties in
`PSpec/Specs.p`.

The model is written from the code in `src/mds/Locker.cc`,
`src/mds/Capability.h`, `src/mds/CInode.cc`, `src/mds/Server.cc`,
`src/mds/SessionMap.h`, `src/mds/locks.c` and `src/client/Client.cc`.
Function names in the comments refer to those files.

## Layout

| File | Content |
| --- | --- |
| `PSrc/Caps.p` | cap bits, message types, set helpers |
| `PSrc/FileLock.p` | the `filelock` table of `locks.c`, its accessors and legal edges |
| `PSrc/Store.p` | the file data, reduced to one integer version, and the blocklist |
| `PSrc/MDS.p` | the MDS: Capability bookkeeping, issue_caps, eval, lock transitions, requests, sessions, max_size |
| `PSrc/Client.p` | the client: cap handling, reads, writes, flush, trim, getattr, setattr, session renewal |
| `PSpec/Specs.p` | the properties |
| `PTst/Tests.p` | application drivers and the test cases |

## How to run

The P tool needs the .NET 8 SDK. Install the tool once:

```
dotnet tool install --global P
```

Compile and check from this directory:

```
p compile
p check -tc tcWriterReader -s 5000
p check -tc tcTwoWriters -s 5000
p check -tc tcThreeClients -s 5000
p check -tc tcStaleSessions -s 5000
```

`-s` is the number of random schedules. The checker writes a trace to
`PCheckerOutput/BugFinding/` when a property fails, and a summary file
with the bug count. Pass `-v` to log every step of every schedule.

## Test cases

| Test | Clients | Sessions | Fixes enabled | Expected result |
| --- | --- | --- | --- | --- |
| `tcWriterReader` | a writer and a reader | stay open | none | passes |
| `tcTwoWriters` | two writers | stay open | none | passes; finding 3 is reachable but rare |
| `tcThreeClients` | two writers and a reader | stay open | none | passes |
| `tcStaleSessions` | two writers | time out and renew | none | reports finding 1, 2 or 3 |
| `tcStaleSessionsRecheck` | two writers | time out and renew | client fix 1 | reports finding 2 or 3 |
| `tcStaleSessionsFixed` | two writers | time out and renew | all three | passes |
| `tcTwoWritersFixed` | two writers | stay open | MDS fix 3 | passes |

The faithful tests report the findings below. The fixed variants show that
the proposed fixes remove them and nothing else appears at 20000 schedules.

## Findings

The model reports three candidate defects. Each one needs confirmation
against a running cluster before it is filed.

### Finding 1: a revocation handled while cap_ttl is expired is never acknowledged

Client side, liveness. Sequence: a client holds Fb with dirty buffered
data. Its cap_ttl expires because a renew ack is late, but the MDS has not
yet marked the session stale, which happens after an MDS lag or because the
MDS deadline is later than the client deadline. The MDS revokes Fc and Fb.
`handle_cap_grant` starts the flush. When the flush completes,
`_flushed` calls `put_cap_ref`, and `check_caps` computes `revoking` from
`caps_issued()`, which ignores caps whose ttl expired. No revocation is
visible, the cache is not released, and nothing is sent. The renew ack then
restores cap_ttl, but `wake_up_session_caps` only wakes waiters. Nothing
runs `check_caps` on the inode again. The MDS waits for the Fc and Fb
release forever, and every reader of the file waits with it. A close of the
file would run `check_caps` and recover, but reads and writes block first.

Proposed fix: when a RENEWCAPS ack restores cap_ttl, run `check_caps` with
`CHECK_CAPS_NODELAY` on inodes whose cap has `implemented & ~issued`. The
model switch `recheck_after_renew` implements it.

### Finding 2: the file cache survives a stale session

Client side, coherence. Sequence: a client holds Fc with cached data. Its
session goes stale and the MDS revokes Fc by force without a message
(`issue_caps` stale branch and `revoke_stale_cap`). Another client is
granted Fw and writes. The first client renews, `wake_up_session_caps`
resets its cap to PIN, and later the MDS grants Fc again. The object cacher
still holds the old data: `check_cap_issue` only increments `cache_gen`,
which nothing reads, and `_release` runs only on a revoke the client saw.
The next cached read returns the old data.

The kernel client invalidates the page cache when Fc is granted and was not
issued before. Proposed fix: do the same in the userspace client. The model
switch `invalidate_on_fc_grant` implements it.

### Finding 3: LOCK to MIX does not gather buffered writes

MDS side, coherence. In LOCK the filelock lets every client keep Fc and Fb
("keep Fcb to allow rapid recall of Fw" in `locks.c`). `scatter_mix` from
LOCK sets the state to MIX at once, with no gather, and `issue_caps` then
revokes Fc and Fb from the client that holds dirty data while it grants Fr
and Fw to the others in the same call. The other client reads the OSD, or
writes to it, before the buffered data is written back. The read returns
old data, or the late writeback overwrites the new write.

The path is reachable with two writers: the loner holds Fwb with dirty
data, a setattr or a max_size update takes the lock from EXCL to LOCK, the
loner keeps Fcb, and both clients want Fw, so `file_eval` calls
`scatter_mix` from LOCK. Proposed fix: from LOCK, go through an
intermediate state that allows no caps until Fc and Fb are released, as
LOCK_SYNC does for Fb. The model switch `gather_lock_to_mix` adds a
LOCK_LOCK_MIX state for this.

## Properties

DataCoherence. A read never returns data older than a write that completed
before the read started. The spec records the set of completed versions
when a read starts and compares the result with the newest of them that
was not lost since. A version is lost when its writer is blocklisted before
the data lands, and a buffered version that was overwritten in the cache
before it was written back is lost with the version that overwrote it.

CapTracking. The client's `Cap::implemented`, as long as the client's
session cap_gen and cap_ttl make the cap valid, is always a subset of the
MDS's `Capability::issued`. The MDS waits for issued caps to drain before
it completes a lock transition, so a cap that the client still trusts but
the MDS believes released breaks every other guarantee.

IoProgress. Every read, write, stat or setattr that an application issues
completes or is rejected. A schedule that ends in the hot state means a
client waits forever for caps, for `max_size`, for a flush, or for an MDS
request that never gets its lock.

LockTransitions. The filelock only moves along the edges Locker takes:
a gather completing to its target state, a stable state starting a
transition, and the direct edges of `scatter_mix` from LOCK,
`simple_xlock`, `xlock_start`, `set_xlocks_done` and `_finish_xlock`.

## What is modelled

The MDS machine mirrors Capability::issue, issue_norevoke, confirm_receipt,
revoke, clean_revoke_from and revalidate with the session cap_gen;
Locker::issue_caps with its stale and re-issue branches; handle_client_caps,
adjust_cap_wanted, _do_cap_update, process_request_cap_release and
kick_cap_releases; _do_cap_release and remove_client_cap; eval, eval_gather,
file_eval, simple_sync, simple_lock, file_excl, scatter_mix, file_xsyn,
simple_xlock, rdlock_start, rdlock_finish, xlock_start, xlock_finish,
_finish_xlock and cancel_locking, with the lock waiters that retry a blocked
request inline as MDSCacheObject::finish_waiting does; the loner selection;
check_inode_max_size, the journal wrlock and the C_MDL_CheckMaxSize waiter;
handle_client_open, handle_client_getattr for the filelock, and
handle_client_setattr for mtime with its early reply; encode_inodestat
including the no_caps rule for stale sessions; find_idle_sessions with
revoke_stale_caps, CEPH_SESSION_STALE, RENEWCAPS and resume_stale_caps;
revoke_stale_cap queued at the front of the dispatch queue; and
evict_client, which blocklists the client at the store, waits for the
blocklist, then kills the session, its caps and its requests.

The client machine mirrors handle_cap_grant, check_caps, send_cap,
add_update_cap, handle_cap_flush_ack, mark_caps_flushing, remove_cap on
trim, get_caps and put_cap_ref around reads and writes, the delayed cap
list, the object cacher dirty set and its flush callback, the max_size
handshake, _getattr, _do_setattr for mtime with encode_inode_release, _open
taking its open ref before deciding whether a request is needed,
cap_is_valid with cap_gen and cap_ttl, CEPH_SESSION_STALE, renew_caps,
wake_up_session_caps, the I_CAP_DROPPED recovery in get_caps, and
_closed_mds_session on eviction.

The file data is one integer version. A sync write lands in the Store at
once and gets the next version. A buffered write gets the next version when
it completes to the application and lands when the client flushes it. The
last landed write wins. A cached read returns the client's cache, which is
one (valid, version) pair. The Store rejects I/O from blocklisted clients
and reports their unlanded versions as lost at blocklist time.

Caps are sets of the F bits Fs, Fx, Fc, Fr, Fw and Fb. `max_size` is 0 or
1. Messages between one pair of machines arrive in order. Journal
completion is a message the MDS sends to itself. Each Capability carries a
`cap_id`, and MDS requests are tracked by request id, because an early
reply lets a client send its next request before the journal completes.

The session timeout is a message the client sends to the MDS after its own
cap_ttl expired. This encodes the timing assumption of the protocol: the
client stops trusting its caps before the MDS declares the session stale.
The client renews after every ttl expiry, as `tick` does on a timer.

## Approximations

Locker::eval collects WAIT_STABLE waiters and runs them after all locks are
evaluated. The model runs the waiters when the lock becomes stable.

A buffered write takes the Fc and Fb references of the dirty set before it
asks the Store for a version. The real client inserts the data into the
object cacher under the client lock, so no cap message can interleave.

File recovery is not modelled. When a killed client leaves a writeable
range, the model clears the range directly instead of marking the inode
NEEDSRECOVER.

The OSD epoch barrier is not modelled. The Store applies a blocklist
atomically before the MDS continues, which assumes the barrier works.

A stale session whose caps cannot be revoked by force is evicted at once,
and a session that is still stale at the next timeout is evicted. The
`defer_client_eviction_on_laggy_osds` behaviour is not modelled.

`is_max_size_approaching` reduces to "`max_size` is 0" because the size is
always 0. Truncation does not change the file data.

## Coverage

With the stale-session test and 400 verbose schedules, the filelock reached
SYNC, LOCK, MIX, EXCL and XSYN, the xlock chain LOCK_XLOCK, PREXLOCK, XLOCK
and XLOCKDONE, and the intermediate states EXCL_XSYN, XSYN_EXCL, EXCL_LOCK,
MIX_LOCK2, LOCK_EXCL, SYNC_LOCK, LOCK_SYNC, SYNC_MIX, EXCL_MIX, MIX_SYNC,
SYNC_EXCL and MIX_EXCL. The remaining states of the table need replicas,
snapshots or recovery.

Two deliberate faults were injected in the first version to confirm that
DataCoherence detects real defects. When the client keeps its cache after
it releases Fc, the checker reports a stale read within 301 schedules. When
LOCK_MIX allows Fb, the checker reports a stale read within 8 schedules.

## Follow-ups

- The OSD epoch barrier: cap messages carry the barrier and a client waits
  for the map before it does I/O after another client's eviction.
- Client reconnect after eviction and `client_reconnect_stale`.
- Truncation and the file data: `issue_truncate`, `handle_cap_trunc`,
  `truncate_seq`, and the cache invalidation of the truncated range.
- The auth, link and xattr simple locks, with As, Ax, Ls, Xs and Xx caps.
- `defer_client_eviction_on_laggy_osds`, where a stale session with
  revoking write caps stays stale and `encode_inodestat` with
  `issue_norevoke` can revalidate its cap.
- Cap export and import between two MDS ranks, with `mseq`.
- Snapshots: `cap_snaps`, FLUSHSNAP and `client_need_snapflush`.
- A kernel-client variant of the client machine.
- Directory inodes, where `get_caps_liked` and `CEPH_CAP_ANY_DIR_OPS`
  change which caps the MDS issues.

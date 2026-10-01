# P model of the CephFS capability protocol

This directory holds a model of the CephFS capability ("caps") protocol
written in P. P is a language for modelling asynchronous state machines
and a checker that explores their interleavings. The model covers one auth
MDS, one regular file, two or three clients, and the CEPH_LOCK_IFILE lock
that governs the file caps. The checker searches for schedules that violate
the properties in `PSpec/Specs.p`.

The model is written from the code in `src/mds/Locker.cc`,
`src/mds/Capability.h`, `src/mds/CInode.cc`, `src/mds/locks.c` and
`src/client/Client.cc`. Function names in the comments refer to those files.

## Layout

| File | Content |
| --- | --- |
| `PSrc/Caps.p` | cap bits, message types, set helpers |
| `PSrc/FileLock.p` | the `filelock` table of `locks.c` and its accessors |
| `PSrc/Store.p` | the file data, reduced to one integer version |
| `PSrc/MDS.p` | the MDS: Capability bookkeeping, issue_caps, eval, lock transitions, max_size |
| `PSrc/Client.p` | the client: handle_cap_grant, check_caps, send_cap, reads, writes, flush, trim |
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
```

`-s` is the number of random schedules. The checker writes a trace to
`PCheckerOutput/BugFinding/` when a property fails. Pass `-v` to log every
step of every schedule. Each run of 5000 schedules takes about 30 seconds.

## Properties

DataCoherence. A read never returns data older than a write that completed
before the read started. The spec records the newest completed version when
a read starts and compares it with the version the read returns. This is the
end-to-end promise of the protocol: Fr or Fc on one client never coexist
with Fw or Fb on another, and a client drops its cache before another client
can write.

CapTracking. The client's `Cap::implemented` is always a subset of the
MDS's `Capability::issued`. The MDS waits for issued caps to drain before it
completes a lock transition, so a cap that the client still uses but the MDS
believes released breaks every other guarantee.

IoProgress. Every read or write that an application issues completes or is
rejected with EBADF. A schedule that ends in the hot state means a client
waits forever for caps, for `max_size`, or for a flush.

LockTransitions. The filelock only moves along the edges of the `locks.c`
table: from a stable state into an intermediate state, and from an
intermediate state to its own target.

## What is modelled

The data of the file is one integer version. A sync write lands in the
Store at once and gets the next version. A buffered write gets the next
version when it completes to the application and lands when the client
flushes it. The last landed write wins, as an object overwrite does. A
cached read returns the client's cache. The cache is one (valid, version)
pair.

Caps are sets of the F bits Fs, Fx, Fc, Fr, Fw and Fb. The other locks
(auth, link, xattr) are not modelled. Fl and Fa are not modelled because no
lock state issues them unless a client asks for lazy IO.

`max_size` is 0 or 1. The file never grows, so one writeable range is
enough to drive the `client_ranges` path: `_do_cap_update`,
`check_inode_max_size`, the journal wrlock held on the filelock while an
update is journaled, `share_inode_max_size`, and the `C_MDL_CheckMaxSize`
waiter.

Messages between one pair of machines arrive in order, as they do on a
messenger session. Journal completion is a message the MDS sends to itself,
so other messages interleave with a pending journal entry.

Each Capability carries a `cap_id`. The MDS ignores updates and releases
for a cap id it no longer has, as `handle_client_caps` and
`_do_cap_release` do. Without this check the model reports a false
tracking violation when a client re-opens a file while its release of the
old cap is in flight.

The driver for each client issues a random sequence of open, close, read,
write, flush tick and trim operations and paces itself through its own
queue, so the checker interleaves it with the protocol machines.

## Approximations

Locker::eval() collects WAIT_STABLE waiters and runs them after all locks
are evaluated. The model runs the `C_MDL_CheckMaxSize` waiter at the point
the lock becomes stable, which is the order used when `eval_gather` runs
without a finisher list.

A buffered write takes the Fc and Fb references of the dirty set before it
asks the Store for a version. The real client inserts the data into the
object cacher under the client lock, so no cap message can interleave. The
version request is a modelling step with no counterpart in the code.

`is_max_size_approaching` reduces to "`max_size` is 0" because the size is
always 0.

The real client delays cap releases on a timer. The model sends the delayed
check to itself, so the checker chooses when it runs.

## Coverage

With three clients and 400 schedules, the filelock reached SYNC, LOCK, MIX
and EXCL and took these edges: SYNC to EXCL, SYNC to MIX, SYNC to LOCK,
EXCL to MIX, MIX to SYNC, MIX to EXCL, LOCK to MIX and LOCK to SYNC. The
EXCL to SYNC and EXCL to LOCK edges were not reached at this budget. The
XSYN states need an MDS rdlock request, which is not modelled yet.

Two deliberate faults were injected to confirm that DataCoherence detects
real defects. When the client keeps its cache after it releases Fc, the
checker reports a stale read within 301 schedules. When LOCK_MIX allows Fb,
the checker reports a stale read within 8 schedules.

## Follow-ups

- MDS requests that take rdlocks and xlocks on the filelock: getattr from
  another client (reaches the XSYN states through `file_xsyn`), setattr and
  truncate.
- The auth, link and xattr simple locks, with As, Ax, Ls, Xs and Xx caps.
- Session stale and reconnect: `cap_gen`, `revoke_stale_caps`, and the
  re-issue path in `issue_caps`.
- Cap export and import between two MDS ranks, with `mseq`.
- Snapshots: `cap_snaps`, FLUSHSNAP and `client_need_snapflush`.
- A kernel-client variant of the client machine. The kernel client differs
  from the userspace client in how it answers revokes and tracks `wanted`.
- Directory inodes, where `get_caps_liked` and `CEPH_CAP_ANY_DIR_OPS`
  change which caps the MDS issues.

# P models

[P](https://p-org.github.io/P/) models of Ceph protocols. Each one states
its properties, models the protocol as it is, and includes configurations
that remove one design element each. Those must produce a counterexample:
that shows the model can see the failure the element prevents.

| Model | Covers | Properties |
|---|---|---|
| [`pg_peering`](pg_peering/README.md) | peering of a replicated PG: past intervals, the prior set, `up_thru`, `find_best_info`, `choose_acting` and pg_temp, the monitor, crashes, false mark-downs, out, lost and `min_size` changes | acked writes survive; the PG goes active and every write is acked once the cluster settles |
| [`pg_statechart`](pg_statechart/README.md) | the whole `PeeringState` statechart, generated from the header, with writes, log trimming, recovery, backfill, the local and remote reservers, preemption, full OSDs, PG deletion and the mgr's commands | no `Crashed` and no failed `ceph_assert`; acked writes are on every copy once clean; the PG goes active+clean, every write is acked and every reservation is released once the cluster settles |

## What the models found

The `unittest_peeringstate` tests named here go in with their fixes.

- **A backfill target at the per-OSD PG limit leaves its PG stuck
  `activating`** (`pg_statechart`, `tcMaxPgBackfillTarget`,
  `tcMaxPgSingle`; tracker 23117). The OSD withholds creating the PG and
  should force a new interval once it has room. But `consume_map` forgets
  the withheld PG at the next map unless the OSD is in the acting set, and
  for an acting set of the primary alone the forced pg_temp maps back to
  the same acting set. Reproduced on a vstart cluster; fixed in
  https://github.com/ceph/ceph/pull/72112.
- **`GetInfo` can finish without the info it asked for** (`pg_peering`,
  `tcScriptStaleNotify`). A notify a peer sent before its current
  `up_from` is discarded by `proc_replica_notify`, but `GetInfo` has
  already taken it as that peer's reply. When the peer is the only
  surviving OSD of an interval that went read-write, the primary goes
  active without that interval's acknowledged writes. Reproduced against
  the real `PeeringState` in `unittest_peeringstate`
  (`StaleNotifyCompletesGetInfo`).
- **Marking an OSD lost does not unblock a PG whose last activation only
  that OSD saw** (`tcScriptLostIncomplete`): the PG goes from `down` to
  `incomplete`, as documented.
- **The mgr's force-recovery, force-backfill and scrub requests abort an
  OSD whose PG is still in `Reset`** (`pg_statechart`, `tcCmdReset`). A
  PG stays in `Reset` after its OSD restarts until the OSD has a newer
  map, and `Reset` has no reaction for these events. Reproduced in
  `unittest_peeringstate`; fixed by listing them in `Reset`
  (`CommandInResetIsIgnored`).
- **A backfill grant from a released reservation round is counted in the
  next round** (`tcBf2StaleGrant`). The primary reaches `Backfilling`
  before every target has granted, and the late grant then aborts it.
  This is the rest of tracker 44248. Reproduced in `unittest_peeringstate`
  (`StaleBackfillGrantCountedInNextRound`).
- **A change of delete priority while the delete reservation's grant is
  queued aborts the OSD** (`tcDeleteRePriority`). Two `DeleteReserved`
  reach the PG; the second finds it in `Deleting`. Reproduced in
  `unittest_peeringstate` (`DeleteReprioritizedWithGrantQueued`).
- **A preemption of the local slot is dropped while the PG waits for its
  remote slots** (`tcRecSlotLost`, `tcBfSlotLost`). Recovery or backfill
  then runs outside `osd_max_backfills`.
- **Tracker 70670's `AdvMap` reactions are not registered**, and
  registering them as written crashes (`tcFixRecWait*Full`).

## Running

The models are not part of the Ceph build: they need the .NET 8 SDK and
the P tool, not the C++ toolchain.

```
brew install dotnet@8
export DOTNET_ROOT=/opt/homebrew/opt/dotnet@8/libexec
dotnet tool install --global P

./run.sh <model> [schedules]             # every case against <model>/expect.txt
./deep.sh <model> <test case> [schedules] # one case under random, PCT and POS
```

`p check -tc` runs every test case whose name starts with the one given,
so no case name may be a prefix of another: both scripts refuse one that is.

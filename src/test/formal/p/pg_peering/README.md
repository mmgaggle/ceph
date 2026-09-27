# Placement group peering: a P model

A model of how one replicated PG peers (`src/osd/PeeringState.cc`): the
monitor publishes OSDMap epochs, every OSD runs the peering state machine
over each epoch in order, an environment crashes, restarts and marks OSDs
down, out and lost, and a client writes. It asks two questions: can an
acknowledged write be lost, and can the PG get stuck inactive once the
cluster has settled?

## The properties

**AckedWritesSurvive.** A write acked by a primary that activated in epoch
A is in the log of every primary that activates a writeable acting set in
a later epoch, whether that activation happens before the ack (a stale
primary still acking) or after it. Marking an OSD lost releases the writes
acked before it, and anything an OSD acks after it has been marked lost.

**PgGoesActive** (liveness). Once the environment settles, the PG goes
active in its current interval, unless every OSD that ever held it has
been marked lost (only `ceph osd force-create-pg` helps then).

**WritesComplete** (liveness). Once the environment settles, every write is
acked, with the same exception.

**MapsSettle.** Once the environment settles, the maps stop changing within
40 epochs: no pg_temp or up_thru churn.

## What is modelled

- **`Mon`** (`OSDMonitor`): every change is a new epoch.
  - Boot: an OSD the map still shows up is first marked down; the booted
    OSD gets `up_from`, a new address, and the maps up to that epoch in
    the reply (`C_Booted`).
  - `MOSDAlive`: ignored from an OSD that is down or at an old address;
    otherwise `up_thru` becomes the epoch the OSD asked in.
  - `MOSDPGTemp`: ignored unless the sender is the acting primary now; it
    also raises the sender's `up_thru`. New maps drop a redundant or
    all-down pg_temp (`clean_temps`).
  - The environment's mark down, out and lost (`lost_at` = `down_at`,
    refused while up) and `min_size` changes.
  - It may hold a subscriber's maps back and deliver several at once.
- **`Osd`** (`PeeringState`, the parts of `OSD` and `PG` peering relies on):
  - every epoch applied in order (`AdvMap`), one `ActMap` per batch;
  - `Reset`, `Stray`, `ReplicaActive`, and the primary's `GetInfo`,
    `GetLog`, `GetMissing`, `WaitUpThru`, `Down`, `Incomplete`,
    `WaitActingChange` and `Active` (with `Recovered` and `Clean`);
  - past intervals with `maybe_went_rw` from `up_thru`/`up_from` and
    `last_epoch_clean`, `pi_compact_rep`'s superseding, `PriorSet` and
    `affected_by_map`;
  - `find_best_info` (including the `history.last_epoch_started` bound),
    `select_replicated_primary`, `calc_replicated_acting`, pg_temp
    requests, `proc_replica_notify`'s duplicate check,
    `update_history`, `merge_log`'s cut point and `copy_after`'s tail;
  - peering messages that wait for the map they were sent in and are
    dropped if the PG reset since (`old_peering_msg`); messages to an old
    address are lost;
  - `up_thru` requests (`queue_want_up_thru`) and pg_temp requests;
  - PG instances created from a notify or log only where the PG maps
    now, client ops parked while such a PG does not exist yet;
  - crash and restart: info, log, past intervals, maps and the PG's epoch
    are on disk; a crashed process is eventually marked down.
- **`Client`** (`Objecter`): sends each write to the acting primary of its
  newest map, resends on every interval change; the primary acks a write
  already in its log (reqid dup detection).
- **`Env`**: `chaos` random failure events among the enabled kinds and
  `writes` writes in random order, then it settles: every crashed OSD not
  marked lost restarts, lost OSDs are marked out. Or a script of steps,
  with "wait until a primary goes active in a newer interval".

## Configurations and results

`../run.sh pg_peering [schedules]` checks each case against `expect.txt`.

| Test case | Configuration | Result |
|---|---|---|
| `tcCrash3` | 3 OSDs, size 3 / min_size 2: crashes, restarts, false mark-downs | holds |
| `tcCrashMin1` | the same with min_size 1 | holds |
| `tcRemap` | size 2 / min_size 1, OSDs marked out and in | holds |
| `tcMinSize` | min_size changes | holds |
| `tcLostSafety` | 4 OSDs, size 2, crashed OSDs marked lost (safety only) | holds |
| `tcLostOverride` | the same with `osd_find_best_info_ignore_history_les` | holds, liveness included |
| `tcScriptLostIncomplete` | scripted: one OSD goes active alone and is then lost | **violated** (by design): the survivor stays `incomplete` |
| `tcScriptLostIgnoreLes` | the same script with the override | holds |
| `tcFlap` | size 2 / min_size 1: many false mark-downs, OSDs out and in | holds |
| `tcScriptStaleNotify` | scripted: a stray's notify from before it flapped reaches a lagging primary in `GetInfo` | **violated**: the primary activates without an acked write |
| `tcScriptMin2StaleNotify` | the same with size 3 / min_size 2 on 4 OSDs: two OSDs take the write, one stays down, the other flaps | **violated** |
| `tcFixScriptStaleNotify`, `tcFixFlap` | the same with the proposed fix (`getInfoKeepsRequest`) | holds |
| `tcBugNoUpThruGate` | a primary activates without waiting for `up_thru` | violated: an acked write is missing after the next activation |
| `tcBugNoStaleFilter` | no `old_peering_msg` filter | violated: a notify from before a reset stands in for a reply |

## What the model shows

- **GetInfo can finish without the info it asked for, and lose acked
  writes** (`tcScriptStaleNotify`). `GetInfo::react(MNotifyRec)` erases the
  sender from `peer_info_requested` before `proc_replica_notify` looks at
  the notify, and `proc_replica_notify` discards a notify the sender sent
  before its current `up_from` ("got info from down osd, discarding",
  added for #12990). The discarded notify still counts as the sender's
  reply. The scenario, replicated size 2 / min_size 1:
  1. osd.0 is marked down; osd.1 goes active alone and acks a write;
  2. osd.1 is marked down and out; osd.0 comes back as primary of [0,2]
     and waits in `Down` for osd.1, the only OSD of that interval;
  3. osd.0 falls behind on maps. osd.1 comes up (out, a stray) and
     notifies osd.0 in epoch e, then is marked down and comes up again;
  4. osd.0 takes the maps in one batch: the map of epoch e resets it
     (`affected_by_map`), so `last_peering_reset` is e and the notify gets
     past `old_peering_msg`; it enters `GetInfo` after the batch and
     queries osd.1 and osd.2. As in #12990, the queued notify is handled
     after the map advance;
  5. the notify from e erases osd.1 from `peer_info_requested` and is
     discarded; osd.2's reply completes `GetInfo`; `choose_acting` never
     sees osd.1's log and osd.0 activates without the write.

  With min_size 2 the same needs the other OSDs of that interval to be
  down (`tcScriptMin2StaleNotify`). The fix is to erase the peer only once `proc_replica_notify` has
  taken its info; with it the scenario and the random `tcFixFlap` hold.
  `src/test/osd/TestPeeringState.cc` has the same scenario against the
  real `PeeringState` (`StaleNotifyCompletesGetInfo`): on main it fails,
  osd.0 leaving `GetInfo` without osd.1's info and going active without
  the write ("got info ... v 6'1 ... from down osd.1 discarding", then
  `choose_acting want=[0,2]`); with the fix it and the rest of
  `unittest_peeringstate` pass.
- **Marking an OSD lost does not always unblock a PG**
  (`tcScriptLostIncomplete`). With min_size 1, osd.1 goes active alone in
  epoch 3 while osd.0 is down, then dies and is marked lost. osd.0 comes
  back as primary. Its prior set is satisfied (a lost OSD counts as up),
  but it learned `history.last_epoch_started` 3 from osd.1's interval, and
  no surviving complete copy has `last_epoch_started` 3, so
  `find_best_info` finds nothing and the PG stays `incomplete`. Marking
  osd.1 lost changes nothing: it was never in `blocked_by`, so
  `affected_by_map` does not restart peering. Only
  `osd_find_best_info_ignore_history_les` (or `ceph-objectstore-tool
  --op mark-complete`) gets it active. This is Ceph's documented
  behaviour, and the reason min_size 1 is dangerous.
- **The two mechanisms that keep acked writes safe are both necessary.**
  Without the `up_thru` gate an interval can go read-write without the
  OSDMap saying so, and the next peering skips it. Without the stale-message
  filter a reply to an earlier query completes `GetInfo`.
- **No stuck-inactive state otherwise.** With every OSD back after the
  failures, the PG always went active and every write was acked.
- **Getting the model right took Ceph's OSD-level rules**, each found as a
  spurious counterexample first: a booting OSD takes its maps from the
  boot reply (`C_Booted`) and only goes active if the newest map of a
  batch shows it up; `dispatch_context` sends nothing while the OSD is
  down or booting; `send_message_osd_cluster` sends nothing to a peer that
  restarted since the PG's epoch; a PG is instantiated from a notify or log
  only where it maps, and client ops wait for such a PG.

## Not modelled

- Log trimming, backfill and async recovery: every OSD is contiguous with
  the authoritative log, so `Incomplete` for want of a complete log,
  backfill targets and most pg_temp traffic do not arise.
- Missing objects, recovery and unfound objects.
- OSDMap trimming, map gaps and the past-interval bounds check.
- Erasure-coded pools, stretch mode, split and merge, PG deletion,
  reservations, the read lease (`WAIT`), `mon_max_pg_per_osd`.
- Transactions that commit after the messages that follow them are sent.
- A monitor that loses requests (the OSD resends only on a new mon
  session).

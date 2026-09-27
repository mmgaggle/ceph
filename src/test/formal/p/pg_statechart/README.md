# The PG state machines: a P model

A model of one replicated PG's whole `PeeringState` statechart
(`src/osd/PeeringState.{h,cc}`). It covers all 35 states and all 184
reactions. The chart is generated from the header, so every event arrives
in every state the way boost::statechart delivers it. The model runs the
chart over the parts of `PG`, `PrimaryLogPG` and `OSD` that feed it:

- client writes, repops and log trimming;
- log-based recovery (pull, push, `MissingLoc`, unfound objects);
- backfill (scan, digest, progress, finish, remove);
- the local and remote `AsyncReserver`s, with preemption;
- backfillfull OSDs;
- PG deletion;
- the mgr's force-recovery and scrub requests;
- the per-OSD PG limit;
- cluster-network link failures, heartbeats, failure reports and
  mark-downs.

It asks four questions:
- Can any event reach `Crashed` ("we got a bad state machine event")?
- Can any modelled `ceph_assert` fail?
- Can an acknowledged write be missing from an object once the PG is clean?
- Can the PG get stuck short of active+clean, or leak a reservation, once
  the cluster has settled?

`pg_peering` models peering alone in more detail of the monitor and the
interval history; this model reuses its peering (`Types.p`, `Acting.p`,
`Mon.p`) and adds everything after activation.

## The chart

`chartgen.py` reads `PeeringState.h` and `PGPeeringEvent.h`:

- `chartgen.py gen <src/osd> PSrc/Chart.p` writes the states, their outer
  and initial states, the events, and every state's reaction list in
  order (`Reaction(state, event)`). A `custom_reaction` whose event has no
  `react()` overload of its own resolves to the state's
  `react(const event_base&)`, which discards; `transition<event_base,
  Crashed>` (Initial, Reset, Started) matches any event.
- `chartgen.py dispatch <src/osd> PSrc/Pg.p` regenerates the dispatch
  region of `Pg.p`. Each custom reaction calls `R_<State>_<Event>`, and
  each state's entry and exit call `En_<State>`/`Ex_<State>`.
- `chartgen.py check <src/osd> PSrc/*.p` fails unless the model
  implements exactly the custom reactions the header declares.

`Pg.p`'s `Handle` is `PeeringState::handle_event`:
- an event goes to the innermost active state first, then outward until
  some state's reaction list names it;
- a transition exits every state inside the innermost common outer state,
  then enters the target and its initial states;
- events posted during a reaction run after it, in order.

Reaching `Crashed` is the OSD's `ceph_abort`, and a model failure.

## The properties

- **No `Crashed`, no failed `ceph_assert`.** The model checks the asserts
  that guard the modelled paths. Among them:
  - AsyncReserver's duplicate request;
  - `add_log_entry`'s version order and `do_repop`'s interval;
  - `PGLog::trim` against `last_complete`;
  - `pg_missing_t::got`;
  - `MissingLoc::rebuild`;
  - `recover_backfill`'s bookkeeping;
  - `do_scan`'s backfill target;
  - `start_peering_interval` while deleting;
  - `choose_acting`'s backfill and async recovery targets.
- **AckedWritesDurable.** When the PG goes clean, every write acked to the
  client is in every acting OSD's copy of its object. Marking an OSD lost
  releases the writes acked before.
- **PgGoesActive, PgGoesClean, WritesComplete** (liveness). Once the
  environment settles, the PG goes active and, without lost OSDs, clean,
  and every write is acked.
- **ReservationsReleased** (liveness). Once the environment settles, every
  local and remote reservation the PG requested is given back.
- **MapsSettle.** The maps stop changing after the cluster settles.
- **Slots** (`slotCheck`). Recovery, backfill and deletion run while the
  PG holds its local reservation.

## What is modelled

- **`Pg`**: the chart and every custom reaction, state entry and exit.
  - Peering as in `pg_peering`. Activation with `MissingLoc`
    (`add_active_missing`, `add_source_info`, `search_for_missing`,
    `discover_all_missing`).
  - **Writes.** A write waits while the PG is not peered or not active,
    while its object is missing or degraded (with `maybe_kick_recovery`),
    and while its object is being recovered.
    - `pre_submit_op` updates the peers' info and async targets' missing.
    - `should_send_op` sends log-only ops to backfill and async targets.
    - Repops complete in order; `pg_committed_to` and
      `calc_trim_to_aggressive` drive trimming.
    - A resent write already in the log is acked from the log.
  - **Recovery.** `start_recovery_ops` runs `recover_replicas`,
    `recover_primary` and `recover_backfill`, and finishes with
    `AllReplicasRecovered`, `RequestBackfill` or `Backfilled`.
    - Pulls handle failure (`on_failed_pull`) and cancellation when the
      source goes down.
    - Pushes use `on_peer_recover` and `on_global_recover`.
    - `find_unfound` posts `UnfoundRecovery`/`UnfoundBackfill`.
  - **Backfill.** Scans and digests (the target posts `BackfillTooFull`
    when full), `prep_backfill_object_push`, removals, and the
    progress/finish/ack exchange.
  - **Reservations.** Every local and remote request, cancel and priority
    update, and every `MRecoveryReserve`/`MBackfillReserve` op with its
    event, including `try_reserve_recovery_space`.
  - **Deletion.** `purge_strays` and `DeleteStart`, then `ToDelete`,
    `WaitDeleteReserved` and `Deleting`, one object per `DeleteSome`,
    until the PG is gone. The delete priority is recomputed on every
    `ActMap`.
  - **Delivery.**
    - Epoch filtering at dequeue (`old_peering_evt`).
    - Data messages are dropped by `can_discard_replica_op`, `_scan` and
      `_backfill`, and wait for `min_epoch`.
    - Replica ops wait in `waiting_for_peered` until the PG is peered;
      a pull is served while inactive.
    - Timers (retry intervals) are events with no delay: they may fire
      at any point.
- **`Osd`**: maps, boot, addresses and the send rules as in `pg_peering`.
  - **The per-OSD PG limit.** `maybe_wait_for_max_pg` withholds creating
    a PG past `mon_max_pg_per_osd × osd_max_pg_per_osd_hard_ratio` (other
    PGs are a count). `consume_map` discards a withheld create whose acting
    set lacks the OSD. `resume_creating_pg` forces a twiddled pg_temp once
    there is room.
  - **The cluster network.** A link can fail one way or both.
    - The messenger is lossless: it queues for an unreachable peer, resends
      when the link heals, and drops the queue once the map shows the peer
      down.
    - Heartbeats fail when a link fails either way. After the grace an
      active OSD reports a peer that is up (`MOSDFailure`), and cancels the
      report when the peer answers again or when it becomes active itself.
    - An OSD marked down while running waits until it is healthy
      (`osd_heartbeat_min_healthy_ratio`) before it boots again, and shuts
      down after `osd_max_markdown_count` mark-downs.
  - The local and remote `AsyncReserver` (`osd_max_backfills` slots,
    priority order, first come first served within a priority, preemption
    of the lowest preemptible item, callbacks queued).
  - The backfillfull state.
  - `handle_fast_force_recovery` and `handle_fast_scrub`, whatever state
    the PG is in.
- **`Mon`**: as in `pg_peering`, plus:
  - the FULL state in the map;
  - failure reports: `mon_osd_min_down_reporters`, each OSD its own host,
    and `mon_osd_min_up_ratio`;
  - forced pg_temp from any OSD.
- **`Client`**: writes to one of `nObjects` objects, resent on every
  interval change.
- **`Env`**: random failures, as in `pg_peering`, plus:
  - an OSD going full and back;
  - another PG preempting an OSD's slots, or holding them (`busy`);
  - the mgr's commands;
  - other PGs coming and going on an OSD;
  - links failing and healing.

  A scripted setup may run first. Its steps can also wait for a PG to
  enter a given state.

Not modelled: erasure-coded pools, read leases (`MLease`, `RenewLease`,
`CheckReadable`), scrub itself, snapshots, splits and merges, `QueryState`
and `QueryUnfound`, and `mark_unfound_lost`.

## Configurations and results

`../run.sh pg_statechart [schedules]` checks each case against `expect.txt`.

Random failure configurations run with every proposed fix, so what they find
is new. The other configurations run Ceph as it is unless named `tcFix*` or
`tcHalfFix*`.

| Test case | Configuration | Result |
|---|---|---|
| `tcCrash2`, `tcCrash3` | 3 OSDs, size 2 / min_size 1 and size 3 / min_size 2: crashes, restarts, false mark-downs | holds |
| `tcRemap` | OSDs marked out and in | holds |
| `tcFull`, `tcPreempt` | OSDs going backfillfull; other PGs preempting reservations | holds |
| `tcAsync` | async recovery (`osd_async_recovery_min_cost` 0) | holds |
| `tcLostSafety` | 4 OSDs, crashed OSDs marked lost (safety only) | holds |
| `tcBf*`, `tcRec*`, `tcBf2*` | a scripted backfill (one or two targets) or log recovery, then full OSDs, preemption or crashes | holds |
| `tcRecWaitLocalFull`, `tcRecWaitRemoteFull` | an OSD goes full while the PG waits for its reservations | holds |
| `tcCommands`, `tcCmdReset` | mgr commands reach a PG in `Reset` | violated: `Crashed` |
| `tcBf2StaleGrant` | a stale backfill grant | violated: `Crashed` |
| `tcDeleteRePriority` | delete priority changes with the grant queued | violated: `Crashed` |
| `tcRecSlotLost`, `tcBfSlotLost` | a preemption while waiting for remote slots | violated: no local slot |
| `tcFixRecWaitLocalFull`, `tcFixRecWaitRemoteFull` | tracker 70670's reactions registered | violated: `Crashed` |
| `tcMaxPgBackfillTarget`, `tcMaxPgSingle` | a backfill target at the PG limit | violated: stuck activating |
| `tcMaxPgResume` | a withheld creation, resumed | holds |
| `tcFix*` | the proposed fixes, scripted and random | holds |
| `tcHalfFixRecSlotLost`, `tcHalfFixBfSlotLost` | `deferWhileWaiting` without `grantFromCheck` | violated: `Crashed` |
| `tcCuts2`, `tcCuts1` | cluster links fail and heal, with 2 or 1 reporters | holds |
| `tcCutPersist2`, `tcCutPersist1` | a link between the acting OSDs stays down | violated: stuck (by design) |

Before the PG-limit and network cases were added, every `holds` case ran
20,000 schedules each under random, PCT and POS scheduling
(`../deep.sh`) with no failure. The PG-limit and network cases have run
300 to 20,000.

## Findings

Four were reproduced against the real `PeeringState` with
`unittest_peeringstate` tests, which go in with their fixes:
- `CommandInResetIsIgnored` (with the fix);
- `StaleBackfillGrantCountedInNextRound`;
- `DeleteReprioritizedWithGrantQueued`;
- `StaleNotifyCompletesGetInfo`, from `pg_peering`.

The PG-limit finding was reproduced on a vstart cluster, and is fixed in
https://github.com/ceph/ceph/pull/72112 with a `qa/standalone` test.

- **The mgr's commands crash a PG still in `Reset`** (`tcCmdReset`,
  `tcCommands`; confirmed in C++).
  - Cause: a PG loaded at boot stays in `Reset` until the OSD has a newer
    map (`PG::read_state`, and `advance_pg` returns early when the PG is
    at the OSD's epoch). `handle_fast_force_recovery` and
    `handle_fast_scrub` queue their events whatever the PG's state.
    `Started` discards `Set/UnsetForce*` and `RequestScrub`; `Reset` does
    not list them, so its catch-all goes to `Crashed`.
  - Effect: `ceph pg force-recovery`, `force-backfill`, `scrub`,
    `deep-scrub` or `repair` for a PG whose primary has just restarted
    aborts that OSD.
  - Fix: list the five events in `Reset` (`resetIgnoresCommands`).
- **A stale backfill grant is counted in the next reservation round**
  (`tcBf2StaleGrant`; confirmed in C++).
  - Cause: with two backfill targets, the primary asks osd.1, gets its
    grant, and asks osd.2. osd.1 revokes (preempted) and the primary
    retries, releasing both. osd.2 grants the first round before it sees
    the release. The primary, in the next round's
    `WaitRemoteBackfillReserved`, takes that grant for the target it is
    waiting on.
  - Effect: it reaches `Backfilling` before osd.2 has granted again, and
    osd.2's real grant then aborts it: `Backfilling` has no reaction for
    `RemoteBackfillReserved`.
  - Timing: in a real cluster the retry waits `osd_backfill_retry_interval`
    (30 s), so the grant must arrive later than that.
  - History: this is what tracker 44248 (61152ac2965) patched for
    `WaitLocalBackfillReserved` only. The `RELEASE_ACK` its commit message
    calls the long-term fix was never added.
- **A preemption of the local slot is dropped while the PG waits for
  remote slots** (`tcRecSlotLost`, `tcBfSlotLost`).
  - Cause: `Active` discards `DeferRecovery`/`DeferBackfill`. In
    `WaitRemote*Reserved` the PG goes on to `Recovering`/`Backfilling`
    without the slot the reserver took back and gave to higher-priority
    work. The same happens to a replica's `REVOKE` while the primary
    waits.
  - Effect: the OSD runs more than `osd_max_backfills`, and the
    preemption's purpose is lost. No crash.
- **Changing the delete priority while the grant is queued aborts the
  OSD** (`tcDeleteRePriority`; confirmed in C++).
  - Cause: `ToDelete::react(ActMap)` restarts `ToDelete` when
    `get_delete_priority()` changes (the OSD went nearfull, backfillfull or
    full). `WaitDeleteReserved` cancels and requests again, but
    `AsyncReserver` had already handed the first grant to its finisher,
    and cancelling an in-progress item does not recall it.
  - Effect: two `DeleteReserved` reach the PG. The second finds it in
    `Deleting`, which has no reaction for it.
- **A backfill target at the PG limit leaves the PG stuck `activating`**
  (`tcMaxPgBackfillTarget`; confirmed on a vstart cluster).
  - Cause: an OSD at the per-OSD PG limit withholds creating a PG and
    remembers it in `pending_creates_from_osd`. Once it has room,
    `resume_creating_pg` forces a pg_temp change so the primary peers again
    and resends. But `OSD::consume_map` discards the pending create at the
    next map unless the OSD is in the PG's acting set
    (`get_pg_acting_role`). A backfill target (or async recovery target)
    is in up only.
  - Effect: after any new map the create is gone. The primary waits in
    `Activating` for the target's `ActivateCommitted` forever, even once
    the limit is raised or PGs are removed: `activating+remapped` until
    something else changes the interval (`ceph pg repeer`, an OSD restart).
  - Reproduction (4 OSDs):
    1. pool `a` with data and a short log, so a new OSD must be backfilled;
    2. `osd.2` at the limit;
    3. `ceph osd pg-upmap-items` moves `a`'s PG onto `osd.2`, which logs
       `withhold creation of pg`;
    4. `ceph osd set noout` produces a new map, and `osd.2` logs
       `doesn't map here, discarding pending_create_from_osd`;
    5. `ceph tell osd.2 config set mon_max_pg_per_osd 1000`.
  - Result: the PG stays `activating+remapped`. Without step 4, or with the
    fix, it goes `active+clean` within seconds.
  - Fix: `OSDMap::is_up_acting_osd_shard()`, the same test the op path
    uses to decide the PG maps to the OSD.
  - The upstream `osd_max_pg_per_osd` qa task misses this: its pools hold no
    data, so the new OSD joins acting without backfill.
  - A second way to the same state (`tcMaxPgSingle`; confirmed on vstart):
    with the primary alone in the acting set, `twiddle()` requests pg_temp
    `[primary, NONE]`. `_get_temp_osds()` drops the `NONE` for a replicated
    pool, so the acting set and the interval stay the same, and the record
    is already erased. The fix gives the twiddle a second up OSD, as the
    monitor's `pg repeer` does.
  - The fix's `qa/standalone/osd/osd-max-pg-backfill-target.sh` covers
    both cases.
- **Tracker 70670's fix is dead code, and registering it crashes**
  (`tcFixRecWaitLocalFull`, `tcFixRecWaitRemoteFull`).
  - `WaitLocalRecoveryReserved::react(AdvMap)` and
    `WaitRemoteRecoveryReserved::react(AdvMap)` exist but are not in the
    states' reaction lists.
  - Registered as written, the local one leaves for `NotRecovering` with
    its reservation still queued, and the grant later reaches
    `NotRecovering`. The remote one posts `RecoveryTooFull`, which
    `WaitRemoteRecoveryReserved` does not handle.

Partitions that heal leave nothing stuck (`tcCuts2`, `tcCuts1`).

A link that stays down between two acting OSDs is a limitation by design
(`tcCutPersist2`, `tcCutPersist1`):
- With `mon_osd_min_down_reporters` 2, each OSD has one reporter, so
  neither is marked down. The PG never finishes peering, and writes hang.
- With 1, the pair flap until one shuts itself down
  (`osd_max_markdown_count`). If that one took writes alone while the
  other was down, the PG stays `down` until it returns.

Candidate fixes in the model:
- **`grantFromCheck`:** a grant counts only for the request outstanding.
  The remote grant must come from the shard last asked, reserver callbacks
  carry the number of the request they answer, and `Active` discards
  others. This fixes the stale grant and the delete race.
- **`deferWhileWaiting`:** `WaitRemote*Reserved` give up on a preemption,
  and `RepWaitRecoveryReserved` on a release.

`deferWhileWaiting` alone (`tcHalfFix*`) turns lost slots back into
stale-grant crashes. Both together hold. The random configurations run
with every proposed fix, so what they find is new.

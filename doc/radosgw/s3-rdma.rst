.. _radosgw_s3_rdma:

============
S3 over RDMA
============

.. versionadded:: Umbrella

S3 over RDMA keeps the S3 control plane on HTTP and moves object data
out of band. Authentication, headers and metadata travel in the HTTP
request and response. The object bytes go directly into memory that the
client registered with its network adapter, and that memory can be GPU
memory.

.. contents::
   :local:
   :depth: 2

Overview
========

Clients
-------

The gateway serves three kinds of client. They differ in how they
describe the memory that receives the data.

cuObject clients
  A client that uses NVIDIA's cuObject client library
  (``libcuobjclient``) registers a memory window. It sends a descriptor
  for the window in the ``x-amz-rdma-token`` header of an ordinary S3
  request. The descriptor names a Dynamically Connected (DC) target,
  which exists only on NVIDIA ConnectX adapters. Any server that holds
  the descriptor and the shared DC key can write into the window
  without a connection to the client. NooBaa's ``s3perf.js --rdma`` and
  AWS SDK middleware for cuObject are examples.

libfabric clients
  A client that uses libfabric registers a window with a libfabric
  provider. It sends a libfabric token in ``x-amz-rdma-token``. The
  token names the provider, the client's endpoint and the memory key.
  Any server process that runs the same provider can write into the
  window without a connection to the client. The provider selects the
  wire: ``tcp``, ``shm``, RDMA reliable connections, AWS SRD or Ultra
  Ethernet.

Reliable Connection clients
  AMD's hipObject library is an example of a client that uses Reliable
  Connection (RC) transport. Such a client pairs a queue pair with the
  gateway through the ``hipobj-rc-v2`` control protocol on
  ``/.hipobj-rc/``. A queue pair
  is the RDMA endpoint of one connection. RC exists on every RDMA
  adapter. Only the paired gateway can write to the client, so the
  gateway relays the data.

Data paths
----------

Object data takes one of two paths, whatever the client's transport.

OSD-direct
  The OSDs write each stripe straight into a window. For cuObject and
  libfabric clients, the window is the client's own memory, and the
  object data never passes through the gateway. This is OSD passthrough
  mode, and its bandwidth grows with the number of OSDs. The gateway
  needs no RDMA adapter and no transport library for it. For RC
  clients, the window is a gateway session buffer. The gateway writes
  the buffer to the client over the paired queue pair while the
  stripes arrive. This is OSD-direct relay. ``rgw_rdma_osd_passthrough``
  turns on both.

Gateway-staged
  The gateway reads the object from RADOS into a registered buffer and
  writes it to the client over the client's transport. For cuObject
  clients, the gateway's cuObject server writes the buffer in one
  transfer. For RC clients, the gateway writes the session buffer while
  the stripes arrive, which is staged relay. Libfabric clients have no
  gateway-staged mode yet, so they fall back to HTTP. Every PUT that
  the gateway serves out of band is gateway-staged, for cuObject and RC
  clients. An upload must pass through the gateway's checksum,
  compression and encryption filters.

The gateway uses OSD-direct when it can, and gateway-staged otherwise.

Transports
----------

An OSD reaches a window through an executor. An executor is a
transport that writes the bytes of a read into the window that a token
names. ``osd_oob_transports`` lists the executors that an OSD starts,
and ``rgw_rdma_transports`` lists the transports that the gateway
starts. Both name the transports ``ofi`` and ``cuobj``. Two executors
exist:

cuObject executor
  Writes to DC descriptors with NVIDIA's ``cuobjserver`` library. It
  needs a ConnectX adapter on the OSD host.

libfabric executor
  Writes to libfabric tokens with any libfabric provider.

Either executor also carries the transfers that stay inside the
cluster, into windows of its own transport: shard reads that an
erasure-coded primary gathers, and stripes into the gateway's relay
windows.

Every out-of-band read is advisory. When a daemon cannot deliver read
data out of band, it sends the same bytes in band, and the request
still completes with correct data. A gateway-staged PUT is not: when
the gateway cannot take the object from the client's memory, as when
no staging buffer holds it, the PUT fails.

How a read reaches client memory
================================

The delivery descriptor
-----------------------

A RADOS read can carry a delivery descriptor. The descriptor belongs to
one operation in the request, so each read in a compound request can
carry its own. It holds these fields:

* ``token``: the client's token. The OSD reads only enough of it to
  choose an executor, and the executor reads the rest. See
  `Executors and tokens`_.
* ``base_offset``: the offset in the window where the first byte of the
  operation's data goes.
* ``flags``: requests, such as a CRC-64/NVME of the delivered bytes,
  and ``FLAG_PRIOR_SETTLED``, which the RADOS client sets on a resend.
  See `Retries`_.

librados clients set it with ``ObjectReadOperation::set_rdma_delivery()``.
The RADOS client sends descriptors only when the cluster's
``require_osd_release`` is Umbrella or later. Otherwise every read is
inline.

An OSD that delivers the data out of band writes it into the window and
reports the byte count in the operation's result. An OSD that cannot or
will not deliver it replies with the data inline, as if the request had
no descriptor. An OSD replies inline in these cases:

* No running executor serves the token.
* The pool's ``rdma_delivery_lease`` expired before the transfer
  started, or too little of the pool's ``rdma_delivery_drain`` is left
  to finish it. See `Fencing a window before reuse`_.
* The request is a retransmission, and the RADOS client does not vouch
  that every earlier attempt settled. RADOS resends reads after peering
  changes, and an inline reply makes sure that a stripe is never
  written twice. See `Retries`_.
* The OSD's PG read lease (``readable_until``) lapsed. See
  `Placement plans`_.
* The descriptor has flag bits that the OSD does not know.

With an inline reply, the OSD also says whether it started a transfer
for the operation at all. When it did not, as in the cases above, or
when its executor sent nothing (no staging buffer, a budget already
spent, no address for the client yet), the result is marked declined:
nothing of the operation reached the window. When the OSD delivered the
data over a transport whose writes are delivery-complete, the result is
marked landed: every byte was in the window before the reply. The RADOS
client marks a result resent when it sent the operation more than once,
since an earlier attempt may have started a transfer that this result
knows nothing of, unless every earlier attempt settled. librados
reports these as ``RDMA_DELIVERY_DECLINED``, ``RDMA_DELIVERY_LANDED``
and ``RDMA_DELIVERY_RESENT``. An OSD of an older release marks results
neither declined nor landed.

Retries
-------

An attempt is settled when its reply marked every operation that
carries a descriptor declined or landed. No write of a settled attempt can land
after its reply.

Reads bounce routinely. A replica whose PG read lease lapsed answers a
balanced read with ``-EAGAIN``. So does a replica whose read failed
with ``-EIO``, and the shard of an erasure-coded shard-direct read
whose read failed with ``-EIO``. The RADOS client then sends the read
to the primary, which reads another copy or reconstructs the data from
the other shards. An OSD that answers a request with an error before it
executes the reads marks every operation that carries a descriptor
declined.

When every earlier attempt of an operation settled, the RADOS client
sets ``FLAG_PRIOR_SETTLED`` on the descriptors of the resend. The OSD
then delivers the resend out of band, as it would a first attempt, and
the result is not marked resent. An attempt that got no reply, as when
the RADOS client resent it after a map change or a session reset, makes
every later attempt a plain resend. An OSD that does not know the flag
delivers the resend inline.

A split read that ends in ``-EAGAIN``, because a sub-read bounced or
the sub-replies mixed inline and out-of-band data, is retried at the
primary. The retry is a first attempt when every sub-read settled, and
a resend otherwise.

Executors and tokens
--------------------

An OSD starts the executors that ``osd_oob_transports`` lists. It
skips, with a warning, a transport that it was built without or that
fails to start. It chooses an executor for each token by the shape of
the token. A token whose third field is ``ofi1`` goes to the libfabric
executor, but only when the token names the OSD's own provider
(``osd_ofi_provider``). A token without that field goes to the cuObject
executor. A libfabric token for another provider, and any token that no
running executor serves, is delivered inline.

Placement plans
---------------

The OSD turns each read into a placement plan. A plan maps ranges of the
reply to offsets in the window. The shape of the plan follows the read:

* A plain read has one range.
* A sparse read has one range per extent. The extent map stays inline.
* An erasure-coded primary read returns reconstructed logical data, so
  it has one range.
* An erasure-coded shard-direct read has one range per chunk. Each
  shard OSD writes its chunks to their logical positions in the window.
  The writes of the shards interleave in the client's buffer, so the
  client does not reassemble anything.

The OSD copies the reply into a registered staging buffer, writes each
range, and waits until every write completes. Only then does it send
the reply. Over a transport whose writes are delivery-complete, a reply
therefore means that the bytes are in the window. The cuObject executor
does not promise that, so its results are never marked landed.
The OSD waits at most until the deadline that
`Fencing a window before reuse`_ describes. It cuts off the writes that
are still in flight then, and delivers the read inline.

Before an OSD starts a transfer, it reads its PG read lease
(``readable_until``) again. The readability test at dispatch does not
cover a read that stalled after dispatch. An OSD whose lease lapsed,
such as a primary that lost contact with its peers, replies inline and
marks the result declined, instead of writing into a window that a new
acting set can already serve.

Gateway behavior
================

Passthrough eligibility
-----------------------

A GET uses passthrough only when the gateway does not need to touch the
data:

* The object is not compressed and not encrypted.
* No Lua data script or Arrow Flight filter applies.
* The object is not a Swift DLO or SLO manifest.
* The D3N data cache is off.
* The requested range fits in the client's registered window.

Multipart objects and range requests are supported. Every stripe lands
at its logical offset in the requested range. Replicated and
erasure-coded pools are supported. See `Erasure-coded pools`_.

Fallback order
--------------

The gateway tries these modes in order, for each request:

#. Passthrough, when the request has a token, ``rgw_rdma_osd_passthrough``
   is on and the request is eligible. If any stripe comes back inline,
   the gateway restarts the whole GET in the next mode. The client does
   not see the restart, because the gateway has not sent HTTP bytes
   yet. When stripe operations were already sent, the gateway first
   waits for the fence described in
   `Fencing a window before reuse`_, unless no write of the request can
   land any more: every stripe came back declined or landed, and none
   was resent. One OSD that does not deliver out of band then costs a
   re-read over HTTP, not the fence. A stripe read that fails fails the
   GET instead, and the gateway sends the error behind the same fence.
#. Gateway-staged mode, when the token is a cuObject descriptor, the
   gateway runs its cuObject server (``cuobj`` in
   ``rgw_rdma_transports``), and one of its staging buffers is free and
   holds the whole response (``rgw_cuobj_buffer_size``, 8 MiB by
   default).
#. The HTTP body, with ``x-amz-rdma-reply: 501``. The cuObject protocol
   defines this value as the signal to fall back to HTTP.

When the data went out of band, the response carries
``x-amz-rdma-reply: 200`` and ``x-amz-rdma-bytes-transferred``.

The gateway limits how much data the OSDs aim at one client at a time
with ``rgw_get_obj_window_size`` (default 16 MiB).

Fencing a window before reuse
-----------------------------

A token gives write access to the window until the client deregisters
the window. Three rules keep a stale write out of a window that is in
use again:

* The gateway drains every outstanding stripe operation before it
  responds. An OSD finishes its writes before it sends the operation's
  reply. A drained reply is therefore the interlock for every OSD that
  is still in contact.
* An OSD delivers a retransmitted request inline, unless every earlier
  attempt of it settled, so that none of them can still write the
  window. See `Retries`_.
* Two pool options bound every transfer, and the OSD enforces both.
  An OSD takes on a transfer only within ``rdma_delivery_lease`` of
  receiving the operation. Every write of it lands, or is cut off,
  within ``rdma_delivery_drain`` after the lease, also a write that the
  executor posts after the lease, as when it waited for a new peer.
  This covers an OSD that disappears during the request, and an
  operation that the gateway's RADOS client sent again. Before a
  fallback rewrites the window, and before it answers the client with
  an error, the gateway waits for the lease plus the drain, which it
  reads from the OSDMap, counted from when the last stripe operation
  completed. It skips the wait when every stripe came back declined, so
  no transfer started, or landed, so every write completed before the
  reply, and no stripe was resent. A stripe that was resent, one an OSD
  started a transfer for and then returned inline (a push cut off), one
  from an older OSD, and one without a result, as when its read timed
  out, keep the wait. A stripe counts as resent only when an earlier
  attempt of it may still write: a resend whose earlier attempts all
  settled is not marked resent, and neither is a split read that the
  primary redoes after every sub-read settled. See `Retries`_. Before a
  success, every stripe's OSD placed its bytes before replying, and the
  gateway waits only when a stripe was resent: its earlier attempt may
  still write. The window is then quiet before it is written again, and
  before the client hears back.

How an OSD cuts off a write depends on the transport:

* The libfabric executor cancels only the late transfer's writes, and
  keeps its endpoint, when the provider promises that a cancelled write
  is discarded: no packet of it is sent once the cancel returns. The
  UET provider says so through an endpoint option. Other transfers on
  the OSD, to other clients and for gathers, go on. Without that
  promise, or when a cancel fails, the executor closes its endpoint and
  opens a new one. A libfabric provider discards the operations of an
  endpoint that closes. That cut-off also fails the other writes in
  flight on the endpoint, and those reads are delivered inline. ``ofi
  status`` counts both kinds: ``plans_cut_off`` and ``cutoffs``.
* cuObject cannot cancel a posted write. A posted write keeps retrying
  for the retry budget of the DC transport, about two seconds. So the
  cuObject executor stops waiting that long before the deadline, and
  does not start a transfer when less than that is left.
* The ``tcp`` provider cannot cut off a write. Closing a socket does not
  discard the bytes that the kernel already queued on it, and the
  kernel delivers them later. The OSD logs a warning at startup. Use
  ``tcp`` for tests only.

The libfabric executor stops waiting for its writes early enough to cut
them off within the drain: by what a cut-off is expected to take, plus a
scheduling slack. The first guess of the cost is 100 ms. Each clean
cut-off then moves the estimate toward twice what it took plus 20 ms:
at once when that is more, and a quarter of the way when it is less.
The estimate never exceeds 2 s. Closing and reopening an endpoint takes
well under 1 ms on ``tcp`` and on the UET reference provider, so the
estimate falls to about 20 ms over a dozen cut-offs. The slack covers a
thread that the scheduler did not run in time. It starts at 50 ms, and
each cut-off moves it the same way toward twice how late the cut-off
started, plus 5 ms. It never drops below 10 ms. Any thread of the
executor that polls its endpoint also cuts off every transfer whose
time has come, so that one idle thread does not delay a cut-off;
``cutoffs_on_behalf`` counts those. A transfer whose budget is no
longer than the cost and the slack together is delivered inline, and
``budget_refused`` counts it. A transfer that has too little budget
left when it is about to start is also delivered inline, before it
sends anything, and ``late_starts`` counts it.
Starting it would only cut it off at once, and every other write in
flight with it.

A cut-off can still end after the budget it protects, usually because
it started late on a loaded host. Whoever waits for the OSD's reply is
not affected: the reply follows the cut-off. That covers a client that
receives a response, and the gateway's fallback, which waits the lease
plus the drain after every operation has completed. Only a client that
gives a request up, and counts the fence from when it sent the request,
needs to allow for it. So a cut-off that ends late by no more than
``osd_oob_cutoff_late_tolerance`` (1 s by default), and that itself
took no longer, is counted in ``cutoffs_late``, and the OSD raises the
``OOB_CUTOFF_LATE`` health warning, with the worst lateness, for
``osd_oob_cutoff_late_alert_period`` (an hour by default) after the
latest one. Delivery goes on. 1 s is a third of the default drain, and
thousands of times what a cut-off costs, so a cut-off that late is a
scheduling delay, not a transport that fails to discard.

A cut-off can also end later than the tolerance although the close or
cancel itself was quick. It then started late because the OSD's threads
did not run: the process was stopped (``SIGSTOP``), paused for a long
time, swapped out, or starved, or its virtual machine did not run. The
OSD says so when nothing polled its endpoint for about as long. The
endpoint is clean once the close or cancel returns: nothing of the
writes it cut off is sent any more. So the OSD logs the cut-off to the
cluster log as an error, with its lateness, raises the
``OOB_CUTOFF_PAST_TOLERANCE`` health error for
``osd_oob_cutoff_late_alert_period`` (an hour by default), and goes on
delivering. Stopping delivery would not undo a write that already
landed, and it would turn every long pause into an outage until the OSD
restarts. Set ``osd_oob_cutoff_late_fail_closed`` to stop delivery
instead.

Who can such a late write reach? Say the OSD received the operation at
``t``. Its writes must land by ``t + lease + drain``, and one landed
``L`` later than that:

* A client that received a response for the request is not affected.
  The OSD replies after the cut-off, the gateway responds after the
  OSD's reply, and the client reuses its window after the response.
* The gateway's fallback is not affected. It waits lease plus drain
  after every operation completed, so past the OSD's reply, which
  follows the cut-off.
* A fallback after the Objecter resent the operation, whose first
  attempt went to the paused OSD, is covered when ``L`` is shorter than
  the time from ``t`` to when the resent operation completed. The
  Objecter resends only once the paused OSD is marked down, after
  ``osd_heartbeat_grace`` (20 s by default) of silence. A pause shorter
  than that does not lead to a resend at all. And over a software
  provider, such as the UET reference provider, nothing moves while the
  OSD's threads do not run, since it sends only while polled. Only a
  longer pause, over a transport that moves data without the OSD's
  threads, such as rocm-ernic's emulated UET engine or a NIC, can let a
  write land after this fence.
* A client that gave a request up, and reused its window lease plus
  drain plus the tolerance after sending it, can see its window written.
  The health error tells an operator that this was possible.

A cut-off can fail. The provider may fail to close the endpoint, so its
writes go on. Or the close or cancel itself may take longer than the
tolerance, as when a provider drains its writes instead of discarding
them, so that every cut-off would be late. In these cases writes may
land in a client's window after the fence, and may keep doing so. The
OSD then stops delivering out of band. It delivers every read inline, logs the
cause to the cluster log, and raises the ``OOB_DELIVERY_UNSAFE`` health
warning. With ``osd_oob_cutoff_failure`` set to ``abort``, the OSD exits
instead. That ends the writes of a software provider, and a device
drops the queue pair's. Restart the OSD once the transport is fixed.
``OOB_DELIVERY_DOWN`` means that a cut-off could not reopen the endpoint;
no write is at risk then, but delivery has stopped.

The lease and the drain bound the OSD side only. They are not a bound
on how long a client keeps a window registered. Both are measured
against the wall clock, so they are a best-effort fence across clock
steps. Give them some slack.

Reusing a window
----------------

The fence covers writes that an OSD cut off. It does not cover a
duplicate of a write that already completed. A provider that retransmits
without connection state at the target, as UET's RUDI mode does, can
deliver a copy of a packet after the write it belongs to has completed,
and after the response that reported the bytes placed. The target keeps
no record of the write, so it places the copy wherever the copy's key
points. Copies can arrive until the network's longest packet lifetime
has passed. That is short on a healthy fabric, but it grows with every
queue a packet waits in. A soak test with 1.5 s of delay on a client's
link saw copies land 300 ms after their GET completed.

So a window owner must not reuse a window's memory while a copy of an
earlier write can still arrive under the window's key. A copy can land
whenever the key is valid, also while the window sits idle between
requests, and while its owner writes new data into it. Re-keying just
before the next request is therefore too late. The owner must do one
of these:

* Retire the key as soon as the request ends, when the response has
  arrived, and before the memory is read for another use, written, or
  lent again: deregister the memory region and register it again,
  under a new key. A write that carries the old key then fails instead
  of landing. This is the usual RDMA practice, and it is cheap on a
  software provider. Make sure that the provider does not hand the old
  key out again soon. The UET provider can also change a region's key
  in place, with ``fi_control(&mr->fid, FI_UET_MR_REKEY, &key)``: the
  old key is dead when the call returns, the region keeps its
  registration, and over a device it is one command instead of
  unpinning and pinning the memory again. The data of the request itself can still be
  read before the key is retired: a copy carries the same bytes to the
  same place.
* Leave the memory untouched, and issue no new token for it, until the
  network's longest packet lifetime has passed since the response.

A request can also end without a response: the connection to the
gateway resets, the gateway exits, or the client times out. The OSDs
may then still write into the window, as `Fencing a window before
reuse`_ describes: until the pool's ``rdma_delivery_lease`` plus its
``rdma_delivery_drain`` after the request was sent, plus the OSDs'
``osd_oob_cutoff_late_tolerance`` for a cut-off that ends late, plus
the time the request took to reach the OSDs, which count from receipt.
Copies of those writes can follow until the packet lifetime after
that. The owner must retire the
key at once, or leave the window untouched until then. A soak test that
killed the gateway saw OSD writes land in clients' windows 50 to 350 ms
afterwards. That is within the contract, but a client that reuses the
window at once sees its new data overwritten.

With UET there is a third way. Writes into a region that its owner did
not mark ``IDEMPOTENT_SAFE`` use RUD instead of RUDI. RUD keeps state
for each initiator at the target, and places no packet twice. A window
owner that sets ``FI_UET_RUDI=0`` in its environment registers its
windows that way, and every OSD then writes them with RUD. It costs
throughput, which falls apart once the round trip grows past the
retransmit timeout (``FI_UET_TX_TIMEOUT``, 200 ms by default): every
packet is sent again before its acknowledgment can arrive. On a test
cluster with 300 ms of delay on one OSD's port, transfers through it
kept missing their deadlines: 94 cut-offs in three minutes, and, before
late writes could be cut off one at a time, about 90% of GETs from
every client fell back to HTTP.

Ceph does the first for the windows it lends itself, as soon as the
operation that used a window ends, in place where the provider can. An
OSD's libfabric gather windows get a new key when the gather releases
them (``osd_oob_rekey_windows``), and so do the gateway's libfabric
relay windows when a session ends (``rgw_rdma_rekey_windows``), also
when a gather or a relay failed. Windows behind a cuObject DC target
keep their key. A window whose gather or relay did not finish cleanly
stays out of use for its quarantine all the same, and for 10 s at
least: its old key keeps a late write out only if the provider drops
what carries it. A window whose re-key fails also stays out of use for
10 s. When a window is registered again instead, the key that leaves
service is not used for another window for 10 s, and an endpoint asks a
provider with a fixed table of regions, such as UET's, for a table of
16384 regions.

Erasure-coded pools
===================

Primary reads and shard-direct reads
------------------------------------

An erasure-coded pool serves a passthrough GET by one of two read
paths. The pool and the gateway's read policy select the path.

Primary reads
  This is the default. The primary reconstructs the logical data and
  writes one contiguous range for each stripe. A plain erasure-coded
  pool works without changes on the client.

Shard-direct reads
  Each shard OSD writes the chunks that it holds to their logical
  positions in the window. Two settings enable this, and both are off
  by default:

  * ``allow_ec_optimizations`` on the pool, which sets the pool's
    ``split_reads`` flag. Replicated pools always have that flag, but
    a client splits a replicated read only when
    ``osd_min_split_replica_read_size`` is set. It is 0 by default,
    which turns that off.
  * ``rados_replica_read_policy = balance`` on the gateway. The pool
    flag only permits split reads. A read is split only when the client
    asks for a balanced read.

  With only the pool flag, reads go to the primary. A shard that cannot
  read its chunks bounces the read to the primary. See
  `Shard read errors`_.

The interleave happens only when a gateway stripe spans several
erasure-coded stripes. When ``rgw_obj_stripe_size`` equals the pool's
``stripe_width``, each shard holds one contiguous range of the request.
The plan then has one write, as for a primary read.

Gathering shard reads out of band
---------------------------------

The primary of an erasure-coded read normally collects the shards it
needs from its peers in the sub-read replies, over the messenger. With
``osd_oob_gather`` on, the peers write their shard data into the
primary's memory over an executor. This needs a pool with
``allow_ec_optimizations``. It covers every read that the primary
decodes for a client or for a partial write, whether or not the
client's read carries a delivery descriptor. Recovery reads do not
gather. The primary then decodes as before, and its client delivery
writes the logical data into the client's window. Object data then
crosses the cluster network out of band on both hops. The first hop is
from the shards to the primary, and the second is from the primary to
the client.

The primary lends one registered window to each peer shard that it
reads from, and puts the window's token in the sub-read. The shard
reads as usual, writes all the data it read into the window, and
replies with only the extents and the crc32c of what it wrote. The
primary copies the shard buffers out of the window, checks the copy
against that checksum, and returns the window to its pool. A copy that
does not match - a write that completed short, or landed somewhere else,
or a late write into the window - counts as a failed shard read: the
primary logs the window and its key, counts it in
``gather_crc_mismatch``, quarantines the window, and reads the
remaining shards inline to rebuild the data. A push whose extents run
past the end of its window, and pushed extents with no window to find
them in, fail the same way, without the count. The primary records
these failures as transport faults, not media errors, so it never
repairs a shard for one. See `Shard read errors`_. The check costs a
crc32c of the shard data on each side, and does not cover data that was
wrong at rest. The primary reads its own shard without a window.

The gather is advisory too. A shard that cannot write replies inline.
A shard follows the same bounds as for client delivery, counted from
when the sub-read arrived. A window whose data the primary did not use
stays out of use for the pool's ``rdma_delivery_lease`` plus its
``rdma_delivery_drain``. This covers a shard that replied inline, and
a read that was cancelled or restarted. With ``osd_oob_rekey_windows``,
the libfabric executor also gives every released window a new key,
after which no write meant for the last gather can land where the
provider drops writes with a retired key. A window whose data the
primary used is lent again at once, unless its new key fails. One whose
data it did not use keeps its quarantine, and stays out of use for 10 s
at least once it has a new key. A cuObject window gets no new key. See
`Reusing a window`_.

The first transport in ``osd_oob_transports`` that started lends the
windows. The libfabric executor registers them on its endpoint. The
cuObject executor puts them behind a DC target on the OSD's adapter.
Peers must run the same transport to write into a window.

The token travels in a new trailing field of the sub-read message, and
the pushed extents and their crc32c in new trailing fields of the
reply. An OSD of an older release ignores the token and replies inline.

Shard read errors
-----------------

A shard-direct read reads one shard and has nothing to rebuild from.
When the shard's store fails the read with a media error (``-EIO``),
the shard bounces the read with ``-EAGAIN``. It logs ``<pgid>
shard-direct read of <object> failed: <error>, bounced to primary
osd.<N> to reconstruct`` to the cluster log as an error, and counts the
bounce in the OSD performance counter ``ec_direct_read_redirect_eio``.
The RADOS client then sends the read to the primary as a plain read,
and the primary decodes around the bad shard. A bounce answers the read
before any transfer starts, so it costs the gateway neither a fallback
nor a fence. See `Retries`_.

When a shard's store fails a client read that the primary of a pool
with ``allow_ec_optimizations`` decodes, the primary also repairs the
shard, unless ``osd_ec_repair_on_read`` is off. It marks the object
missing on that shard and recovers it at once. It logs ``<pgid> shard
<shard> failed to read <object> v <version> with a media error;
marking it missing to repair it from the other shards`` to the cluster
log as a warning, and counts the shard in the OSD performance counter
``ec_read_repair``. The PG goes through recovery with ``repair`` set,
and the pushes count as repairs on the OSDs that take them, toward
``OSD_TOO_MANY_REPAIRS``. The read completes with the
decoded data either way. The primary repairs only in an active and
clean PG with no scrub running and no write holding the object, and
only while k+1 shards that the read did not find damaged hold the
object. A pool with m=1 therefore never repairs on read. A shard left
alone is found by the next read of the object, or by a deep scrub.

A gather window fault is not a media error. The shard is healthy and
the fabric lost its push, so the primary records the fault as
``-EBADMSG``, reads around the shard, and does not repair it. Both
kinds of failure appear in the cluster-log warning ``Error(s) ignored
for <object> (shard errors ...) enough copies available``: ``-5`` for a
media error, ``-74`` for a gather fault. With
``osd_read_ec_check_for_errors`` on (off by default), the read fails
with ``-EIO`` instead, whatever the cause, and nothing is repaired.

Reliable Connection clients (hipobj-rc-v2)
==========================================

Control protocol
----------------

An RC transfer needs a queue pair on each side, paired before data
moves. The client and the gateway exchange the pairing parameters in
three signed HTTP requests:

``POST /.hipobj-rc/prepare``
  The client sends its RC token (queue pair number and GID), a packet
  sequence number (PSN), a cookie, the operation, the object and the
  byte range. The gateway authorizes the request as the S3 operation
  that it stands for. It creates a session, and answers with the
  session ID and its own queue pair number and PSN.

``POST /.hipobj-rc/ready``
  The client sends its queue pair number and the address and remote key
  of its memory region. The gateway pairs its queue pair with the
  client's, runs the transfer, and answers when the transfer is
  complete. The answer carries the byte count, the ETag, the version ID
  and a CRC-64/NVME of the delivered bytes.

``POST /.hipobj-rc/cancel``
  The client ends a session that it no longer needs.

Every ``x-amz-rdma-*`` request header must be signed. With SigV4 it
must be in the ``SignedHeaders`` list; SigV2 signs every ``x-amz-*``
header. The gateway refuses a request with an unsigned protocol header
with ``403``. A device on the network path can change such a header
without breaking the signature. The gateway also refuses anonymous
requests. A session belongs to the identity that prepared it: the user,
subuser or role session. READY and CANCEL from another identity fail,
and leave the session alone. A CANCEL of a session that has already
ended succeeds.

When ``rgw_rdma_rc_enabled`` is off, the control routes answer ``501``
with ``x-amz-rdma-protocol-status: unsupported``. That answer tells a
hipObject client to use plain HTTP.

GET relay
---------

For a GET, the session buffer sits between the OSDs and the client.
The object reaches the buffer in one of two ways:

OSD-direct relay
  The gateway exposes its session buffers to the OSDs. Each stripe read
  carries a delivery descriptor for the session buffer, and the OSDs
  write their stripes into it, as in passthrough mode. The gateway does
  not copy the object. This needs ``rgw_rdma_osd_passthrough``, and a
  transport in ``rgw_rdma_transports`` that can expose the buffers.
  The first one that can does so. With ``ofi``, the buffers are exposed
  over libfabric, on any adapter that the provider supports. With
  ``cuobj``, they are exposed as a cuObject DC target, which needs an
  mlx5 adapter on the gateway.

Staged relay
  This is gateway-staged mode for RC clients. The gateway reads the
  object from RADOS as usual and copies it into the session buffer. The
  gateway uses this path when OSD-direct relay is not available, and
  for objects whose data it must transform, such as compressed objects.
  It is also the fallback when an OSD returns a stripe inline.

In both cases the gateway sends the bytes to the client while the read
is still in progress. Each time the start of the buffer is complete up
to a new point, the gateway writes the new bytes to the client. The
last write carries the session cookie as its immediate value. RC
delivers writes in order, so the completion of the last write tells the
client that all earlier writes are in its memory.

A relay can fail after the OSDs received delivery descriptors. An OSD
can then still take on a write into the session buffer until the pool's
``rdma_delivery_lease`` expires. That write can land until the pool's
``rdma_delivery_drain`` runs out after the lease. The gateway keeps the
buffer out of use for the lease plus the drain, unless every stripe
came back declined or landed and none was resent. When a stripe comes
back inline, the gateway waits for the same fence before staged relay
copies the object into the buffer. With ``rgw_rdma_rekey_windows``, the
gateway also gives the buffer's libfabric window a new key when the
session ends, after which no OSD write meant for that session can land
where the provider drops writes with a retired key. After a clean
session the buffer is free at once; after a failed one it keeps the
quarantine, and for 10 s at least. See `Reusing a window`_.

PUT
---

For a PUT, the gateway takes a session buffer at PREPARE, arms a
receive on it, and gives the client its address and remote key. The
client writes the whole object with one RDMA write-with-immediate,
with the session cookie as its immediate value. The gateway then stores
the buffer through the normal PUT path. Bucket default encryption,
compression, notifications and object lock apply as they do to any PUT.

Transports
==========

cuObject
--------

The cuObject descriptor is opaque to Ceph. The gateway and the OSDs
read only its leading address and size fields. The rest names the
client's remote key and DC target. Every writer needs the DC key that
the client library uses, which is ``0xffeeddcc`` by default.

The OSD executor needs ``cuobj`` in ``osd_oob_transports``, a build
with ``WITH_OSD_CUOBJ``, a ConnectX-5 or newer adapter, ``rdma-core``,
and NVIDIA's proprietary ``cuobjserver`` library. Gateway-staged mode
for cuObject clients needs ``cuobj`` in ``rgw_rdma_transports``,
``rgw_cuobj_rdma_ip``, which has no default, a build with
``WITH_RADOSGW_CUOBJ``, and the same adapter and library on the gateway
host. No GPU is needed on the OSD, gateway or client hosts. Only GPU
memory targets on the client need CUDA.

Three host settings are easy to miss. Each one fails with an error that
does not name the real cause.

``rdma_ucm`` must be loaded
  The cuObject server connects through ``rdma_cm``, so
  ``/dev/infiniband/rdma_cm`` must exist::

    modprobe rdma_ucm

  Without it, the OSD logs ``cuObjServer RDMA session failed to start``
  and disables cuObject delivery. A passing ``ib_send_bw`` run does not
  prove that the module is loaded. perftest uses raw verbs by default
  (``rdma_cm QPs : OFF``), so the fabric can reach line rate while
  cuObject cannot start a session.

Locked memory must be raised
  Every OSD registers ``osd_oob_buffer_count`` times
  ``osd_oob_buffer_size`` of memory for each executor, which is 256 MiB
  at the defaults. With ``osd_oob_gather``, each executor also
  registers ``osd_oob_window_count`` times ``osd_oob_window_size`` for
  gather windows, 128 MiB more. A cuObject executor whose buffers are
  all in use registers a one-time buffer of up to four times
  ``osd_oob_buffer_size``. The gateway's cuObject server registers
  ``rgw_cuobj_buffer_count`` times ``rgw_cuobj_buffer_size``, 1 GiB at
  the defaults. That is far above the usual 8 MiB ``memlock`` limit.
  Give the OSDs, and the gateway in staged mode,
  ``LimitMEMLOCK=infinity``. For a vstart cluster, run
  ``ulimit -l unlimited``.

The RDMA address must belong to the RDMA device
  ``osd_cuobj_rdma_ip``, and ``rgw_cuobj_rdma_ip`` on the gateway, must
  name an address that the RDMA device carries. When the ConnectX
  ports are bonded and tenant traffic is VLAN-tagged, that is the
  address on the VLAN above the bond. It is
  usually not the public address. ``ibv_devinfo`` and the GID table
  under ``/sys/class/infiniband/<device>/ports/1/gids`` show the
  addresses of the device. A RoCE v2 entry whose GID ends in the
  IPv4-mapped form of the address shows the pairing.

A client that reads into host memory needs no GPU and no NVIDIA kernel
driver. ``libcufile`` reaches that configuration only with DMABuf
enabled. Otherwise it logs ``nvidia_peermem.ko is not loaded. Disabling
UserSpace RDMA access.``, registers no RDMA devices, and
``cuMemObjGetDescriptor`` fails::

  export CUFILE_DMABUF_ENABLE=true

The client's own RoCE address must also be in ``rdma_dev_addr_list`` in
``cufile.json``, which is empty by default. ``CUFILE_ENV_PATH_JSON``
selects another copy of the file::

  "rdma_dev_addr_list": [ "10.0.9.7" ],

Leave ``rdma_transport_type`` at ``DC_V1``, and keep ``rdma_dc_key``
equal to ``osd_cuobj_dc_key`` on the OSDs. The defaults on both sides
agree.

libfabric
---------

The owner of a window sends this token::

  <base hex>:<size hex>:ofi1:<provider>:<endpoint name hex>:<memory key hex>

The fields are:

* ``base``: the remote address of the first byte of the window, as the
  owner's provider reads it. That is a virtual address when the
  provider uses ``FI_MR_VIRT_ADDR``, and an offset into the region
  otherwise. A writer writes byte ``i`` of the window at ``base + i``.
* ``size``: the length of the window.
* ``provider``: the provider's name as ``fi_info`` reports it, for
  example ``tcp`` or ``verbs;ofi_rxm``. Both ends must use the same
  provider.
* ``endpoint name``: the bytes that ``fi_getname()`` returns for the
  owner's endpoint, at most 192 bytes. A writer passes them to
  ``fi_av_insert()``.
* ``memory key``: the key of the region behind the window. Providers
  with keys longer than 8 bytes are not supported.

The token starts with the same ``addr:size`` fields as a cuObject
descriptor, so the gateway forwards it in passthrough mode without
reading the rest. The owner never adds a writer to its address vector,
so any OSD that holds the token can write into the window.

The executor asks the provider for delivery-complete writes. The
completion of a write then means that the bytes are in the owner's
memory. A provider that cannot promise this still works, but the OSD
logs a warning at startup.

The first write to a peer adds the peer to the provider's address
vector. Some providers take long to do that. A thread of the executor
adds the peer, and the writes to that peer wait for it, each only until
its deadline. A write that gives up has sent nothing, and its read is
delivered inline. The insert goes on, so a later read finds the peer
ready. The executor asks the provider for a thread-safe domain. With
one, writes to other peers go on during the insert. A provider that
offers only ``FI_THREAD_DOMAIN`` allows no other call meanwhile. Then
the insert first waits until the writes in flight are done, so that
none of them misses its cut-off, and writes that start meanwhile wait
for it, again only until their deadlines. The OSD logs at startup which
case applies: ``concurrent peer inserts`` or ``peer inserts pause
writes``. In the second case the executor's other work waits for the
insert too: a gather window's token, the progress thread that places
data in gather windows, and the gather's read of a window. Gathers then
wait for the insert, and a shard's push whose budget runs out meanwhile
is delivered inline.

Clients choose their endpoint names, so the executor bounds what they
can make it do. At most 64 first contacts are queued or running at
once; a read for yet another new client is delivered inline at once,
and ``inserts_refused`` counts it. The address vector holds at most 1024
peers. A new one evicts the least recently used peers that no write is
using, never one that is being added. When every peer is in use, a read
for a new client is delivered inline, and ``inserts_refused`` counts it
too. A failed insert is remembered for a second, and reads for that
client are delivered inline meanwhile.

Providers that progress manually place incoming data only while the
application polls them. Each window owner in Ceph polls its endpoint
from a thread. A libfabric client must poll its endpoint while it waits
for the response, or use a provider with automatic progress.

These providers are known to work:

``tcp``
  Needs no special hardware. Set ``osd_ofi_node`` to the address the
  OSD sends from. It cannot cut off a write that misses its deadline,
  so use it for tests only.

``shm``
  Works between processes on one host. It is useful for tests.

``verbs;ofi_rxm``
  RDMA reliable connections on any verbs device. Set ``osd_ofi_domain``
  to the device, for example ``mlx5_0`` or ``rxe0``. On soft-RoCE the
  provider fails to open with "Unable to create verbs CQ". The rxm
  provider sizes its completion queues from the universe size, and
  soft-RoCE allows 32767 entries per queue. Set
  ``FI_UNIVERSE_SIZE=16`` in the environment of every process.

``uet``
  Ultra Ethernet. See `UET`_.

UET
---

Ultra Ethernet Transport (UET) reaches Ceph through the libfabric
executor. UET's RUDI mode is reliable, unordered and connectionless,
and it is meant for idempotent operations. A window owner keeps no
state for the OSDs that write into it. cuObject's DC transport has the
same property, but UET runs on any Ethernet adapter.

The UEC reference provider (https://github.com/ultraethernet/uet-ref-prov)
is a software UET stack over raw Ethernet sockets. It does not register
itself with libfabric. A libfabric wrapper that registers it as the
provider ``uet`` is needed. Point ``FI_PROVIDER_PATH`` at the directory
that holds the wrapper, in the environment of every daemon and client.

Each OSD needs these settings:

* ``ofi`` in ``osd_oob_transports``, and ``osd_ofi_provider`` set to
  ``uet``.
* ``osd_ofi_domain`` set to the interface that the OSD sends and
  receives on. The interface's IPv4 address is the OSD's fabric
  endpoint. The value can use ``$id``, for example ``uet-o$id``.
* The ``CAP_NET_RAW`` capability, for the raw socket. An OSD normally
  drops every capability that its block-device plugins do not need.
  When ``osd_oob_transports`` names ``ofi`` and ``osd_ofi_provider`` is
  ``uet``, the OSD also keeps ``CAP_NET_RAW``.

The gateway's relay windows need ``ofi`` in ``rgw_rdma_transports``,
``rgw_ofi_provider`` set to ``uet``, an interface in
``rgw_ofi_domain``, and ``CAP_NET_RAW``.

Give every UET interface the same MTU, and use jumbo frames where the
network allows them. The provider sizes each packet's payload from the
interface MTU: about 8 KiB at an MTU of 9000, against 1 KiB at 1500. UET
travels over UDP, to port 4793 by default.

The wrapper must discard an endpoint's writes when the endpoint closes,
as ``fi_endpoint(3)`` requires. The OSD relies on that to cut off a late
write, unless the wrapper promises that a cancelled write is discarded;
it then cancels the late write alone. A wrapper that drains its writes
on close instead lets a cut-off write land after the deadline. The OSD
measures each cut-off. One whose close or cancel itself takes longer
than ``osd_oob_cutoff_late_tolerance``, as a draining close does, counts
as a failed cut-off, and the OSD stops delivering out of band. The OSD
also warns at startup when the wrapper says that closing does not
discard. See `Fencing a window before reuse`_.

A wrapper that offers ``FI_THREAD_SAFE`` lets the OSD add a new client
while its writes to other clients go on. One that offers only
``FI_THREAD_DOMAIN`` pauses the OSD's writes for each new client. See
`libfabric`_.

The reference provider has these limits:

* It is software. A window owner places incoming data only while it
  polls its endpoint.
* It keeps process-wide state, so each process has one interface and
  one endpoint. Processes on one host need separate interfaces and
  addresses.
* The first write to a new peer finds the next hop. Current versions
  ask the kernel over rtnetlink, and wait for ARP at most
  ``UET_NH_WAIT_MS`` (1 s by default). Older versions run ``ip route``,
  ``arp`` and ``ping``, which takes 10 seconds when the peer does not
  answer the ping, as rocm-ernic's emulated UET NIC did not. Reads whose
  deadline passes during the wait are delivered inline.
* A memory key is the index of a region in a table that the provider
  fills round robin, so a key comes back after as many registrations as
  the table holds. The OSD and the gateway ask for 16384 regions. See
  `Reusing a window`_.
* RUDI keeps no state at the target, so duplicates of completed writes
  can arrive late. See `Reusing a window`_.
* It sends writes unencrypted, because the security sublayer is not
  configured.
* It supports IPv4 only.

To run several endpoints on one host, put each UET interface in its own
VRF, the gateway's interface included. The provider finds a peer's next
hop by a route lookup on its own interface. Without VRFs, a peer's
address is a local address of the host, so the lookup finds no next hop
on the wire.

Integrity
=========

The gateway does not touch passthrough data, so it cannot compute a
checksum itself. When ``rgw_rdma_crc64nvme`` is on, the gateway asks
each OSD for a CRC-64/NVME of the data that the OSD delivered. The OSD
computes the values from the bytes that it writes. For an erasure-coded
primary read, those are the bytes after reconstruction. The OSD reports
one value for each range of its placement plan. When the plan has one
range, that value also stands for the whole reply. The executor does
not matter.

The gateway folds the values in logical order with the same combining
math that S3 uses for multipart full-object checksums. For a
shard-direct read, the RADOS client first folds the ranges of all
shards, when they cover the operation's bytes without gaps. Some objects
have a stored full-object ``crc64nvme`` checksum, the AWS
``x-amz-checksum-crc64nvme`` type. For a whole-object GET of such an
object, the gateway compares the folded value with the stored checksum.
It does so before it sends any response bytes. A mismatch fails the
GET with ``500``, and the gateway logs both values. When a stripe comes
back without a value, as from an OSD of an older release, the gateway
does not compare.

This comparison covers the storage node, the reconstruction, and the
shard transfers inside the cluster. It does not cover the final write
into client memory. That write relies on the transport's own integrity
protection. Ranges with gaps between them do not fold into a
whole-object checksum, so a sparse read with several extents is not
compared.

RC clients get a checksum of the delivered bytes in the READY answer
(``X-Amz-Rdma-Checksum``), when ``rgw_rdma_crc64nvme`` is on. An RC
client can compare that value with the bytes in its own buffer, which
also covers the final write.

Configuration reference
=======================

Build options
-------------

``WITH_RADOSGW_CUOBJ``
  Gateway-staged mode with NVIDIA's ``cuobjserver`` library.

``WITH_OSD_CUOBJ``
  The OSD's cuObject executor, with the ``cuobjserver`` library.

``WITH_OOB_OFI``
  The libfabric executor, gather windows over libfabric, and the
  gateway's relay windows over libfabric. Needs the libfabric headers
  and library, API version 1.18 or later.

``WITH_RADOSGW_RDMA_RC``
  The ``hipobj-rc-v2`` server. It is on by default when ``WITH_RDMA``
  is on. The DC target for OSD-direct relay also needs the ``mlx5``
  direct verbs library at build time.

Pool options
------------

The OSDs enforce both options, and the gateway reads the same values
from the OSDMap, so no daemon setting must agree with them. Set them
with ``ceph osd pool set <pool> <option> <seconds>``. A value of 0
restores the default.

``rdma_delivery_lease``
  How long, in seconds, after it receives a stripe operation an OSD can
  still take on a transfer against its delivery descriptor. A transfer
  that would start later is delivered inline. The default is 5.

``rdma_delivery_drain``
  How long, in seconds, after the lease every write that an OSD started
  lands or is cut off. Make it longer than a transport needs to cut off
  its writes. That is about two seconds for cuObject. For libfabric, it
  is the time an endpoint takes to close and reopen. The default is 3.

Gateway options
---------------

Transports and out-of-band behavior:

* ``rgw_rdma_transports``: the transports that the gateway starts, in
  order of preference. ``cuobj`` starts the gateway's cuObject server
  for gateway-staged mode, and can expose the RC relay windows as a DC
  target. ``ofi`` opens a libfabric endpoint that can expose the relay
  windows. The default is empty.
* ``rgw_rdma_osd_passthrough``: have the OSDs write GET data directly,
  into client windows and into RC relay windows. The default is false.
* ``rgw_rdma_crc64nvme``: compute checksums of out-of-band transfers.
  The gateway compares whole-object passthrough GETs with the stored
  checksum, and reports a checksum to RC clients. The default is true.

libfabric endpoint (``ofi``):

* ``rgw_ofi_provider``: the provider. It must match the OSDs'
  ``osd_ofi_provider``. The default is ``tcp``.
* ``rgw_ofi_domain`` and ``rgw_ofi_node``: the domain and the local
  address to bind. The address must be reachable from the OSDs. When
  they are empty, the provider chooses.
* ``rgw_rdma_rekey_windows``: give a relay window a new memory key when
  its session ends, so that no OSD write meant for the session can land
  in the next one. The default is true. See `Reusing a window`_.

cuObject server (``cuobj``):

* ``rgw_cuobj_rdma_ip`` and ``rgw_cuobj_rdma_port``: its address and
  port. The default port is 20886. The address has no default, and the
  cuObject server does not start without it.
* ``rgw_cuobj_buffer_size`` and ``rgw_cuobj_buffer_count``: the staging
  buffers, 128 buffers of 8 MiB by default.
* ``rgw_cuobj_num_dcis``: the DC initiators, 128 by default.
* ``rgw_cuobj_dc_key``: the DC key of the server and of the relay DC
  target. It must match the clients' key and ``osd_cuobj_dc_key``. The
  default is ``0xffeeddcc``.

RC clients:

* ``rgw_rdma_rc_enabled``: serve RC sessions. The default is false.
* ``rgw_rdma_rc_device``, ``rgw_rdma_rc_gid_hint``,
  ``rgw_rdma_rc_port`` and ``rgw_rdma_rc_gid_index``: select the verbs
  device, port and GID.
* ``rgw_rdma_rc_buffer_size`` and ``rgw_rdma_rc_buffer_count``: the
  session buffers, 16 buffers of 64 MiB by default. Each session holds
  one buffer from PREPARE until the session ends. A transfer larger
  than one buffer is refused with ``413``.
* ``rgw_rdma_rc_max_sessions`` and ``rgw_rdma_rc_max_sessions_per_user``:
  the session limits, 1024 and 64 by default. A PREPARE over a limit,
  or one that finds no free buffer, gets ``503 SlowDown``.
* ``rgw_rdma_rc_send_depth``: the writes to the client that can be in
  flight for one session, 64 by default.
* ``rgw_rdma_rc_prepare_timeout_ms`` and ``rgw_rdma_rc_exec_timeout_ms``:
  how long a session waits for READY, and how long a transfer can take,
  100 s and 30 s by default.

OSD options
-----------

Executors and out-of-band behavior:

* ``osd_oob_transports``: the executors that the OSD starts, in order
  of preference. ``ofi`` starts the libfabric executor and ``cuobj``
  starts the cuObject executor. The default is empty.
* ``osd_oob_buffer_size`` and ``osd_oob_buffer_count``: the staging
  buffers of each executor, 16 buffers of 16 MiB by default. A buffer
  must hold the largest read that is delivered out of band, such as a
  gateway stripe (``rgw_get_obj_max_req_size``, 4 MiB by default). The
  libfabric executor delivers a read inline when it does not fit or
  finds no free buffer. The cuObject executor registers a one-time
  buffer for it instead, of up to four times ``osd_oob_buffer_size``,
  which is slower. A larger read is delivered inline.
* ``osd_oob_gather``: lend windows when this OSD is the primary of an
  erasure-coded read. The default is false.
* ``osd_oob_window_size`` and ``osd_oob_window_count``: the window
  pool, 16 windows of 8 MiB by default. A shard read larger than a
  window, or one that finds no free window, is returned inline.
* ``osd_oob_rekey_windows``: give a libfabric gather window a new memory
  key when it is released, so that no write meant for an earlier gather
  can land in a later one. The default is true. See `Reusing a window`_.
* ``osd_oob_cutoff_failure``: what the OSD does when the libfabric
  executor fails to cut off writes, or when the close or cancel itself
  takes longer than the tolerance. With
  ``osd_oob_cutoff_late_fail_closed``, a cut-off that ends later than the
  tolerance counts too. ``disable``, the default, stops out-of-band
  delivery and raises ``OOB_DELIVERY_UNSAFE``. ``abort`` makes the OSD
  exit. See `Fencing a window before reuse`_.
* ``osd_oob_cutoff_late_tolerance``: how late a cut-off may end, past the
  budget it protects, and still be only counted and warned of
  (``OOB_CUTOFF_LATE``). The default is 1000 ms.
* ``osd_oob_cutoff_late_fail_closed``: stop delivering when a quick
  cut-off ends later than the tolerance, instead of logging it and
  raising ``OOB_CUTOFF_PAST_TOLERANCE``. The default is false.
* ``osd_oob_cutoff_late_alert_period``: how long ``OOB_CUTOFF_LATE`` and
  ``OOB_CUTOFF_PAST_TOLERANCE`` stay raised after the latest late
  cut-off. The default is an hour.

Erasure-coded reads:

* ``osd_ec_repair_on_read``: have the primary repair a shard whose
  store failed a client read with a media error. The default is true.
  See `Shard read errors`_.

libfabric executor (``ofi``):

* ``osd_ofi_provider``: the provider. The default is ``tcp``.
* ``osd_ofi_domain`` and ``osd_ofi_node``: the domain, such as an RDMA
  device or a network interface, and the local address to bind. When
  they are empty, the provider chooses.

cuObject executor (``cuobj``):

* ``osd_cuobj_rdma_ip``: the RDMA interface address. The default is the
  OSD's public address. You must set it when the RDMA adapter is not
  the public-network interface.
* ``osd_cuobj_rdma_port``: the local ``rdma_cm`` port. The default, 0,
  lets the library choose. Clients never connect to this port.
* ``osd_cuobj_num_dcis``: the DC initiators, 128 by default. Use at
  least the number of OSD op worker threads.
* ``osd_cuobj_dc_key``: the DC key. It must match the key of the client
  library across the cluster. The default is ``0xffeeddcc``, the
  library's default.

Operations
==========

Status
------

Each executor reports its counters through the OSD's admin socket. An
OSD registers each command only when that executor started::

  ceph daemon osd.N cuobj status
  ceph daemon osd.N ofi status

Both report plans started, completed and failed, and bytes written.
Both also show ``gather_crc_mismatch``: gathered shard data that did not
match its shard's checksum, whichever executor lent the window. See
`Gathering shard reads out of band`_. ``cuobj status`` also shows the
writes in flight, and ``buffers_leaked``, the staging buffers that plans
which timed out with writes outstanding never gave back. ``ofi status``
shows the windows lent and exhausted, ``staging_busy``, the reads
delivered inline because no staging buffer was free, the provider and
how the endpoint cuts off writes (``transport``), and the endpoint's
last error (``last_error``). It also shows:

* ``cutoffs``, the endpoint resets, and ``plans_cut_off``, the late
  transfers cancelled alone; ``cancels_failed``, the cancels that fell
  back to a reset; ``cancel_discards``, whether the executor cancels
  late transfers alone, and ``close_discards``, whether the provider
  promises that closing an endpoint discards its writes (``-1`` when it
  does not say). See `Fencing a window before reuse`_.
* ``cutoffs_failed`` and ``cutoffs_late``, ``max_cutoff_lateness_ms``
  and ``late_tolerance_ms``, ``unsafe`` and ``broken``.
* ``cutoff_slack_ms``, the scheduling slack, and ``cutoffs_on_behalf``.
* ``cutoffs_past_tolerance``, the quick cut-offs that ended later than
  the tolerance, and ``max_poll_gap_ms``, the longest the endpoint went
  without being polled, of a second or more.
* ``cutoff_cost_ms``, the current estimate of a cut-off's cost, and
  ``budget_refused`` and ``late_starts``, the reads delivered inline
  because too little of their budget was left.
* ``peer_timeouts``: reads that gave up, with nothing sent, while a new
  peer was added to the address vector or while they waited for the
  endpoint. ``peers``, ``pending_inserts`` and ``inserts_refused``. See
  `libfabric`_.
* ``windows_rekeyed``, ``windows_rekeyed_quarantined`` (re-keyed after a
  gather that did not finish cleanly, and quarantined all the same),
  ``windows_rekey_failed`` and ``key_collisions``;
  ``rekeys_in_place`` and ``rekeys_reregistered``, and
  ``rekey_in_place``, whether the provider re-keys in place. See
  `Reusing a window`_.

An OSD whose libfabric executor stopped raises ``OOB_DELIVERY_UNSAFE`` or
``OOB_DELIVERY_DOWN`` in ``ceph health detail``. One that cut off writes
late raises ``OOB_CUTOFF_LATE`` within the tolerance, and the health
error ``OOB_CUTOFF_PAST_TOLERANCE`` beyond it. ``OOB_CUTOFF_LATE`` is not
shown while ``OOB_CUTOFF_PAST_TOLERANCE`` is. Only the libfabric
executor raises these checks.

Two OSD performance counters count erasure-coded reads that hit a media
error. ``ec_direct_read_redirect_eio`` counts shard-direct reads that a
shard could not read and bounced to the primary.
``ec_read_repair`` counts the shards that a primary marked missing to
repair them after a read decoded around their media error. Each event
also goes to the cluster log. See `Shard read errors`_.

Accounting
----------

Bytes that move out of band appear in the beast access log, the ops log
and the usage log. They count as bytes sent for a GET and bytes
received for a PUT, although they do not cross the HTTP socket. For RC
clients, only the bytes that the OSDs write into a relay buffer are
counted; staged relay and RC PUT bytes are not.

Testing
-------

* ``unittest_ofi_rma`` tests the libfabric token format, writes over
  the ``tcp`` and ``shm`` providers, slow and refused first contacts,
  eviction, cut-offs that fail or end late, within and beyond the
  tolerance, writers left idle near their deadlines, late writes
  cancelled alone
  (over ``tcp``, with a test hook that stands in for a provider whose
  cancel discards), the cut-off cost estimate,
  writes that start too late, concurrent writes and their CPU use,
  that a write with a re-keyed window's old token does not land, and
  the gather window cycle under load, with cut-offs alongside.
* ``unittest_ec_gather`` tests the check of gathered shard data against
  its shard's checksum.
* ``unittest_oob_placement`` tests placement plans.
* ``unittest_rdma_token`` tests token parsing and transport lists.
* ``unittest_crc64nvme`` tests CRC-64/NVME against the NVM Command Set
  test cases, and checks that its implementations agree.
* ``unittest_rdma_delivery`` tests the wire formats of delivery
  descriptors and results, how the client library marks a result
  resent, and when an attempt counts as settled, so that a resend
  carries ``FLAG_PRIOR_SETTLED``. It follows a push that an OSD cut off
  through the client library into the gateway's decision to fall back
  to HTTP.
* ``qa/standalone/erasure-code/test-erasure-eio.sh`` covers shard-direct
  reads whose shard read fails with ``-EIO``, and their bounce to the
  primary.
* ``unittest_rgw_rdma_fence`` tests when the gateway fences, and how it
  reads each stripe's reply.
* ``unittest_rgw_rdma_rc_wire`` tests the ``hipobj-rc-v2`` wire
  encoding.
* ``ceph_test_rgw_ofi_get`` is an S3 client for OSD-direct delivery
  over libfabric. It registers a window, sends a signed GET with the
  token, and compares the window with a local copy of the object::

    ceph_test_rgw_ofi_get <provider> <domain|-> <node|-> <endpoint> \
      <bucket> <key> <access key> <secret> <expected file>

Limitations
===========

* An OSD writes on the op worker thread and waits for the writes to
  complete before the next operation on that thread.
* An OSD copies each read into a staging buffer. It does not register
  the read's own buffers.
* Every call into one libfabric endpoint is serialized, except the
  adding and removing of peers on a provider that offers
  ``FI_THREAD_SAFE``.
* Every libfabric endpoint of one provider must use the same address
  family. Mixed IPv4 and IPv6 endpoints are not tested.
* A split read, erasure-coded shard-direct or replicated balanced,
  delivers a sparse read inline.
* Only cuObject clients can PUT with ``x-amz-rdma-token``, in
  gateway-staged mode. The object must fit one of the gateway's staging
  buffers (``rgw_cuobj_buffer_size``), and a PUT that finds no free
  buffer large enough fails with ``503``. Libfabric clients have no
  gateway-staged mode, so a libfabric GET that cannot use passthrough
  gets the HTTP body.
* The gateway does not serve encrypted objects, or objects with a DLO
  or SLO manifest, to RC clients. PREPARE answers ``501`` with the
  unsupported marker, and the client reads the object over HTTP.
* An RC GET can name a ``versionId``. Multipart part uploads and other
  subresources are not available over RC.
* An RC PUT must arrive as one write-with-immediate that carries the
  whole object.
* An RC transfer runs on the request thread.
* The cuObject executor's gather windows are not tested on hardware.

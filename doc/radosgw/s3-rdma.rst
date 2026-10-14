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
  token names the wire protocol, the client's endpoint and the memory
  key. Any server process whose provider speaks the same wire protocol
  can write into the window without a connection to the client. The
  provider selects the
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
  stripes arrive. This is OSD-direct relay. ``rgw_rdma_osd_direct``
  turns on both, and it is on by default.

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

OSD-direct PUT
--------------

With ``rgw_rdma_osd_direct``, a PUT can leave the object in client memory.
The client registers the object's bytes as a window that peers can
read. It then sends a PUT with ``x-amz-rdma-token`` and an empty body.
The window's size is the object's size. Both token types work: a
libfabric token whose window allows remote reads, and a cuObject
descriptor.

The gateway writes the object as stripes of ``rgw_obj_stripe_size``
that carry no data. Each stripe write carries a delivery descriptor
with the pull flag. The primary OSD of the stripe RDMA-reads the stripe
out of the window into a staging buffer, and then runs the write as
usual. In an erasure-coded pool, the primary encodes the stripe and
sends each shard to its OSD. In a replicated pool, it sends the stripe
to the replicas. The object's bytes never pass through the gateway.

Each OSD reports the CRC-64/NVME of the bytes that it pulled. If the
client chose CRC-32C, each OSD also reports the CRC-32C of those bytes.
A client chooses CRC-32C with ``x-amz-checksum-crc32c`` or with
``x-amz-checksum-algorithm: CRC32C``. The gateway folds each set of
values in object order, with the math of `Integrity`_. It stores the
checksum of the client's algorithm as the object's full-object checksum.
If the client chose no algorithm, the gateway stores ``crc64nvme``. If
the client sends the checksum, the two values must match. Otherwise
the PUT fails with ``400 BadDigest``, and nothing is stored. A later
passthrough GET of the whole object compares the stored checksum with
what the OSDs deliver.

The object's head holds no data. The gateway writes the head last, in
one operation with the checksum and the ETag, so a reader sees the old
object or the new one, never part of the new one.

The gateway has no MD5 of the object. The ETag has the form of a
multipart upload's ETag: the MD5 of the stripes' CRC-64/NVME values in
big-endian order, then ``-`` and the number of stripes. The suffix tells
S3 clients that the ETag is not the MD5 of the object. The same bytes in
the same stripes always get the same ETag.

Multisite sync does not check these objects with
``rgw_sync_obj_etag_verify``. The fetching zone skips the check for an
object stored in one piece whose ETag has the multipart form.

A successful PUT answers ``200`` with ``x-amz-rdma-reply: 200``,
``x-amz-rdma-bytes-transferred`` and the stored checksum in
``x-amz-checksum-crc64nvme`` or ``x-amz-checksum-crc32c``. The answer
also carries the folded values of the OSDs in
``x-amz-rdma-checksum-crc64nvme``. If the client chose CRC32C, the answer
also carries ``x-amz-rdma-checksum-crc32c``.

The gateway never stores a body-less PUT with a token as an empty
object. If the gateway cannot serve such a PUT out of band, it answers
``501`` with ``x-amz-rdma-reply: 501``, and the client sends the body
instead. The gateway declines these PUTs:

* PUTs that arrive while ``rgw_rdma_osd_direct`` is off, unless a
  cuObject gateway-staged PUT serves the token.
* Appends, parts of multipart uploads, and PUTs with ``Content-MD5``.
* Objects that the placement compresses or that the gateway encrypts,
  torrents, and PUTs that a Lua data filter reads.
* PUTs with a client checksum other than ``CRC64NVME`` or ``CRC32C``.
* PUTs behind a SAL filter, such as ``rgw_filter`` ``base`` or ``d4n``.
  The filter's writer does not pass the pulled write on.
* PUTs with a stripe that an OSD could not pull. The gateway removes
  the stripes that it wrote, and the old object stays.

Every OSD executor can pull. The libfabric executor asks its provider
for RMA reads, and pulls into a buffer of the OSD's arena (see
`Registered memory`_). A provider without RMA reads still starts, for
delivery only. The cuObject executor reads with
``handlePutObject``, as the gateway's staged PUT does.

The OSD checks every pull of a write request before it starts any of
them. The pulls of one request are all or nothing. After one fails, no
later pull starts, and no write of the request runs. With
``osd_oob_transfer_threads`` at 0, a pull holds the object's lock, the
PG's lock and the op thread until it completes. That takes at most the
pool's lease plus drain. With transfer threads, a transfer thread runs
the pull, and the write holds no lock (see `Pulled writes on transfer
threads`_).

An OSD that cannot pull fails the write with ``EOPNOTSUPP``. That
happens in these cases:

* The OSD was built without a transport.
* None of its transports can pull with the token.
* Its client message throttle (``osd_client_message_size_cap``) has no
  room for the payload now.
* Its transfer threads refuse the pull, over one of their caps or
  because the OSD stops. The OSD does not pull on the op thread instead.

The pulled payloads of one request count against that throttle
together, as the data of one inline message does. The OSD reserves them
once, after it checks every pull and before the first one starts. As
the messenger does, it admits a request larger than the throttle's
limit while the throttle is at or under its limit. The request gives
the bytes back when it completes. The pulled and inline bytes of one
request together must not exceed ``osd_max_write_size``. The OSD fails
a larger request with ``EMSGSIZE`` before it pulls anything.

An OSD of an earlier release fails the write with ``EINVAL``, because
the write has no data. Below an Umbrella ``require_osd_release``, the
RADOS client does not send the pull descriptor. Every OSD then fails
the write with ``EINVAL``, and the result is marked declined. In all
these cases nothing is written.

When a pulled write fails, its reply marks the descriptor of an op
declined only when nothing of that op's pull reached the transport. The
reply leaves the result flags of an op at 0 when its pull started. That
holds whether the write fails in its pull or after it.

The window must stay registered and unchanged until the reply arrives.
A token for a readable window lets any holder read that memory. When
the PUT is complete, re-key the window.

An OSD can still read the window after the reply, when the RADOS client
resent a stripe write to another OSD. The pull of the earlier attempt
runs until it ends: at most the pool's lease plus drain after that
attempt reached its OSD. That OSD writes nothing from what it reads. A
re-key before then only makes that read fail. See `Pulled writes on
transfer threads`_.

Transports
----------

A transport moves object data between two processes out of band. Ceph
uses transports in two roles:

Client transports
  The transports that move object data to and from clients.
  ``oob_client_transports`` lists them, and so decides which tokens the
  cluster accepts.

Cluster transport
  The transport that moves object data between Ceph daemons. It carries
  the shard reads that an erasure-coded primary gathers, and the stripes
  that the OSDs write into the gateway's relay buffers.
  ``oob_cluster_transport`` names it.

Three transports exist:

libfabric
  Any libfabric provider, for example ``verbs_ofi_rxm``, ``uet`` or
  ``tcp``. It writes into the windows that libfabric tokens name. It can
  be a client transport and the cluster transport.

cuObject (``cuobj``)
  Writes into the windows that DC descriptors name, with NVIDIA's
  ``cuobjserver`` library. It needs a ConnectX adapter on each host that
  runs it. It can be a client transport and the cluster transport.

Reliable Connection (``rc``)
  The gateway's ``hipobj-rc-v2`` server. It is a client transport, and
  only the gateway runs it.

An OSD runs the libfabric and cuObject transports through executors. An
executor writes the bytes of a read into the window that a token names,
and pulls the payload of a write out of such a window.

Every out-of-band read is advisory. When a daemon cannot deliver read
data out of band, it sends the same bytes in band, and the request
still completes with correct data. A gateway-staged PUT is not: when
the gateway cannot take the object from the client's memory, as when
no staging buffer holds it, the PUT fails.

Configuring a cluster
=====================

Quick start
-----------

Put the options below in the ``[global]`` section of the cluster's
configuration, so that every OSD and every gateway reads the same
values. A daemon reads them only when it starts.

#. Set ``oob_network`` to the subnets of the interfaces that carry
   out-of-band traffic. See `The out-of-band network`_.
#. Set ``oob_client_transports`` to the transports of your clients: the
   libfabric provider that they use, ``cuobj`` for cuObject clients,
   and ``rc`` for RC clients such as hipObject.
#. Set ``oob_cluster_transport`` to the transport between the daemons:
   one libfabric provider, or ``cuobj``. If you leave it empty, the
   gathers of erasure-coded reads stay on the messenger, and RC GETs
   take staged relay.
#. Give each OSD and each gateway unlimited locked memory, for example
   with ``LimitMEMLOCK=infinity`` in its systemd unit. See `Registered
   memory`_.
#. Restart the OSDs and the gateways.

This configuration serves libfabric, cuObject and RC clients on
ConnectX adapters, and moves data between the daemons over libfabric::

  [global]
  oob_network           = 10.90.0.0/24
  oob_client_transports = verbs_ofi_rxm, cuobj, rc
  oob_cluster_transport = verbs_ofi_rxm

``fi_info`` names a layered provider with a semicolon, as in
``verbs;ofi_rxm``. A semicolon starts a comment in a configuration file,
and drops the rest of the line, so write the name with an underscore
instead: ``verbs_ofi_rxm``. Both spellings name the same provider, and
so does ``verbs`` alone.

The gateway then uses OSD-direct transfers for every token of a client
transport. Each transport also needs some host configuration. See
`cuObject`_, `libfabric`_ and `UET`_.

The out-of-band network
-----------------------

``oob_network`` works like ``public_network``. It lists subnets, and
each daemon takes the first address of its host in one of them. That
is the daemon's out-of-band address. The daemon finds the interface
that carries the address, and from the interface what each transport
needs:

* ``verbs_ofi_rxm``, ``cuobj`` and ``rc``: the RDMA device, its port,
  and the index of the RoCE v2 GID that holds the address. The GID
  table names the interface of each GID, so an address on a VLAN above
  a bond works too. The cuObject DC target behind an OSD's gather
  windows uses the same device, port and GID, also for an IPv6 address.
* ``uet``: the interface itself, which is the provider's domain.
* ``tcp`` and other providers: the address, to bind to.

If no RDMA device carries the address, the daemon logs ``no RDMA device
carries <address>``, and starts no transport that needs one.

Each entry of ``oob_network`` must be a subnet with a prefix length, as
in ``public_network``: ``10.90.0.0/24`` or ``fd00::/64``. An entry such
as ``10.90.0.5`` or ``10.90.0.0/255.255.255.0`` is an error. The daemon
logs it, for example ``oob_network entry '10.90.0.5' is not a subnet,
such as 10.0.0.0/24 or fd00::/64``. It then starts no out-of-band
transport, but it does not exit.

If ``oob_network`` is empty, each daemon uses its public address. That
works only when the RDMA adapter carries the public address.

Two options change the choice for one daemon:

* ``oob_network_interface`` limits the choice to the interfaces that it
  names. The value can use ``$id``, for example ``uet-o$id``.
* ``oob_addr`` gives the address directly. Set it in the section of the
  daemon, such as ``[osd.3]``.

Choosing transports
-------------------

``oob_client_transports`` lists the transports that the cluster serves
to clients:

* Each OSD starts an executor for the libfabric provider in the list,
  and one for ``cuobj``.
* The gateway starts its cuObject server for ``cuobj``, for
  gateway-staged mode, and its ``hipobj-rc-v2`` server for ``rc``.
* The gateway passes a cuObject descriptor to the OSDs only when the
  list names ``cuobj``. It passes a libfabric token only when the list
  names a libfabric provider. An OSD whose provider speaks another wire
  protocol than the token returns the stripe inline, and the gateway
  falls back. See `Fallback order`_.

``oob_cluster_transport`` names one transport for the transfers between
daemons. Every daemon must run the same one. It turns on two things:

* The primary of an erasure-coded read lends windows to its peer
  shards, which write their shard data into them. See `Gathering shard
  reads out of band`_.
* The gateway exposes its RC session buffers to the OSDs, which write
  their stripes into them. See `GET relay`_.

A libfabric provider works on any adapter that it supports. ``cuobj``
needs a ConnectX adapter on every host that runs it.

An OSD runs one libfabric endpoint. If ``oob_cluster_transport`` and
``oob_client_transports`` name different libfabric providers, the OSD
starts the provider of ``oob_cluster_transport``, and logs a warning
for the other one. A daemon also skips, with a warning, a transport
that it was built without or that fails to start.

Registered memory
-----------------

Each daemon that runs a transport allocates one arena of memory when it
starts. ``oob_memory`` sets the size of the arena, 2 GiB by default, in
whole blocks of 64 MiB. All transports of the daemon share it. A daemon
that runs no transport allocates nothing. An OSD whose transports all
fail to start frees its arena.

Transfers take buffers of the arena at the size that they need, rounded
up to a power of two from 1 MiB to 64 MiB:

* An OSD stages in one buffer each read that it delivers out of band,
  and each payload that it pulls. Each executor registers the whole
  arena once for this.
* The gateway stages each cuObject transfer of gateway-staged mode in
  one buffer.
* The gateway gives each RC session a buffer of the session's size. It
  takes the buffer from the arena at PREPARE. It registers the buffer
  for that session alone, because the client and the OSDs write into
  it. When the session ends, the gateway removes the registration and
  returns the buffer. A PUT's buffer and queue pair have their own
  protection domain. No other session's queue pair can reach the
  buffer, whatever key it holds.

The executor of the cluster transport also reserves 16 gather windows of
8 MiB, two blocks of the arena, when the OSD starts. It reserves them
only when at least one more block stays for staging, that is with an
``oob_memory`` of 192 MiB or more. Otherwise the OSD logs ``oob_memory
has no room for 16 gather windows of 8388608 bytes and for staging``,
and gathers stay inline. If the executor cannot set the windows up, the
two blocks go back to staging. That happens when the libfabric window
pool cannot register them, or when the cuObject DC target fails to
open. See `Gathering shard reads out of band`_.

A transfer larger than 64 MiB, or one that finds no room in the arena,
takes its fallback. An OSD delivers the read inline, and a shard replies
inline. A pull fails, and the gateway declines the PUT with ``501``. A
cuObject GET gets the HTTP body, and a cuObject PUT fails with ``503``.
An RC PREPARE fails with ``413`` for a transfer larger than 64 MiB, and
with ``503 SlowDown`` when the arena is full.

A window that a daemon lends to a peer gets a memory key of its own. A
libfabric window gets a new key when it comes back. The OSD's gather
windows behind a cuObject DC target keep their key. See `Reusing a
window`_.

The arena is far above the usual 8 MiB ``memlock`` limit. Give the OSDs
and the gateways ``LimitMEMLOCK=infinity``. For a vstart cluster, run
``ulimit -l unlimited``. Each OSD registers an arena of its own, so a
host locks ``oob_memory`` once for each OSD that it runs. On a host with
many OSDs, set a smaller ``oob_memory`` in the ``osd`` section.

The arena is not part of ``osd_memory_target``. The OSD maps it outside
the heap, so the cache autotuner sizes BlueStore's caches without it.
An OSD that runs a transport uses up to ``osd_memory_target`` plus
``oob_memory``, and the registration keeps the whole arena in memory.
Plan host memory for both. Lower ``osd_memory_target`` by
``oob_memory``. With cephadm's ``osd_memory_target_autotune``, lower
``mgr/cephadm/autotune_memory_target_ratio`` so that the host keeps
``oob_memory`` free for each OSD.

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
inline, and its result is marked declined, because no descriptor left
the client.

The descriptors travel after the fields of the version 9 ``osd_op``
message, and only when some operation carries one. An OSD that does not
know them ignores them and replies inline. The results travel the same
way, after the fields of the version 8 ``osd_op_reply``.

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
* The OSD's transfer threads refused or shed the transfer, over one of
  their caps or because the OSD stopped. See `Transfer threads`_.
* The descriptor has flag bits that the OSD does not know.

With an inline reply, the OSD also says whether it started a transfer
for the operation at all. When it did not, as in the cases above, or
when its executor sent nothing (no staging buffer, a budget already
spent, no address for the client yet, an endpoint busy adding a
client), the result is marked declined:
nothing of the operation reached the window. When the OSD delivered the
data over a transport whose writes are delivery-complete, the result is
marked landed: every byte was in the window before the reply. The RADOS
client marks a result resent when it sent the operation more than once,
since an earlier attempt may have started a transfer that this result
knows nothing of, unless every earlier attempt settled. librados
reports these as ``RDMA_DELIVERY_DECLINED``, ``RDMA_DELIVERY_LANDED``
and ``RDMA_DELIVERY_RESENT``.

An OSD of an older release marks results neither declined nor landed.
The caller then fences, because it cannot tell such a reply from a push
that was cut off. An OSD of this release that was built without any
transport marks every result that carries a descriptor declined. The
caller then does not need to fence its window. Such an OSD fails pulled
writes with ``EOPNOTSUPP``. The RADOS client reports declined by itself
only for descriptors that it withheld.

Retries
-------

An attempt is settled when its reply marked every operation that
carries a descriptor declined or landed. No write of a settled attempt can land
after its reply. An attempt that went out without its descriptors, below
an Umbrella ``require_osd_release``, is settled too.

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

An OSD starts the executors of the transports in
``oob_client_transports`` and ``oob_cluster_transport``: a libfabric
executor, a cuObject executor, or both. See `Choosing transports`_. It
chooses an executor for each token by the shape of
the token. A token whose third field is ``ofi`` and a version, such as
``ofi1``, goes to the libfabric executor. The executor serves it only
when the token names the wire protocol of the OSD's own endpoint. A
token whose third field is hex goes to the cuObject executor. A
libfabric token of another wire or of an unknown version, and any token
that no running executor serves, is delivered inline.

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
acting set can already serve. No push of the request starts after that
lease or the pool's ``rdma_delivery_lease`` ends. In a request with
several descriptor-bearing reads, a later read whose push cannot start
in time is delivered inline and declined.

Transfer threads
----------------

With ``osd_oob_transfer_threads`` at 0, the default, an OSD runs each
transfer on the op worker thread that serves the request. That thread
holds the PG's lock until the transfer ends, so other operations on the
PG wait. With a value above 0, that many transfer threads run the OSD's
pushes and pulls instead. These are the pushes of reads into client
windows, the pushes of erasure-coded shards into gather windows, and
the pulls of OSD-direct PUTs. See `Gathering shard reads out of band`_
and `Pulled writes on transfer threads`_.

Under the PG's lock, the op thread checks the read, plans its push, and
fixes the reply's versions, result and flags. It then hands the push to
a transfer thread, releases the object's lock and the request's unit of
``osd_client_message_cap``, and goes on. The reply still leaves only
after every write completes or is cut off. It never leaves before the
op thread releases the object's lock.

Replies to descriptor-bearing reads therefore leave a PG in the order
that their pushes complete. A later write to the same object can be
acknowledged before an earlier read's reply. That read still returns
the data and the version from before the write.

A push or a pull waits in a queue until a transfer thread is free. A
queued push starts nothing once the pool's lease ends, or once the PG
read lease that the OSD read at handoff ends. A queued pull starts
nothing once the pool's lease plus drain ends.
``osd_oob_transfer_queue_max`` and ``osd_oob_transfer_queue_bytes`` cap
the transfers that wait for or run on the transfer threads, and the OSD
refuses a transfer over either cap. In each case, the OSD delivers the
read of a push inline and marks it declined. A pull fails its write
with ``ETIMEDOUT`` when it waited too long, and with ``EOPNOTSUPP``
when the OSD refused it.

When ``osd_fast_shutdown`` is off, a stopping OSD refuses each new push
or pull for its transfer threads. Once its op threads stop, it sheds the
transfers still queued. A shed transfer starts nothing. The OSD delivers
the read of a shed push inline, marks it declined, and still sends the
reply. It then waits for the running transfers, each push until its
writes land or are cut off, and each pull until it ends. It stops its
messengers only after that.

Some transfers stay on the op thread, also with transfer threads.
cuObject transfers, pulls included, stay there unless
``osd_oob_cuobj_transfer_threads`` is set. Each thread that runs them
needs a DC initiator of its own (see `cuObject`_). The pulls of a
request that runs in a coroutine also stay on the op thread (see
`Pulled writes on transfer threads`_). The caps do not count the
transfers that stay on the op threads.

Pulled writes on transfer threads
---------------------------------

With ``osd_oob_transfer_threads`` above 0, a transfer thread runs the
pulls of an OSD-direct PUT (see `OSD-direct PUT`_). Under the PG's lock,
the op thread checks every pull of the write request, and reserves the
payloads in the client message throttle. It then hands one job with all
the pulls to the transfer threads. The write parks: it waits for the
job, and the op thread releases the object's lock and goes on.

When the pulls end, the transfer thread queues a kick for the PG. The
kick goes ahead of the client ops in the OSD's queue. Under the PG's
lock, it queues the write again, with the ops that wait behind it. The
write runs again from the start. It takes the pulled payloads, and goes
on as a write with inline data does. If a pull failed, the write fails
with that pull's error instead.

While the write waits, the PG keeps a hold on its object. The hold
keeps only the waiting ops, and no lock, so a scrub or a backfill can
still work on the object. Every later client op on the object or its
snapshots waits in the hold, behind the write. That includes reads, and
other pulled writes, which start no pull meanwhile. Ops on other objects
of the PG go on.

Writes to one object therefore keep their order. The PG checks the
holds before any other list that an op can wait in, such as the wait
for a scrub. The kick queues the write first, and the ops behind it
after it, in one step. So a scrub or a recovery cannot let a later op
pass the write. Only the flush ops of a cache tier, and PG ops such as
an object listing, pass a hold.

Nothing answers the write while its pulls run, not even with an error.
A client that is blocklisted meanwhile gets ``EBLOCKLISTED`` only after
the pulls end. The pulls end by the pool's lease plus drain after the
OSD received the write. Unlike a read, a waiting write keeps its unit of
``osd_client_message_cap`` and its reservation in the client message
throttle until it completes.

A request with a ``CALL`` on an erasure-coded pool with
``allow_ec_optimizations`` runs in a coroutine. Its pulls run on its op
thread, and it does not park. A pull on the op thread holds the object's
lock, the PG's lock and the op thread until it ends.

When the PG's interval changes and the OSD stays its primary, the PG
queues each waiting write again, with the ops behind it. A write whose
pulls still run then waits for the same pulls again. It does not pull
again, and it does not reserve the throttle again. When the OSD is no
longer the primary, or when it stops, the PG drops the waiting writes
and their ops. It sends them no reply.

When a write runs again, the PG can drop it as it drops any op: after a
split, a session reset or a backoff. A pull of a dropped write that has
not started never starts. A pull that already started runs until it
ends, and the OSD discards its payload. The client sends a dropped
write again.

The OSD keeps a list, by request, of the pulls that run on its transfer
threads. A later attempt of a write can reach the OSD while a pull of an
earlier attempt still runs. Examples are a resend after a split, a
session reset or an interval change. The later attempt then waits for
that pull in the hold of its object, in whichever PG it reaches. It
pulls nothing, and nothing answers it, until that pull ends. It then
pulls the window itself, or the OSD answers it as a duplicate when the
write is done.

A resend to another OSD, such as a new primary, does not wait. That OSD
can pull the window and answer while the earlier pull still reads it.
The OSD of the earlier attempt discards what it reads.

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

Only a GetObject request, an HTTP ``GET``, can take an out-of-band
mode. The gateway serves a SelectObjectContent request over HTTP
whatever token it carries, because Select runs the data through its
query engine. So does a GET on the website endpoint of an object that
has ``x-amz-website-redirect-location``, which the gateway answers with
its ``301``.

The gateway tries these modes in order, for each request:

#. Passthrough, when the request has a token of a transport in
   ``oob_client_transports``, ``rgw_rdma_osd_direct`` is on, and the
   request is eligible (see `Passthrough eligibility`_). If any stripe
   comes back inline, the gateway restarts the whole GET in the next
   mode. The client does not see the restart, because the gateway has
   not sent HTTP bytes yet. When stripe operations were already sent, the gateway first
   waits for the fence described in
   `Fencing a window before reuse`_, unless no write of the request can
   land any more: every stripe came back declined or landed, and none
   was resent. One OSD that does not deliver out of band then costs a
   re-read over HTTP, not the fence. A stripe read that fails fails the
   GET instead, and the gateway sends the error behind the same fence.
#. Gateway-staged mode, when the token is a cuObject descriptor and the
   gateway runs its cuObject server (``cuobj`` in
   ``oob_client_transports``). The whole response must fit the client's
   window, the size in the token, as for passthrough. The gateway's arena
   must also have room for a buffer that holds the whole response, at
   most 64 MiB. See `Registered memory`_.

   The gateway first puts every byte of the response in the window
   with its RDMA write. Only then does it send the status line,
   ``x-amz-rdma-reply`` and ``x-amz-rdma-bytes-transferred``. If the
   write fails or is short, the client gets a plain S3 error
   (``500 InternalError``) with no ``x-amz-rdma-*`` headers. The client
   must not use the window's contents then.
#. The HTTP body, with ``x-amz-rdma-reply: 501``. The cuObject protocol
   defines this value as the signal to fall back to HTTP.

Some stores and filters do not pass the token on, such as
``rgw_filter`` ``d4n`` or an older SAL store. They send the data to the
gateway instead. The gateway then serves that same read in the next
mode, and does not read the object again. If such a store also sent
stripe operations with the token, their writes could land over the
data. The gateway then fails the GET behind the fence. A gateway-staged
response becomes a ``500`` error, but a response over HTTP has already
gone out by then.

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
  success, every stripe's OSD placed its bytes before replying. The
  gateway then waits only when a stripe was resent, before the response
  and before a gateway-staged write into the window. The stripe's
  earlier attempt may still write. The window is then quiet before it is
  written again, and before the client hears back.

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
  posts no write after that point. It does not start a transfer when
  less than that is left; ``cuobj status`` counts those reads in
  ``budget_refused``.

  The writes of a transfer that stopped waiting stay posted on its
  thread's cuObject channel until they complete or fail. A write posted
  behind them would wait for them, and could land after its own
  transfer's drain. So until then the thread starts no transfer: it
  delivers its reads inline, with nothing staged, and ``cuobj status``
  counts them in ``channel_busy``. The first read on the thread that
  finds the earlier writes done gives the earlier transfer's staging
  buffer back. On an op worker thread, the buffer stays held until then,
  also when no further read comes. A transfer thread also makes this
  check each time it has nothing to run for 100 ms.

  A write whose submission fails may have posted part of itself, and
  nothing reports a completion for that part. The executor keeps that
  transfer's staging buffer until the DC retry budget has run out on the
  part. The budget counts from when the writes ahead of the part on the
  channel were done. The thread goes on starting transfers meanwhile. A transfer
  posted behind a part that is still retrying times out and holds the
  channel, as above.
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
``OOB_CUTOFF_LATE`` health warning, with the worst lateness, for an hour
after the latest one. Delivery goes on. 1 s is a third of the default drain, and
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
``OOB_CUTOFF_PAST_TOLERANCE`` health error for an hour, and goes on
delivering. Stopping the executor would not undo a write that already
landed, and it would turn every long pause into an outage until the OSD
restarts. Set ``osd_oob_cutoff_policy`` to ``disable`` to stop the
libfabric executor instead.

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
land in a client's window after the fence, and may keep doing so. With
``osd_oob_cutoff_policy`` set to ``disable`` or ``abort``, a quick
cut-off that ends later than the tolerance fails too, although its
endpoint is clean. When a cut-off fails, the OSD stops its libfabric
executor. It delivers inline every read that the executor would have
written, lends none of its gather windows, logs the cause to the
cluster log, and raises the ``OOB_DELIVERY_UNSAFE`` health warning.
With ``osd_oob_cutoff_policy`` set to ``abort``, the OSD exits instead. That ends the writes of a
software provider, and a device drops the queue pair's. Restart the OSD
once the transport is fixed. ``OOB_DELIVERY_DOWN`` means that a cut-off
could not reopen the endpoint; no write is at risk then, but libfabric
delivery has stopped.

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
operation that used a window ends, also when a gather or a relay
failed. An OSD's libfabric gather windows get a new key when the gather
releases them, in place where the provider can. When a window is
registered again instead, the key that leaves service is not used for
another window for 10 s. An endpoint also asks a provider with a fixed
table of regions, such as UET's, for a table of 16384 regions. A gather
window returned while a ``FI_THREAD_DOMAIN`` provider adds a client gets
its new key from a later gather instead, and is not lent before (see
`libfabric`_).

The gateway closes the libfabric relay window of an RC session when the
session ends, which retires the window's key. The registration cache of
the verbs provider keeps the key of a closed region valid. So a daemon
sets ``FI_MR_CACHE_MONITOR=disabled`` in its own environment before it
opens a libfabric endpoint. If the provider keeps the key of a closed
region all the same, the gateway finds this at startup and logs it.
Every relay buffer then stays out of use for 10 s after its session.

A libfabric window whose gather or relay may still have a write in
flight stays out of use for its quarantine all the same, and for 10 s
at least. Its old key keeps a late write out only if the provider drops
what carries it. A gather window whose re-key fails also stays out of
use for 10 s.

Only the OSD's gather windows behind a cuObject DC target keep their
key. The gateway registers each relay buffer behind its DC target for
one session, so each session's buffer has a key of its own. The gateway
removes that registration when the session ends. A buffer that OSD
writes may still reach stays registered until its quarantine passes,
and only then goes back to the arena. Until then, the registration
keeps the buffer's key from naming other memory.

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
needs from its peers in the sub-read replies, over the messenger. When
``oob_cluster_transport`` names a transport, the peers write their shard
data into the primary's memory over it instead. This needs a pool with
``allow_ec_optimizations``. The primary then decodes as before, and its
client delivery writes the logical data into the client's window.
Object data then crosses the cluster network out of band on both hops.
The first hop is from the shards to the primary, and the second is from
the primary to the client.

The gather covers every read that the primary decodes for a client,
whether or not the client's read carries a delivery descriptor. Reads
for partial writes and recovery reads do not gather. A partial write
needs the very shards that it reads, so it cannot decode around a shard
whose push failed its checksum.

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
``rdma_delivery_drain``. This covers a shard that replied inline with
data it read, since a write it attempted may still land, and a read
that was cancelled or restarted. A shard that read nothing - every
object of its sub-read failed, a media error say - attempted no write,
and its window comes back at once, so one object a shard cannot read,
read over and over, cannot use up the windows. So does the window of a
shard that says in its reply that its executor sent nothing into it:
the push was refused before any write went out, because the shard's
endpoint was adding a client, no staging buffer was free, or the lease
had run out, say. The libfabric executor also gives every released
window a new key, after which no write meant
for the last gather can land where the provider drops writes with a
retired key. A window whose data the primary used is lent again at once,
unless its new key fails. One whose data it did not use keeps its
quarantine, and stays out of use for 10 s at least once it has a new
key. A cuObject window gets no new key. See `Reusing a window`_.

With ``osd_oob_transfer_threads`` above 0, a shard runs its push on a
transfer thread, and sends its sub-read reply from there. Its op thread
reads and checks the sub-read under the PG's lock, hands the push off,
and goes on. The reply still leaves only after the push completes or is
cut off. So the primary reads a window only after the shard's reply. A
push that waits in the queue past the pool's lease, or that the shard
refuses or sheds, sends nothing. The shard then replies inline and says
that it sent nothing, as when its executor refuses a push.

A shard's sub-read replies then leave its PG in the order that their
pushes complete. A reply that leaves after an interval change carries
the old epoch, and the primary drops it. The primary still completes
the client reads of a PG in the order that it started them. So while a
shard's push is slow, later client reads of that PG wait for it. The
shard's PG goes on serving writes and other sub-reads meanwhile.

The executor of ``oob_cluster_transport`` lends the windows. It reserves
16 windows of 8 MiB in the OSD's arena when it starts. A shard read
larger than a window, or one that finds no free window, is returned
inline.

The libfabric executor registers each window on its endpoint, and
starts a progress thread that places the data pushed into the windows.
The cuObject executor puts the windows behind a DC target on the device,
port and GID that carry the OSD's out-of-band address. It registers
them for remote writes only, so no one that holds a window's token and
the DC key can read a window back. A peer can write into a window only
over the window's transport, so every OSD must run the same cluster
transport.

The token travels in a new trailing field of the sub-read message, and
the pushed extents, their crc32c, and whether the shard sent nothing,
in new trailing fields of the reply. An OSD of an older release ignores
the token and replies inline.

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

When ``oob_client_transports`` does not name ``rc``, the control routes
answer ``501`` with ``x-amz-rdma-protocol-status: unsupported``. That answer tells a
hipObject client to use plain HTTP.

GET relay
---------

For a GET, the session buffer sits between the OSDs and the client.
The object reaches the buffer in one of two ways:

OSD-direct relay
  The gateway exposes its session buffers to the OSDs. Each stripe read
  carries a delivery descriptor for the session buffer, and the OSDs
  write their stripes into it, as in passthrough mode. The gateway does
  not copy the object. This needs ``rgw_rdma_osd_direct`` and an
  ``oob_cluster_transport``. With a libfabric provider, the gateway
  exposes the buffers over libfabric, on any adapter that the provider
  supports. With ``cuobj``, it exposes them as a cuObject DC target,
  which needs an mlx5 adapter on the gateway.

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
``rdma_delivery_drain`` runs out after the lease. When the reads fail,
the gateway keeps the buffer out of use for the lease plus the drain,
unless every stripe came back declined or landed and none was resent.
Over cuObject, and over a libfabric provider without delivery-complete
writes, no stripe comes back landed, so such a relay is held unless
every stripe was declined. When the reads deliver the whole object,
every OSD placed its stripe before it replied, and the gateway holds
the buffer only when a stripe read was resent, because the read's
earlier attempt can write after every reply. That is so even when the
writes to the client fail afterwards. A session whose buffer is held
this way ends, even when the client's queue pair was only not ready,
which otherwise lets the client send READY again. When a stripe comes
back inline, the gateway waits for the same fence before staged relay
copies the object into the buffer.

The gateway also closes the buffer's libfabric window when the session
ends, which retires the window's key. No OSD write meant for that
session can then land where the provider drops writes with a retired
key. A buffer whose session left a write in flight keeps the
quarantine, and for 10 s at least. Any other buffer is free at once,
unless the provider caches registrations. Such a provider keeps the key
of a closed window, so every relay buffer then stays out of use for
10 s after its session. See `Reusing a window`_.

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

The OSD executor needs ``cuobj`` in ``oob_client_transports`` or in
``oob_cluster_transport``, a build with ``WITH_CUOBJ``, a ConnectX-5 or
newer adapter, ``rdma-core``, and NVIDIA's proprietary ``cuobjserver``
library. Gateway-staged mode for cuObject clients needs ``cuobj`` in
``oob_client_transports``, and the same build, adapter and library on
the gateway host. No GPU is needed on the OSD, gateway or client hosts.
Only GPU memory targets on the client need CUDA.

Each OSD creates one DC initiator for each op worker thread, and one
for each transfer thread with ``osd_oob_cuobj_transfer_threads``. The
gateway creates one for each frontend thread (``rgw_thread_pool_size``),
and at least 128. Every DC initiator and target uses ``cuobj_dc_key``.
The library chooses its ``rdma_cm`` port, and clients never connect to
it.

If the library has no DC initiator for a gateway frontend thread, the
thread logs ``cuObject channel allocation failed`` once. It then serves
its gateway-staged requests over HTTP. A GET gets the HTTP body with
``x-amz-rdma-reply: 501``. A PUT without a body that needs gateway-staged
mode is declined with ``501``, and the client sends the body. A GET that
took its staging buffer on another thread and resumes on this one fails
with ``500``.

Three parts of the host configuration are easy to miss:

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
  The cuObject executor and the cuObject server register the daemon's
  arena, ``oob_memory``, 2 GiB by default. That is far above the usual
  8 MiB ``memlock`` limit. Without more, the registration fails. See
  `Registered memory`_.

The RDMA address must belong to the RDMA device
  The daemon's address in ``oob_network`` must be an address that the
  ConnectX adapter carries. When the ConnectX ports are bonded and
  tenant traffic is VLAN-tagged, that is the address on the VLAN above
  the bond. It is usually not the public address, so an empty
  ``oob_network`` does not work there. ``ibv_devinfo`` and the GID
  table under ``/sys/class/infiniband/<device>/ports/1/gids`` show the
  addresses of the device. A RoCE v2 entry whose GID ends in the
  IPv4-mapped form of the address shows the pairing. If no adapter
  carries the address, the daemon logs ``no RDMA device carries
  <address>``.

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
equal to ``cuobj_dc_key``. The defaults on both sides agree.

libfabric
---------

The libfabric code is the ofi-rma library, in ``src/ofi-rma``. It
depends only on libfabric, so a client can link it to lend windows
without building Ceph. It can also lend windows in GPU memory, through
libfabric's ``FI_HMEM``. Set ``WITH_SYSTEM_OFI_RMA`` to build against an
installed ofi-rma instead of the submodule.

The owner of a window sends this token::

  <base hex>:<size hex>:ofi1:<wire>:<endpoint name hex>:<memory key hex>[:<auth hex>]

The fields are:

* ``base``: the remote address of the first byte of the window, as the
  owner's provider reads it. That is a virtual address when the
  provider uses ``FI_MR_VIRT_ADDR``, and an offset into the region
  otherwise. A writer writes byte ``i`` of the window at ``base + i``.
* ``size``: the length of the window.
* ``wire``: the wire protocol and its version, from the provider's
  ``fi_ep_attr``, for example ``rxm.1`` for ``verbs_ofi_rxm`` or
  ``xnet.1`` for ``tcp``. Both ends must speak the same wire. The token
  names the protocol, not the provider, so the UET providers of two NIC
  vendors take each other's tokens.
* ``endpoint name``: the bytes that ``fi_getname()`` returns for the
  owner's endpoint, at most 192 bytes. A writer passes them to
  ``fi_av_insert()``.
* ``memory key``: the key of the region behind the window. Providers
  with keys longer than 8 bytes are not supported.
* ``auth``: optional. The authorization key that the region is bound
  to, such as a UET JobID. ofi-rma 0.3 does not use it yet, and an OSD
  delivers a token that has it inline.

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
adds the peer. A write that gives up waiting for it has sent nothing,
and its read is delivered inline. The insert goes on, so a later read
finds the peer ready. The executor asks the provider for a thread-safe
domain. With one, the writes to a new peer wait for its insert, each
only until its deadline, and writes to other peers go on meanwhile.
The OSD logs at startup which case applies: ``concurrent peer
inserts`` or ``peer inserts pause writes``.

A provider that offers only ``FI_THREAD_DOMAIN`` allows no other call
during an insert, and adds one peer at a time. The insert first waits
until the writes in flight are done, so that none of them misses its
cut-off, and until the gather windows lent before it are back (see
below), and then holds the endpoint until the provider returns. The
endpoint is quiet from when the insert starts to wait until it is
done. An op thread or a transfer thread waits at most 10 ms for a quiet
endpoint, also for its own read's client. Once the endpoint has been
quiet for 10 ms, the thread does not wait at all. It then goes on
without the endpoint, so that the operations and transfers queued
behind it do not wait too:

* A read for any client, a new client's first read included, is
  delivered inline, and declined. A push this OSD makes as a shard of
  an erasure-coded read is delivered inline, and its reply says that
  nothing was sent, so the primary lends the window again at once.
  ``insert_busy`` counts both.
* A gather lends no window, and its shards reply inline.
  ``windows_declined_insert`` counts the windows not lent.
* A gather window returned meanwhile gets its new key from a later
  gather, and is not lent before. ``windows_rekeys_put_off`` counts
  it.

A quiet spell shorter than 10 ms, as when ``tcp`` adds a peer while no
write is in flight and no gather window is lent, refuses nothing.
Inserts queued back to back quiet the endpoint one after another.

The progress thread that places data in gather windows cannot poll
during the insert. A shard that pushes into a window then can wait for
the insert, and hold the thread that runs its push. The insert
therefore first waits for the windows lent before it, polling
meanwhile, so that the shards' pushes into them land: until the
gathers return them, which takes about as long as a gather, or until
8 s, the default lease plus drain, after a window was lent. A shard
whose push comes later still, in a pool with a longer lease or drain,
waits for the insert within its budget: a push that runs out of budget
is cut off, and the shard replies inline.

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
  Needs no special hardware. It binds to the daemon's address in
  ``oob_network``. It cannot cut off a write that misses its deadline,
  so use it for tests only.

``shm``
  Works between processes on one host. It is useful for tests.

``verbs_ofi_rxm``
  RDMA reliable connections on any verbs device, through libfabric's
  ``verbs;ofi_rxm``. The daemon finds the
  device, for example ``mlx5_0`` or ``rxe0``, from its address in
  ``oob_network``. On soft-RoCE the
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

* ``uet`` in ``oob_client_transports``, in ``oob_cluster_transport``,
  or in both.
* An address in ``oob_network`` on the interface that the OSD sends and
  receives on. The OSD uses the interface as the provider's domain, and
  the interface's IPv4 address is the OSD's fabric endpoint. The
  reference provider needs one interface for each process, so give each
  OSD an interface of its own, and set ``oob_network_interface`` to its
  name. The value can use ``$id``, for example ``uet-o$id``.
* The ``CAP_NET_RAW`` capability, for the raw socket. An OSD normally
  drops every capability that its block-device plugins do not need.
  When either option names ``uet``, the OSD also keeps ``CAP_NET_RAW``.

The gateway's relay windows need ``uet`` in ``oob_cluster_transport``,
an address in ``oob_network`` on the gateway's own UET interface, and
``CAP_NET_RAW``.

An OSD-direct PUT (see `OSD-direct PUT`_) needs RMA reads. The OSD
always asks for them, and the wrapper must offer ``FI_READ`` and
``FI_REMOTE_READ``. Under RUDI, a read is idempotent at
the client, so it goes over RUDI too: each request packet asks for one
packet of data, which the client sends back in its answer, keeping no
state for the OSD. When the wrapper offers ``FI_HMEM``, the client can
lend a GPU buffer, for GETs and PUTs alike. The reference provider
copies each packet with the CPU, so it can do that only for GPU memory
that the CPU can address, such as an APU's.

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
as a failed cut-off, and the OSD stops its libfabric executor. The OSD
also warns at startup when the wrapper says that closing does not
discard. See `Fencing a window before reuse`_.

A wrapper that offers ``FI_THREAD_SAFE`` lets the OSD add a new client
while its writes to other clients go on. One that offers only
``FI_THREAD_DOMAIN`` pauses the OSD's writes for each new client, and
reads, the new client's first among them, are delivered inline
meanwhile. See `libfabric`_.

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
checksum itself. When ``rgw_rdma_checksum`` is on, the gateway asks each
OSD for checksums of the data that the OSD delivered. The OSD computes
the values from the bytes that it writes. For an erasure-coded primary
read, those are the bytes after reconstruction. The OSD reports one
value for each range of its placement plan. When the plan has one
range, that value also stands for the whole reply. The executor does
not matter.

The OSDs compute CRC-64/NVME, CRC-32C, or both. A client asks for one
with the request header ``x-amz-rdma-checksum-algorithm``, which takes
``CRC64NVME``, the default, or ``CRC32C``. For a GET of the whole object
whose stored full-object checksum is CRC-64/NVME or CRC-32C, the gateway
also asks for that algorithm. Any other value of the header
fails the GET with ``400 InvalidArgument``.

The gateway folds the values in logical order with the same combining
math that S3 uses for multipart full-object checksums. For a
shard-direct read, the RADOS client first folds the ranges of all
shards. The ranges must cover the operation's bytes without gaps. The
fold
works the same way for replicated and erasure-coded pools.

The response reports each folded value in its own header,
``x-amz-rdma-checksum-crc64nvme`` or ``x-amz-rdma-checksum-crc32c``,
rendered as S3 renders ``x-amz-checksum-crc64nvme`` and
``x-amz-checksum-crc32c``. Those S3 headers name the stored checksum of
the whole object. These headers cover exactly the bytes of the
transfer, so a ranged GET gets the checksum of its range.
A client can compare the value with the bytes in its own buffer, which
also covers the final write into its memory. A stripe from an OSD of an
older release comes back without a value. The response then carries no
header for that algorithm.

Some objects have a stored full-object ``crc64nvme`` or ``crc32c``
checksum, the AWS ``x-amz-checksum-crc64nvme`` and
``x-amz-checksum-crc32c`` types. For a whole-object GET of such an
object, the gateway compares the folded value with the stored checksum.
It does so before it sends any response bytes. A mismatch fails the GET
with ``500``, and the gateway logs both values.

This comparison covers the storage node, the reconstruction, and the
shard transfers inside the cluster. Ranges with gaps between them do
not fold into a whole-object checksum, so a sparse read with several
extents is not compared.

An OSD of an older release does not know the CRC-32C request. It
delivers such a read inline, so the GET falls back to HTTP. It fails
such a pull, so the PUT falls back to the HTTP body. Before you ask for
CRC-32C, make sure that every OSD runs this release.

If ``rgw_rdma_checksum`` is on, RC clients get the same headers in the
READY answer. The READY request asks for an algorithm with
``x-amz-rdma-checksum-algorithm``. For a relayed GET, the gateway uses
the OSDs' values. If a stripe reported no value, the gateway computes
the values from its own buffer. A READY answer of a PUT also reports the object's stored
checksum in ``x-amz-checksum-<algorithm>``.

Configuration reference
=======================

Build options
-------------

``WITH_OOB_OFI``
  The libfabric transport: the OSD's executor, gather windows over
  libfabric, and the gateway's relay windows over libfabric. Needs the
  libfabric headers and library, API version 1.18 or later.

``WITH_CUOBJ``
  The cuObject transport, in the OSD and in the gateway, with NVIDIA's
  ``cuobjserver`` library. It also needs the ``mlx5`` direct verbs at
  build time: ``libmlx5`` and ``infiniband/mlx5dv.h``, from the
  development package of ``rdma-core``. The OSD's DC target for gather
  windows uses them. Without them, the configure step fails.

``WITH_RDMA_RC``
  The gateway's ``hipobj-rc-v2`` server. It is on by default when
  ``WITH_RDMA`` is on. With ``cuobj`` as the cluster transport, the
  gateway's DC target also needs the ``mlx5`` direct verbs library at
  build time.

Pool options
------------

The OSDs enforce both options, and the gateway reads the same values
from the OSDMap, so no daemon setting must agree with them. Set them
with ``ceph osd pool set <pool> <option> <seconds>``. A value of 0
restores the default. The lease can be at most 6 seconds and the drain
at most 4 seconds.

The monitor rejects larger values, because a transfer can hold an op
thread and the PG's lock while it runs. A transfer on a transfer thread
holds neither (see `Transfer threads`_). The client ops on the object
of a pulled write still wait for its pulls.

``rdma_delivery_lease``
  How long, in seconds, after it receives a stripe operation an OSD can
  still take on a transfer against its delivery descriptor. A transfer
  that would start later is delivered inline. The default is 5.

``rdma_delivery_drain``
  How long, in seconds, after the lease every write that an OSD started
  lands or is cut off. Make it longer than a transport needs to cut off
  its writes. That is about two seconds for cuObject. For libfabric, it
  is the time an endpoint takes to close and reopen. The default is 3.

Common options
--------------

Set these options in the ``global`` section, so that every OSD and
every gateway reads the same values. A daemon reads them only when it
starts.

``oob_network``
  The subnets of the out-of-band network, as a comma-separated list.
  Each daemon takes the first address of its host in these subnets. The
  default is empty, which means the daemon's public address. See `The
  out-of-band network`_.

``oob_network_interface``
  Limits the choice of address to these interfaces. The value can use
  ``$id``. The default is empty.

``oob_addr``
  The out-of-band address of one daemon. It overrides ``oob_network``.
  Set it in the section of that daemon. The default is empty.

``oob_client_transports``
  The transports that the cluster serves to clients: a libfabric
  provider, ``cuobj`` and ``rc``. The default is empty, which turns
  out-of-band transfers with clients off. See `Choosing transports`_.

``oob_cluster_transport``
  The transport between daemons: one libfabric provider, or ``cuobj``.
  The default is empty, which keeps gathers on the messenger and relays
  in the gateway. See `Choosing transports`_.

``oob_memory``
  The size of the arena that each daemon allocates for out-of-band
  transfers, in whole blocks of 64 MiB. The default is 2 GiB. It comes
  on top of ``osd_memory_target``. See `Registered memory`_.

``cuobj_dc_key``
  The DC key of every cuObject DC initiator and target in the cluster.
  It must match the key of the cuObject client library. The default is
  ``0xffeeddcc``, the library's default.

Gateway options
---------------

``rgw_rdma_osd_direct``
  Have the OSDs move object data straight to and from client memory.
  The OSDs write the data of a GET into client windows and into RC relay
  buffers. They pull the object of a body-less PUT out of client memory.
  The default is true. See `Data paths`_ and `OSD-direct PUT`_.

``rgw_rdma_checksum``
  Compute checksums of out-of-band transfers. The gateway reports them
  in ``x-amz-rdma-checksum-<algorithm>``, and compares whole-object
  passthrough GETs with the stored checksum. An OSD-direct PUT computes
  them whatever this option says. The default is true. See
  `Integrity`_.

RC sessions:

* ``rgw_rdma_rc_max_sessions`` and ``rgw_rdma_rc_max_sessions_per_user``:
  the session limits, 1024 and 64 by default. A PREPARE over a limit,
  or one that finds no room in the arena, gets ``503 SlowDown``.
* ``rgw_rdma_rc_send_depth``: the writes to the client that can be in
  flight for one session, 64 by default.
* ``rgw_rdma_rc_prepare_timeout_ms`` and ``rgw_rdma_rc_exec_timeout_ms``:
  how long a session waits for READY, and how long a transfer can take,
  100 s and 30 s by default.

Each session holds one buffer from PREPARE until the session ends. A
transfer larger than 64 MiB is refused with ``413``. The buffer comes
from the arena, and goes back to it when the session ends.

A buffer that an OSD write can still land in stays out of the arena for
the pool's lease plus drain. It stays out for 10 s at least when it had
a libfabric relay window. See `GET relay`_. Such a buffer no longer
counts against its user's session limit. Until it goes back, no RC
session and no cuObject staging can use that memory. If the gateway
cannot remove the buffer's registration, it never uses that memory
again.

OSD options
-----------

``osd_oob_cutoff_policy``
  What the OSD does when its libfabric executor cannot cut off writes
  in time. With ``warn``, the default, a cut-off that fails stops the
  executor and raises ``OOB_DELIVERY_UNSAFE``. A quick cut-off that
  ends later than the tolerance raises ``OOB_CUTOFF_PAST_TOLERANCE``,
  and delivery goes on. With ``disable``, such a late cut-off also
  stops the executor. With ``abort``, the OSD exits in both cases. See
  `Fencing a window before reuse`_.

``osd_oob_cutoff_late_tolerance``
  How late a cut-off can end, past the budget it protects, and still be
  only counted and warned of (``OOB_CUTOFF_LATE``). A close or cancel
  that itself takes longer fails. The default is 1000 ms.

Only the libfabric executor cuts off writes, so these two options apply
to it alone. The cuObject executor stops waiting ahead of the deadline
instead. See `Fencing a window before reuse`_.

``osd_ec_repair_on_read``
  Have the primary repair a shard whose store failed a client read with
  a media error. The default is true. See `Shard read errors`_.

The OSD reads the options of its transfer threads only when it starts.
See `Transfer threads`_.

``osd_oob_transfer_threads``
  The number of transfer threads that run the OSD's pushes and pulls.
  The default is 0, which runs each transfer on its op thread.

``osd_oob_transfer_queue_max``
  How many transfers can wait for or run on the transfer threads. The
  OSD refuses a transfer past this cap. It delivers the data of a
  refused push inline, and fails the write of a refused pull. The
  default is 64.

``osd_oob_transfer_queue_bytes``
  How many bytes of data the transfers that wait for or run on the
  transfer threads can hold. The OSD refuses a transfer that takes them
  past this cap. The default is 512 MiB.

``osd_oob_transfer_thread_timeout``
  The heartbeat grace of a transfer thread. A thread that runs one
  transfer for longer marks the OSD unhealthy, but the OSD never kills
  it. A transfer ends within the pool's lease plus drain from receipt,
  which is 10 s at most, plus ``osd_oob_cutoff_late_tolerance`` for a
  late cut-off. With transfer threads, the OSD raises a value that does
  not cover that to 1 s past it, and logs a warning. The default is
  30 s.

``osd_oob_cuobj_transfer_threads``
  Run cuObject transfers on the transfer threads too. The default is
  false, which keeps them on the op threads. With true, the OSD also
  creates a DC initiator for each transfer thread. See `cuObject`_.

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
writes in flight, and these counters:

* ``buffers_leaked``: the staging buffers that plans kept. A plan keeps
  its buffer when it ends with writes outstanding, because it timed out
  or a poll of the channel failed without resetting it. It also keeps
  it when a submission failed.
* ``buffers_reclaimed``: the kept buffers that came back since.
* ``stale_completions``: the completions of earlier plans' writes that a
  later read took.
* ``channel_busy``: the reads delivered inline, with nothing staged,
  because writes of a plan that timed out, or whose poll failed, were
  still posted on the thread's channel. The buffer of a failed
  submission does not hold the channel.
* ``budget_refused``: the reads delivered inline, with nothing staged,
  because less than the DC retry budget was left.

A kept buffer comes back when a later cuObject read on its thread finds
its writes completed, or when the transport resets the channel. A
transfer thread also checks its channel each time it has nothing to run
for 100 ms. An op worker thread that serves no further cuObject read
keeps the buffer, so
``buffers_leaked`` can stay above ``buffers_reclaimed`` on an idle OSD.

``ofi status`` shows the windows lent and exhausted, ``staging_busy``,
the reads delivered inline because no staging buffer was free, the
provider and how the endpoint cuts off writes (``transport``), and the
endpoint's last error (``last_error``). It also shows:

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
  endpoint; with a ``FI_THREAD_DOMAIN`` provider, ``insert_busy``
  counts those that gave up on an insert. ``peers``,
  ``pending_inserts`` and ``inserts_refused``. See `libfabric`_.
* ``insert_busy``: calls of op or transfer threads that gave up on an
  insert of a ``FI_THREAD_DOMAIN`` provider, having waited for it at
  most 10 ms: writes refused with nothing sent, to a known client or to
  a new one not yet added (the read delivered inline and declined, or
  the shard's push delivered inline), gathers' reads of a window ordered
  without the endpoint, and, rarely, a gather window's token or new key
  asked for just as an insert began.
  ``windows_declined_insert``: gather windows not lent meanwhile, one
  for each shard a gather would have lent one to.
  ``windows_rekeys_put_off``: windows returned meanwhile, re-keyed by a
  later gather. See `libfabric`_.
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

An OSD that started a transport also reports the ``oob_xfer``
performance counters, which ``ceph daemon osd.N perf dump oob_xfer``
shows. See `Transfer threads`_.

* ``queued``: the pushes and pulls that wait for a transfer thread.
* ``running``: the pushes and pulls that run now, on a transfer thread
  or on an op thread.
* ``refused``: the transfers refused over a cap or while the OSD
  stopped.
* ``expired``: the transfers that waited too long to start: a push past
  its lease, a pull past the lease plus drain.
* ``shed``: the transfers still queued when the OSD stopped.
* ``queue_lat``: how long a transfer waited for a transfer thread.
* ``push_lat``: how long a push took, on a transfer thread or on an op
  thread.
* ``pull_lat``: how long the pulls of one write request took, on a
  transfer thread or on an op thread.

A refused, expired or shed transfer starts nothing. The data of such a
push goes inline. A refused or expired pull fails its write. When
``expired`` or ``refused`` grows under load, the transfer threads do
not keep up. More reads then go inline, and the gateway declines more
PUTs.

``ceph daemon osd.N dump_oob_transfers`` shows the settings of the
transfer threads and the number of queued transfers. Its
``pinned_bytes`` is the data that the transfer threads hold. The command
also shows each running transfer:

* ``kind``, which is ``push`` or ``pull``, ``age``, ``queued_for``,
  ``post_by_in``, ``pinned_bytes``, and ``inline``, which is true for a
  transfer that runs on its op thread.
* For a read push: ``object``, ``op``, ``bytes`` and ``ranges``.
* For a push of an erasure-coded shard: ``pgid``, the shard's own PG
  instance, such as ``1.0s1``, ``sub_read``, the primary's sub-read
  tid, and ``bytes``.
* For a pull: ``object``, ``ops``, the number of pulls in the write
  request, and ``bytes``, their payloads together.

A read whose op thread handed its push off gets the event
``oob_handed_off`` in ``ceph daemon osd.N dump_historic_ops``. A read or
a shard's sub-read with a push gets ``oob_settled`` when its reply
leaves. ``op_r_latency`` counts such a read from receipt to its reply,
with the push included. A pulled write that waits for its pulls, and
each op that waits behind it, gets the event ``waiting for oob pull``.

``ceph pg <pgid> query`` lists the holds of the PG in
``oob_pull_holds``, one for each object. Each entry has the ``object``,
the ``owner``, the request ID of the waiting write, and the request IDs
of the ``waiters`` behind it. ``age`` is the time since the hold began,
in seconds. ``settled`` is true once the pulls that the hold waits for
ended. ``own_pulls`` is false when the owner waits for the pulls of an
earlier attempt of its request. See `Pulled writes on transfer
threads`_.

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
  that slow first contacts on a ``FI_THREAD_DOMAIN`` endpoint hold no
  op thread longer than the insert wait, whether it lends, returns or
  reads a window or writes to a known or a new peer, that their inserts
  let pushes into windows lent before them land first, eviction,
  cut-offs that fail or end late, within and beyond the tolerance,
  writers left idle near their deadlines, late writes cancelled alone
  (over ``tcp``, with a test hook that stands in for a provider whose
  cancel discards), the cut-off cost estimate,
  writes that start too late, concurrent writes and their CPU use,
  that a write with a re-keyed window's old token does not land, and
  the gather window cycle under load, with cut-offs alongside.
* ``unittest_ec_gather`` tests the check of gathered shard data against
  its shard's checksum, and that a reply says when its shard sent
  nothing into the window. It also runs a shard's push through the
  transfer threads with a fake executor: placed, failed, expired,
  refused and shed.
* ``unittest_oob_xfer`` tests the transfer threads: their caps,
  transfers past their leases, and shedding at stop. It also tests
  transfers that run on the submitting thread, the heartbeat of a long
  transfer, and what ``dump_oob_transfers`` shows.
* ``unittest_oob_read_delivery`` tests that the read push gives the same
  replies as the earlier code, for every kind of read and outcome. It
  also tests the handoff to the transfer threads, pushes past their
  leases and shed pushes. It checks that the reply leaves only after the
  op thread releases the object.
* ``unittest_oob_pull`` tests the pulled write with a fake executor. It
  covers the reservation in the client message throttle, also for a
  request larger than the limit. It tests that no pull starts before
  every pull is checked, and that the pulls are all or nothing. It
  checks that the checksums match the earlier code, and that a second
  run pulls and reserves nothing. It checks that an error reply declines
  only the ops whose pull started nothing. It covers the park and the
  kick, the job of a dropped write, and a resend that waits for an
  earlier job.
* ``unittest_oob_placement`` tests placement plans.
* ``unittest_oob_abandoned`` tests how the cuObject executor counts a
  completion against the plan that posted it, and when it gives the
  staging buffer of a plan that timed out back.
* ``unittest_rdma_token`` tests token parsing and transport lists.
* ``unittest_crc64nvme`` tests CRC-64/NVME against the NVM Command Set
  test cases, and checks that its implementations agree.
* ``unittest_rdma_delivery`` tests the wire formats of delivery
  descriptors and results, how the client library marks a result
  resent, and when an attempt counts as settled, so that a resend
  carries ``FLAG_PRIOR_SETTLED``. It follows a push that an OSD cut off
  through the client library into the gateway's decision to fall back
  to HTTP. It also checks that ``osd_op`` and ``osd_op_reply`` carry the
  descriptors and results only as a suffix of their version 9 and
  version 8 encodings. It checks that the client library reports
  withheld descriptors as declined, and that ``ObjectOperation::dup()``
  keeps the descriptor and result vectors aligned with the operations.
* ``qa/standalone/erasure-code/test-erasure-eio.sh`` covers shard-direct
  reads whose shard read fails with ``-EIO``, and their bounce to the
  primary.
* ``qa/standalone/osd/osd-oob-async.sh`` runs pushes on transfer threads
  over the libfabric ``tcp`` provider. While one push is held, the other
  operations on its PG do not wait for it, and with 0 threads they do.
  It covers read pushes and erasure-coded shard pushes, an interval
  change, a slow stop, and pushes past the lease or over
  ``osd_oob_transfer_queue_max``. Its librados client is
  ``ceph_test_osd_oob_read``, which needs a build with ``WITH_OOB_OFI``.

  It also runs pulls on transfer threads. While a pull is held, the
  operations on other objects of its PG do not wait, and with 0 threads
  they do. An inline write and a read behind the pulled write keep their
  order. It holds a pull during a deep scrub, a backfill, a repeer, a
  primary change, a blocklist, a session reset and a slow stop. It
  splits the PG before the pull starts, and while it runs.

  It checks that a pulled write with a ``CALL`` on an erasure-coded pool
  with ``allow_ec_optimizations`` pulls on its op thread. The ``put``
  mode of ``ceph_test_osd_oob_read`` sends the pulled writes. It prints
  ``crc=declined`` for a write whose resend the OSD answered as a
  duplicate.
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

* With ``osd_oob_transfer_threads`` at 0, the default, an OSD runs each
  transfer on the op worker thread. It waits for the transfer to end
  before the next operation on that thread. A cuObject transfer always
  runs that way, unless ``osd_oob_cuobj_transfer_threads`` is set. So
  does the pull of a request that runs in a coroutine. See `Transfer
  threads`_.
* While a pulled write waits for its pulls on a transfer thread, every
  client op on its object waits behind it, reads included. See `Pulled
  writes on transfer threads`_.
* An erasure-coded primary completes the client reads of a PG in the
  order that it started them. While a shard's push is slow, later
  client reads of that PG wait for it, also with transfer threads.
* The cuObject executor keeps one channel per thread that runs its
  transfers. After a transfer on it times out, the thread delivers its
  reads inline until the transfer's writes complete or the DC transport
  gives up on them, for up to the DC retry budget.
* An OSD copies each read into a staging buffer. It does not register
  the read's own buffers.
* An OSD runs one libfabric endpoint, so ``oob_cluster_transport`` and
  ``oob_client_transports`` can name one libfabric provider between
  them. See `Choosing transports`_.
* Every call into one libfabric endpoint is serialized, except the
  adding and removing of peers on a provider that offers
  ``FI_THREAD_SAFE``.
* Every libfabric endpoint of one provider must use the same address
  family. Mixed IPv4 and IPv6 endpoints are not tested.
* A split read, erasure-coded shard-direct or replicated balanced,
  delivers a sparse read inline.
* Only cuObject clients can PUT with ``x-amz-rdma-token`` in
  gateway-staged mode. The object must fit one buffer of the gateway's
  arena, 64 MiB, and a PUT that finds no room fails with ``503``.
  Libfabric clients have no
  gateway-staged mode, so a libfabric GET that cannot use passthrough
  gets the HTTP body. A PUT with a body and a libfabric token is read
  over HTTP, as a gateway that does not run its cuObject server reads
  a PUT with a body and any token, and the response has no
  ``x-amz-rdma-reply`` header. A gateway that runs its cuObject server
  takes any other token on a PUT for a cuObject descriptor, and a token
  whose address and size do not parse fails the PUT with ``400``.
* An OSD-direct PUT does not serve appends, multipart parts, compressed
  or encrypted objects. The pulled payload counts against the OSD's
  client message throttle, but not against the cost that the OSD's
  mClock scheduler charges the request. If a stripe write was resent
  after its OSD committed it, the stripe has no checksum, and the
  gateway declines the PUT. The cuObject pull is compiled against a
  header stub only and is not tested on hardware.
* The gateway does not serve encrypted objects, or objects with a DLO
  or SLO manifest, to RC clients. PREPARE answers ``501`` with the
  unsupported marker, and the client reads the object over HTTP.
* An RC GET can name a ``versionId``. Multipart part uploads and other
  subresources are not available over RC.
* An RC PUT must arrive as one write-with-immediate that carries the
  whole object.
* An RC transfer runs on the request thread.
* Each RC session registers its buffer when PREPARE runs, and removes
  the registration when the session ends. A PUT also zeroes its buffer.
  On an E810 adapter, this costs about 2.5 ms for a 64 MiB PUT session.
  Zeroing takes 1.6 ms, registering 0.74 ms and deregistering 0.21 ms.
  At 4 MiB it costs about 0.13 ms. Soft-RoCE (``rxe``) is similar.
* The cuObject executor's gather windows, and cuObject transfers on the
  transfer threads, are not tested on hardware.

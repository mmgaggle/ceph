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
  Writes to libfabric tokens with any libfabric provider. It also
  carries the transfers that stay inside the cluster: shard reads that
  an erasure-coded primary gathers, and stripes into the gateway's
  relay windows.

Every out-of-band transfer is advisory. When a daemon cannot deliver
out of band, it sends the same bytes in band, and the request still
completes with correct data.

How a read reaches client memory
================================

The delivery descriptor
-----------------------

A RADOS read can carry a delivery descriptor. The descriptor belongs to
one operation in the request, so each read in a compound request can
carry its own. It holds these fields:

* ``token``: the client's token, which the OSD does not interpret.
* ``base_offset``: the offset in the window where the first byte of the
  operation's data goes.
* ``flags``: requests, such as a CRC-64/NVME of the delivered bytes.

librados clients set it with ``ObjectReadOperation::set_rdma_delivery()``.

An OSD that delivers the data out of band writes it into the window and
reports the byte count in the operation's result. An OSD that cannot or
will not deliver it replies with the data inline, as if the request had
no descriptor. An OSD replies inline in these cases:

* No running executor serves the token.
* The pool's ``rdma_delivery_lease`` expired before the transfer
  started. See `Fencing a window before reuse`_.
* The request is a retransmission. RADOS resends reads after peering
  changes, and an inline reply makes sure that a stripe is never
  written twice.
* The descriptor has flag bits that the OSD does not know.

Executors and tokens
--------------------

An OSD starts the executors that ``osd_oob_transports`` lists. It
skips, with a warning, a transport that it was built without or that
fails to start. It chooses an executor for each token by the shape of
the token. A token whose third field is ``ofi1`` goes to the libfabric
executor, but only when the token names the OSD's own provider
(``osd_ofi_provider``). Any other token goes to the cuObject executor.
A token that no running executor serves is delivered inline.

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
the reply. A reply therefore means that the bytes are in the window.

Before an OSD starts a transfer, it reads its PG read lease
(``readable_until``) again. The readability test at dispatch does not
cover a read that stalled after dispatch. A primary that lost contact
with its peers replies inline, instead of writing into a window that a
new acting set can already serve.

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
   yet. When stripe operations already reached the OSDs, the gateway
   first waits for the fence described in
   `Fencing a window before reuse`_.
#. Gateway-staged mode, when the token is a cuObject descriptor and the
   gateway runs its cuObject server (``cuobj`` in
   ``rgw_rdma_transports``).
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
* An OSD delivers a retransmitted request inline.
* The pool's ``rdma_delivery_lease`` bounds how long after receipt an
  OSD can still start a transfer. This covers an OSD that disappears
  during the request, and an operation that the gateway's RADOS client
  sent again. Before a fallback rewrites the window, the gateway waits
  for the lease plus ``rgw_rdma_fence_drain_ms``. That outlasts the
  lease and the transport's drain, so the window is quiet before it is
  written again.

The lease bounds the OSD side only. It is not a bound on how long a
client keeps a window registered. The lease is measured against the
wall clock, so it is a best-effort fence across clock steps. Give it
some slack.

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
    ``split_reads`` flag. Replicated pools always have that flag.
  * ``rados_replica_read_policy = balance`` on the gateway. The pool
    flag only permits split reads. A read is split only when the client
    asks for a balanced read.

  With only the pool flag, reads go to the primary.

The interleave happens only when a gateway stripe spans several
erasure-coded stripes. When ``rgw_obj_stripe_size`` equals the pool's
``stripe_width``, each shard holds one contiguous range of the request.
The plan then has one write, as for a primary read.

Gathering shard reads out of band
---------------------------------

The primary of an erasure-coded read normally collects the shards it
needs from its peers in the sub-read replies, over the messenger. With
``osd_oob_gather`` on, the peers write their shard data into the
primary's memory over an executor. The primary then decodes as before,
and its client delivery writes the logical data into the client's
window. Object data then crosses the cluster network out of band on
both hops. The first hop is from the shards to the primary, and the
second is from the primary to the client.

The primary lends one registered window to each peer shard that it
reads from, and puts the window's token in the sub-read. The shard
reads as usual, writes all the data it read into the window, and
replies with only the extents. The primary rebuilds the shard buffers
from the window and returns the window to its pool. The primary still
reads its own shard directly.

The gather is advisory too. A shard that cannot write replies inline.
A window whose data the primary did not use stays out of use for the
pool's ``rdma_delivery_lease`` plus a drain bound. This covers a shard
that replied inline, and a read that was cancelled or restarted.

The first transport in ``osd_oob_transports`` that started lends the
windows. The libfabric executor registers them on its endpoint. The
cuObject executor puts them behind a DC target on the OSD's adapter.
Peers must run the same transport to write into a window.

The token travels in a new trailing field of the sub-read message, and
the pushed extents in a new trailing field of the reply. An OSD of an
older release ignores the token and replies inline.

Reliable Connection clients (hipobj-rc-v2)
==========================================

Control protocol
----------------

An RC transfer needs a queue pair on each side, paired before data
moves. The client and the gateway exchange the pairing parameters in
three SigV4-signed HTTP requests:

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

Every ``x-amz-rdma-*`` request header must be in the SigV4
``SignedHeaders`` list. The gateway refuses a request with an unsigned
protocol header. A device on the network path can change such a header
without breaking the signature. The gateway also
refuses anonymous requests. A session belongs to the user that
prepared it. READY and CANCEL from another user fail as if the session
did not exist.

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
  object from RADOS as usual and copies it into the session buffer. The gateway uses this path when OSD-direct relay
  is not available, and for compressed objects. It is also the fallback
  when an OSD returns a stripe inline.

In both cases the gateway sends the bytes to the client while the read
is still in progress. Each time the start of the buffer is complete up
to a new point, the gateway writes the new bytes to the client. The
last write carries the session cookie as its immediate value. RC
delivers writes in order, so the completion of the last write tells the
client that all earlier writes are in its memory.

When a relay fails after the OSDs received delivery descriptors, an OSD
can still write into the session buffer until the pool's
``rdma_delivery_lease`` expires. The gateway keeps that buffer out of
use for the lease plus ``rgw_rdma_fence_drain_ms``.

PUT
---

For a PUT, the gateway registers a staging buffer at PREPARE and gives
its address to the client. The client writes the whole object with one
RDMA write-with-immediate. The gateway then stores the buffer through
the normal PUT path. Bucket default encryption, compression,
notifications and object lock apply as they do to any PUT.

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
for cuObject clients needs ``cuobj`` in ``rgw_rdma_transports``, a
build with ``WITH_RADOSGW_CUOBJ``, and the same adapter and library on
the gateway host. No GPU is needed on the
OSD, gateway or client hosts. Only GPU memory targets on the client
need CUDA.

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
  at the defaults.
  That is far above the usual 8 MiB ``memlock`` limit. Give the OSDs,
  and the gateway in staged mode, ``LimitMEMLOCK=infinity``. For a
  vstart cluster, run ``ulimit -l unlimited``.

The RDMA address must belong to the RDMA device
  ``osd_cuobj_rdma_ip`` must name an address that the RDMA device
  carries. When the ConnectX ports are bonded and tenant traffic is
  VLAN-tagged, that is the address on the VLAN above the bond. It is
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

Providers that progress manually place incoming data only while the
application polls them. Each window owner in Ceph polls its endpoint
from a thread. A libfabric client must poll its endpoint while it waits
for the response, or use a provider with automatic progress.

These providers are known to work:

``tcp``
  Needs no special hardware. Set ``osd_ofi_node`` to the address the
  OSD sends from.

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

The reference provider has these limits:

* It is software. A window owner places incoming data only while it
  polls its endpoint.
* It keeps process-wide state, so each process has one interface and
  one endpoint. Processes on one host need separate interfaces and
  addresses.
* The first write to a new peer runs ``ip route``, ``arp`` and ``ping``
  to find the next hop, and the endpoint waits until ``ping`` returns.
  If the peer does not answer the ping, the wait is 10 seconds. The
  OSD's write then times out, and the read is delivered inline.
* It sends writes unencrypted, because the security sublayer is not
  configured.
* It supports IPv4 only.

To run several endpoints on one host, put each UET interface in its own
VRF, the gateway's interface included. The provider finds peers with
``ip route get``. Without VRFs, a peer's address is a local address of
the host, so the lookup fails or the ping gets no answer.

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
GET.

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

Pool option
-----------

``rdma_delivery_lease``
  How long, in seconds, after it receives a stripe operation an OSD can
  still start a transfer against its delivery descriptor. A transfer
  that would start later is delivered inline. The default is 5. Set it
  with ``ceph osd pool set <pool> rdma_delivery_lease <seconds>``. The
  OSDs enforce the value, and the gateway reads the same value from the
  OSDMap, so no daemon setting must agree with it.

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
* ``rgw_rdma_fence_drain_ms``: the transport drain bound added to the
  lease before the gateway rewrites or reuses a window that OSDs can
  still write. Make it cover the transport's retry budget, which is
  about two seconds at the cuObject defaults. The default is 3000.

libfabric endpoint (``ofi``):

* ``rgw_ofi_provider``: the provider. It must match the OSDs'
  ``osd_ofi_provider``. The default is ``tcp``.
* ``rgw_ofi_domain`` and ``rgw_ofi_node``: the domain and the local
  address to bind. The address must be reachable from the OSDs. When
  they are empty, the provider chooses.

cuObject server (``cuobj``):

* ``rgw_cuobj_rdma_ip`` and ``rgw_cuobj_rdma_port``: its address and
  port. The default port is 20886.
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
  the session limits, 1024 and 64 by default. A PREPARE over a limit
  gets ``503 SlowDown``.
* ``rgw_rdma_rc_send_depth``: the writes to the client that can be in
  flight for one session, 64 by default.
* ``rgw_rdma_rc_prepare_timeout_ms`` and ``rgw_rdma_rc_exec_timeout_ms``:
  how long a session waits for READY, and how long a transfer can take.

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
  buffer for it instead, which is slower.
* ``osd_oob_op_timeout_ms``: how long the writes of one read can take,
  5000 by default. After that the read is delivered inline.
* ``osd_oob_gather``: lend windows when this OSD is the primary of an
  erasure-coded read. The default is false.
* ``osd_oob_window_size`` and ``osd_oob_window_count``: the window
  pool, 16 windows of 8 MiB by default. A shard read larger than a
  window, or a gather that finds no free window, uses inline replies.

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

Each executor reports its counters through the OSD's admin socket::

  ceph daemon osd.N cuobj status
  ceph daemon osd.N ofi status

The counters cover plans started, completed and failed, bytes written,
writes in flight, and windows lent and exhausted. The ``ofi status``
output also names the provider and the last provider error.

Accounting
----------

Bytes that move out of band appear in the beast access log, the ops log
and the usage log. They count as bytes sent for a GET and bytes
received for a PUT, although they do not cross the HTTP socket.

Testing
-------

* ``unittest_ofi_rma`` tests the libfabric token format, and writes
  over the ``tcp`` and ``shm`` providers.
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
* Every call into one libfabric endpoint is serialized, which matches
  the ``FI_THREAD_DOMAIN`` threading level that the executor asks for.
* Every libfabric endpoint of one provider must use the same address
  family. Mixed IPv4 and IPv6 endpoints are not tested.
* Shard-direct erasure-coded reads deliver sparse reads inline.
* Only cuObject clients can PUT with ``x-amz-rdma-token``, in
  gateway-staged mode. Libfabric clients have no gateway-staged mode,
  so a libfabric GET that cannot use passthrough gets the HTTP body.
* The gateway does not serve encrypted objects, or objects with a DLO
  or SLO manifest, to RC clients. PREPARE answers ``501`` with the
  unsupported marker, and the client reads the object over HTTP.
* An RC GET can name a ``versionId``. Multipart part uploads and other
  subresources are not available over RC.
* An RC PUT must arrive as one write-with-immediate that carries the
  whole object.
* An RC transfer runs on the request thread.
* The cuObject executor's gather windows are not tested on hardware.

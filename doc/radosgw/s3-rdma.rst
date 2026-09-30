.. _radosgw_s3_rdma:

=============
S3 over RDMA
=============

.. versionadded:: Umbrella

Ceph Object Gateway can serve S3 object data over RDMA using the NVIDIA
cuObject library. The S3 control plane (authentication, headers,
metadata) stays on HTTP, while object data moves out of band: it is
DMA-written straight into memory that the S3 client registered with its
RDMA NIC, including GPU memory on clients using GPUDirect Storage.

A client using the cuObject client library (``libcuobjclient``, for
example via NooBaa's ``s3perf.js --rdma`` or an AWS SDK middleware)
registers a memory window and sends its RDMA descriptor with each
request in the ``x-amz-rdma-token`` header. The descriptor is opaque:
it names the client's memory window, remote key and Dynamically
Connected (DC) target, so any server holding it (and the matching
``dc_key``) can push data into that window without a pre-established
connection.

Two data paths are available for GET:

Staged (gateway) mode
  The gateway runs its own ``cuObjServer``. Object data is read from
  RADOS as usual, accumulated in a pre-registered gateway buffer, and
  RDMA-written to the client in one transfer at the end of the request.
  This is the base mode; it requires the gateway host to have an
  RDMA-capable NIC and the ``cuobjserver`` library, and it is also used
  for PUT (the upload must flow through the gateway's checksum,
  compression and encryption filters). Build with
  ``-DWITH_RADOSGW_CUOBJ=ON`` and enable with ``rgw_cuobj_enabled``.

OSD passthrough mode
  With ``rgw_cuobj_osd_passthrough`` enabled, the gateway forwards the
  client's descriptor to the OSDs instead: each stripe read carries an
  *advisory delivery descriptor* (a per-operation field on the RADOS
  request message, alongside the read it applies to, holding the
  opaque token and the stripe's offset within the requested
  range). An OSD that can push builds an
  op-aware placement plan and RDMA-writes the reply data directly into
  the client's memory window, returning only byte counts; an OSD that
  cannot — not built with cuObject, disabled, lease expired, or a
  retransmitted request — simply returns the data inline as a normal
  read, which the gateway treats as the signal to restart the GET in a
  fallback mode. Degradation is therefore always plain, correct,
  in-band data; there is no protocol error to handle.

  Object data never touches the gateway, removing the OSD-to-gateway
  network hop and the gateway staging buffer, and letting transfer
  bandwidth scale with the number of OSDs. This matches the cuObject
  architecture's "gateway instructs data nodes" reference flow. In this
  mode the gateway itself needs neither the cuObject library nor an
  RDMA NIC; the OSDs do (build them with ``-DWITH_OSD_CUOBJ=ON`` and
  set ``osd_cuobj_enabled``).

  Because the descriptor rides alongside each read on the request
  message rather than being a special operation, any read shape can
  use it (and each read in a compound request can carry its own):
  plain reads take a
  linear placement, sparse reads scatter per extent (the extent map
  stays inline), erasure-coded *primary* reads work unchanged (the
  reply is reconstructed logical data), and erasure-coded *direct*
  reads — the client split-read path — have each shard OSD scatter its
  ~16K chunks to their logical positions in the client window, so the
  shards' concurrent writes interleave into the client's buffer and
  client-side reassembly disappears.

Fallback behavior
=================

Modes degrade transparently, per request:

#. **Passthrough** is attempted when the token is present,
   ``rgw_cuobj_osd_passthrough`` is enabled and the request is
   eligible (see below). If any stripe comes back inline — an OSD
   that predates the feature, was built without it, has it disabled,
   refused an expired lease or a retransmitted op — the gateway
   restarts the whole GET in the next mode down. The restart is
   invisible to the client: no HTTP bytes have been sent, and
   rewriting any client memory ranges that were already delivered is
   harmless. When stripe operations already reached the OSDs, the
   gateway first waits out the pool's ``rdma_delivery_lease`` plus
   ``rgw_cuobj_fence_drain_ms`` so that a write an OSD we lost track
   of may still start, or one still queued in its NIC, cannot land
   after the fallback rewrites the same ranges (see Deployment notes
   below).
#. **Staged** mode is used when the gateway has a working
   ``cuObjServer``.
#. Otherwise the response carries the data in the **HTTP body** with
   ``x-amz-rdma-reply: 501``, which the cuObject protocol defines as
   the "fall back to HTTP" signal.

Passthrough eligibility
=======================

A GET uses passthrough only when the gateway would not need to touch
the data:

* the object is not compressed and not encrypted (no server-side
  transform may run);
* no Lua data script or Arrow Flight filter is attached;
* the object is not a Swift DLO/SLO user manifest;
* D3N datacache is not enabled; and
* the requested range fits within the client's registered window.

Replicated and erasure-coded pools are both supported; see
`Erasure-coded pools`_ for the two read paths an EC pool can serve a
passthrough GET by, and what each one requires.

Multipart objects and range requests are fully supported; every stripe
lands at its logical offset within the requested range.

Erasure-coded pools
===================

An EC pool serves a passthrough GET by one of two read paths. Which
one runs is a property of the pool and of the gateway's read policy,
not of this feature.

Primary reads
  The default. The primary reconstructs the logical data and replies
  with it, so the OSD builds a linear placement and writes one
  contiguous range per stripe. Nothing needs enabling: a plain EC pool
  works with no client changes.

Shard-direct reads
  Each shard OSD instead scatters the chunks it holds to their logical
  positions in the client's window, so the shards' concurrent writes
  interleave and client-side reassembly disappears. Two settings are
  needed, neither of them on by default:

  * ``allow_ec_optimizations`` on the pool, which is what sets the
    pool's ``split_reads`` flag. Replicated pools carry ``split_reads``
    unconditionally, so it appears there without any opt-in.
  * ``rados_replica_read_policy = balance`` on the gateway. A read that
    does not carry the balanced-read flag is never split, so the pool
    flag alone is not enough — it grants permission, while the client
    still decides per request.

  With only the first of the two, reads continue to go to the primary
  and the linear placement runs.

Shard-direct reads skip the CRC64-NVME verification described under
`Integrity`_, because interleaved layouts do not concatenation-combine.
Turning them on therefore trades end-to-end checksum verification for
the removal of the reconstruct-and-reassemble step; consider whether
that is the right trade for a given pool.

The interleave itself is only exercised when a gateway stripe spans
several EC stripes. Where ``rgw_obj_stripe_size`` equals the pool's
``stripe_width``, each shard holds one contiguous range of the request
and the plan collapses to a single write, which is indistinguishable
from a primary read.

Configuration
=============

Gateway (staged mode and protocol handling):

* ``rgw_cuobj_enabled``, ``rgw_cuobj_rdma_ip``, ``rgw_cuobj_rdma_port``,
  ``rgw_cuobj_buffer_size``, ``rgw_cuobj_buffer_count``,
  ``rgw_cuobj_num_dcis`` — the staged-mode ``cuObjServer``.
* ``rgw_cuobj_osd_passthrough`` — enable OSD-direct delivery for GET.
* ``rgw_cuobj_crc64nvme`` — ask the OSDs to CRC64-NVME each stripe as
  it is RDMA-written; the gateway combines the per-stripe values in
  logical order and, for whole-object GETs, verifies the result
  against the object's stored full-object ``crc64nvme`` checksum
  before responding. This is end-to-end integrity across client
  memory, the fabric and the storage node — corruption anywhere on
  that path fails the GET instead of reaching the application. On by
  default; per-stripe checksums are computed with carry-less-multiply
  accelerated tables. A stripe served by shard-direct EC reads is
  checksummed one chunk at a time by each shard OSD and folded in
  logical order on the client, so interleaved placements verify the
  same way contiguous ones do.
* ``rgw_cuobj_fence_drain_ms`` — transport drain bound added to the
  pool's ``rdma_delivery_lease`` when a passthrough attempt that
  already reached the OSDs restarts in a fallback mode; size it to
  cover the RDMA transport's retry budget (roughly two seconds at the
  cuObject defaults).

Pool (enforced by the OSDs, read by the gateway from the OSDMap):

* ``rdma_delivery_lease`` — how long, in seconds, after receiving a
  stripe operation an OSD may still *initiate* a transfer against its
  delivery descriptor; one that would start later is delivered inline
  instead. Set it with ``ceph osd pool set <pool>
  rdma_delivery_lease <seconds>``; the default is 5. The gateway sizes
  its fence from the same OSDMap value the OSDs enforce, so there is
  no per-daemon setting to keep in step. The lease bounds the OSD
  side; it is what makes an abandoned window quiescent on this path,
  not a general bound on how long a client holds a window
  registered.

OSD (passthrough execution):

* ``osd_cuobj_enabled`` — instantiate the OSD's cuObject endpoint.
* ``osd_cuobj_rdma_ip`` — RDMA interface address; defaults to the
  OSD's public address. **Must** be set explicitly when the RDMA NIC
  is not the public-network interface.
* ``osd_cuobj_rdma_port`` — local ``rdma_cm`` binding; ``0`` (the
  default) lets the library choose. Clients never connect to this
  port.
* ``osd_cuobj_buffer_size`` / ``osd_cuobj_buffer_count`` — the
  pre-registered staging pool. The buffer size must cover the largest
  stripe read (``rgw_get_obj_max_req_size``, default 4 MiB); requests
  that cannot be served from the pool fall back to slower one-shot
  registrations.
* ``osd_cuobj_num_dcis`` — DC initiators; must be at least the number
  of OSD op worker threads.
* ``osd_cuobj_dc_key`` — must match the cuObject client library's DC
  key cluster-wide (default ``0xffeeddcc``, the library default).

Host prerequisites
==================

Three host settings are easy to miss, and each one fails with an error
that does not name the real cause.

``rdma_ucm`` must be loaded
  ``cuObjServer`` connects through ``rdma_cm``, so
  ``/dev/infiniband/rdma_cm`` has to exist::

    modprobe rdma_ucm

  Without it the OSD logs ``cuObjServer RDMA session failed to start``
  and disables ``READ_RDMA``; the server object still constructs, and
  only ``isConnected()`` reports the failure. A passing ``ib_send_bw``
  run does *not* establish that this is in place — perftest defaults to
  ``rdma_cm QPs : OFF`` and exercises raw verbs only, so the fabric can
  benchmark at line rate while cuObject cannot start a session at all.

Locked memory must be raised
  Every OSD registers ``osd_cuobj_buffer_count`` times
  ``osd_cuobj_buffer_size`` of RDMA memory — 256 MiB at the defaults —
  which is far above the customary 8 MiB ``memlock`` ceiling. Give the
  OSDs (and the gateway, in staged mode) ``LimitMEMLOCK=infinity``, or
  ``ulimit -l unlimited`` for a vstart cluster.

The RDMA address must belong to the RDMA device
  ``osd_cuobj_rdma_ip`` has to name an address the RDMA device actually
  carries. Where the ConnectX ports are bonded and tenant traffic is
  VLAN-tagged, that is the address on the VLAN above the bond, which is
  typically not the public address. ``ibv_devinfo`` and the GID table
  under ``/sys/class/infiniband/<device>/ports/1/gids`` show which
  addresses the device carries; a RoCE v2 entry whose GID ends in the
  IPv4-mapped form of the address confirms the pairing.

Clients that read into host memory
----------------------------------

Such a client needs no GPU and no NVIDIA kernel driver, but
``libcufile`` only reaches that configuration with DMABuf enabled.
Otherwise it logs ``nvidia_peermem.ko is not loaded. Disabling
UserSpace RDMA access.``, registers no RDMA devices, and
``cuMemObjGetDescriptor`` fails::

    export CUFILE_DMABUF_ENABLE=true

The client's own RoCE address must also be listed in
``rdma_dev_addr_list`` in ``cufile.json``, which is otherwise empty
(``CUFILE_ENV_PATH_JSON`` selects an alternate copy)::

    "rdma_dev_addr_list": [ "10.0.9.7" ],

Leave ``rdma_transport_type`` at ``DC_V1``, and keep ``rdma_dc_key``
equal to ``osd_cuobj_dc_key`` on the OSDs; the defaults on both sides
already agree.

Deployment notes
================

* OSD nodes need a ConnectX-5 or newer (or RoCE-capable) NIC,
  ``rdma-core``, and the proprietary ``cuobjserver`` library from
  NVIDIA. No GPU is needed on OSD, gateway or client hosts; only
  GPU-memory targets on the client require CUDA. See `Host
  prerequisites`_ for the kernel module, locked-memory and client
  library settings this depends on.
* The in-flight window per GET is bounded by
  ``rgw_get_obj_window_size`` (default 16 MiB), which throttles how
  much RDMA traffic the OSDs aim at one client NIC at a time.
* The descriptor grants write access to the client's registered
  window until the client deregisters it. Three mechanisms keep stale
  writes out of a reused buffer (the lease/interlock scheme): the
  gateway drains outstanding stripe operations before any response —
  and because the OSD-side push completes before the op reply is
  sent, a drained reply *is* the interlock for every OSD still in
  contact; OSDs deliver retransmitted requests inline (RADOS re-sends
  reads after peering changes) so a stripe is never double-pushed;
  and for OSDs that vanish mid-request, or whose original copy of an
  operation the gateway's RADOS client has since resent, the pool's
  ``rdma_delivery_lease`` bounds how long after receipt a write may
  still start, so the gateway's fence (lease plus
  ``rgw_cuobj_fence_drain_ms``, applied before any fallback rewrite)
  outlasts lease-plus-transport-drain and the window is quiescent
  before it is written again. An OSD also re-checks its PG read lease
  (``readable_until``) immediately before starting a push, since the
  readability check at dispatch does not cover a read that stalled
  afterward: a primary that has lost contact with its peers delivers
  inline rather than write into a window the new acting set may
  already be serving. The delivery lease is measured against the
  wall clock, so it is best-effort fencing across clock steps —
  size it with slack rather than treating it as a hard barrier. The
  ``cuobj status`` OSD admin-socket command exposes plan and
  in-flight-write counters for observing the interlock.
* Planned follow-ups: moving the OSD-side push off the op worker
  thread (submissions are already batched asynchronously, but the
  reply path still waits for the batch); shard-space sparse reads on
  erasure-coded direct reads (currently delivered inline); and
  eliminating the OSD's staging copy by registering the BlueStore
  hugepage read-buffer pool with the RDMA NIC.

Integrity
=========

Because the gateway never touches passthrough data, verification moves
to where the data actually is: each OSD checksums (CRC64-NVME) the
exact bytes it pushed, after they crossed the fabric, and the gateway
folds the per-stripe values with the same combining math S3 uses for
multipart full-object checksums. Whole-object GETs of objects that
carry a stored full-object ``crc64nvme`` checksum (the AWS
``x-amz-checksum-crc64nvme`` type) are verified before any response
bytes are committed. Erasure-coded *direct* (split) reads and sparse
reads currently omit per-stripe checksums — their interleaved layouts
do not concatenation-combine — and simply skip verification.

Accounting
==========

Bytes moved over RDMA appear in the beast access log, the ops log and
the usage log (attributed to bytes sent for GET, bytes received for
PUT), even though they do not traverse the HTTP socket.

Reliable Connection clients (hipObject)
=======================================

The gateway also serves clients that use Reliable Connection (RC)
transport instead of cuObject's Dynamically Connected (DC) transport.
DC exists only on NVIDIA ConnectX NICs. RC exists on every RDMA NIC,
including Broadcom Thor-2 and AMD Pensando. AMD's hipObject client
library uses RC, and its control protocol is called ``hipobj-rc-v2``.

An RC transfer needs a queue pair on each side, paired before any data
moves. A queue pair is the RDMA endpoint that carries one connection.
The client and the gateway exchange the pairing parameters over three
SigV4-signed HTTP requests, and the object data then moves over the
paired queue pair:

``POST /.hipobj-rc/prepare``
  The client sends its RC token (queue pair number and GID), a packet
  sequence number (PSN), a cookie, the operation, the object and the
  byte range. The gateway authorizes the request as the S3 operation
  it stands for, creates a session, and answers with the session ID
  and its own queue pair number and PSN.

``POST /.hipobj-rc/ready``
  The client sends its queue pair number and the address and remote
  key of its memory region. The gateway pairs its queue pair with the
  client's queue pair, runs the transfer, and answers when the
  transfer is complete. The answer carries the byte count, the ETag,
  the version ID and a CRC-64/NVME of the delivered bytes.

``POST /.hipobj-rc/cancel``
  The client ends a session that it no longer needs.

Every ``x-amz-rdma-*`` request header must be in the SigV4
``SignedHeaders`` list. The gateway refuses a request with an unsigned
protocol header, because anything on the network path could change an
unsigned header without breaking the signature. Anonymous requests are
refused. A session belongs to the user that prepared it. READY and
CANCEL from any other user fail as if the session did not exist.

GET relay
---------

For a GET, the gateway holds the session's buffer between the OSDs and
the client. There are two ways for the object to reach that buffer:

OSD-direct relay
  The gateway exposes its session buffers as a cuObject DC target.
  Each stripe read then carries a delivery descriptor for the session
  buffer, and the OSDs RDMA-write their stripes into it, exactly as
  in OSD passthrough mode. The gateway does not copy the object. This
  needs an mlx5 NIC on the gateway, ``rgw_cuobj_osd_passthrough``, and
  OSDs built with ``WITH_OSD_CUOBJ``.

Staged relay
  The object is read from RADOS as usual and copied into the session
  buffer. The gateway uses this path when OSD-direct relay is not
  available, and for compressed objects. It is also the fallback when
  an OSD returns a stripe inline instead of writing it.

In both cases the gateway pushes the bytes to the client while the read
is still in progress. Each time the start of the buffer is complete up
to a new point, the gateway RDMA-writes the new bytes to the client.
The last write carries the session cookie as its immediate value. RC
delivers writes in order, so the client's completion for the last
write means that all earlier writes are in its memory.

When a relay fails after the OSDs received delivery descriptors, an
OSD can still write into the session buffer until the pool's
``rdma_delivery_lease`` expires. The gateway keeps that buffer out of
use for the lease plus ``rgw_cuobj_fence_drain_ms``.

PUT
---

For a PUT, the gateway registers a staging buffer at PREPARE and gives
its address to the client. The client writes the whole object with one
RDMA write-with-immediate. The gateway then stores the buffer through
the normal PUT path. Bucket default encryption, compression,
notifications and object lock apply as they do to any PUT.

Configuration
-------------

Build with ``-DWITH_RADOSGW_RDMA_RC=ON``. This is the default when
``WITH_RDMA`` is on. OSD-direct relay also needs the ``mlx5`` direct
verbs library at build time.

* ``rgw_rdma_rc_enabled``: serve RC sessions. When it is off, the
  control routes answer ``501`` with
  ``x-amz-rdma-protocol-status: unsupported``. That answer tells a
  hipObject client to use plain HTTP.
* ``rgw_rdma_rc_device``, ``rgw_rdma_rc_gid_hint``,
  ``rgw_rdma_rc_port``, ``rgw_rdma_rc_gid_index``: select the verbs
  device, port and GID.
* ``rgw_rdma_rc_buffer_size``, ``rgw_rdma_rc_buffer_count``: the
  registered session buffers. Each session holds one buffer from
  PREPARE until the session ends. A transfer larger than one buffer is
  refused with ``413``.
* ``rgw_rdma_rc_max_sessions``, ``rgw_rdma_rc_max_sessions_per_user``:
  session limits. A PREPARE over a limit gets ``503 SlowDown``.
* ``rgw_rdma_rc_send_depth``: how many writes to the client can be in
  flight for one session.
* ``rgw_rdma_rc_prepare_timeout_ms``, ``rgw_rdma_rc_exec_timeout_ms``:
  how long a session waits for READY, and how long a transfer can take.
* ``rgw_rdma_rc_osd_direct``, ``rgw_rdma_rc_dc_key``: the DC target
  for OSD-direct relay. The key must match ``osd_cuobj_dc_key``.
* ``rgw_rdma_rc_crc64nvme``: report a CRC-64/NVME of the delivered
  bytes in the READY answer.

Limitations
-----------

* The gateway does not serve encrypted objects, or objects with a DLO
  or SLO manifest, over RC. PREPARE answers ``501`` with the
  unsupported marker, and the client reads the object over HTTP.
* A target can name a ``versionId`` for a GET. Multipart part uploads
  and other subresources are not available over RC.
* A PUT must arrive as one write-with-immediate that carries the whole
  object.
* The transfer runs on the request thread, as in staged mode.

UET delivery (mock-up)
======================

This branch carries a mock-up of OSD-direct delivery over Ultra Ethernet
Transport (UET). It shows that OSDs can write GET data straight into a
client's memory without a connection per OSD, as cuObject's DC transport
does, but on any Ethernet NIC.

The client registers a memory window with a UET provider and sends an
ordinary S3 GET with this token in ``x-amz-rdma-token``::

  <base hex>:<window size hex>:uet1:<client IPv4>:<memory key hex>

The token starts with the same ``addr:size`` fields as a cuObject token,
so the gateway treats it the same way. With
``rgw_cuobj_osd_passthrough`` on, the gateway forwards it to the OSDs in
each stripe read's delivery descriptor. An OSD with UET delivery enabled
writes its stripe into the window with RMA writes in UET's RUDI mode.
RUDI is reliable, unordered and connectionless, and it is meant for
idempotent operations. The client keeps no state for the OSDs. It does
not add their addresses and does no handshake. The OSD replies to the
gateway after its writes complete, and the gateway answers the GET after
every stripe's reply is in.

OSDs choose an executor by the token's shape. A token with the ``uet1``
tag goes to the UET executor, and any other token goes to cuObject. A
token that no running executor serves is delivered inline, as before.

The mock-up uses the UEC reference provider
(https://github.com/ultraethernet/uet-ref-prov), a software UET stack
over raw Ethernet sockets. Build the provider, then build Ceph with
``-DWITH_OSD_UET=ON -DUET_REF_PROV_DIR=<provider checkout>``. Each OSD
needs:

* ``osd_uet_enabled`` set to true.
* ``osd_uet_ifname`` set to the interface it sends and receives on. The
  interface's IPv4 address is the OSD's fabric endpoint. The value can
  use ``$id``, for example ``uet-o$id``.
* The ``CAP_NET_RAW`` capability, for the raw socket. An OSD normally
  drops every capability its block-device plugins do not need. With
  ``osd_uet_enabled`` set, it keeps ``CAP_NET_RAW`` as well.

``ceph_test_rgw_uet_get`` is a client for the mock-up. It registers a
window, sends a signed GET with the token, and verifies the window
against a local copy of the object. ``ceph daemon osd.N uet status``
shows each OSD's delivery counters.

The mock-up has these limits:

* The provider is software. A target places incoming data only while
  its application polls, so the client polls during the GET.
* The provider keeps process-wide state. Each OSD has one UET endpoint,
  and the OSD serializes every call into it.
* Writes are sent unencrypted. The provider's security sublayer works in
  server mode, but the mock-up does not configure it.
* The OSD writes on the op thread and waits for completion, as the first
  cuObject executor did.

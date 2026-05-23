.. _msgr2-psp:

msgr2 PSP Security Protocol mode (secure-psp)
=============================================

``secure-psp`` is a third msgr2 connection mode alongside ``crc`` and
``secure``. It is intended to carry msgr2 frames over a TCP stream
that the kernel and a PSP-capable NIC inline-encrypt at the network
layer using Google's PSP Security Protocol, eliminating the
userspace AEAD copy that ``secure`` (AES-GCM via ``crypto_onwire``)
incurs on every send.

The first landing of ``secure-psp`` is the wire-protocol surface: a
new mode value, ``ms_*_mode`` parser support, telemetry, and the
backend-abstraction scaffolding for the netlink interface. The data
path for ``secure-psp`` remains the same in-process AEAD that
``secure-aesgcm`` uses until the kernel-offload path lands. From an
operator's perspective the wire mode is selectable but the
DRAM-bandwidth saving from PSP is not yet realized.

Background: what PSP is
-----------------------

PSP is a packet-level AEAD designed to replace IPsec for
intra-datacenter use, with a stateless receive side that scales to
large meshes. Linux kernel v6.18 added PSP core support with an
mlx5 driver implementation; ConnectX-7-and-above is the supported
NIC family at that revision. The protocol is documented in
``Documentation/networking/psp.rst`` in the Linux source tree.

PSP is invisible at the messenger layer: the kernel and NIC wrap
TCP packets in UDP+PSP framing on transmit and strip it on receive,
so the messenger reads and writes plain TCP throughout. msgr2 frame
format is unchanged for ``secure-psp`` connections.

Configuration
-------------

``secure-psp`` is a value for the existing ``ms_*_mode`` knobs
(``ms_cluster_mode``, ``ms_service_mode``, ``ms_client_mode``,
``ms_mon_cluster_mode``, ``ms_mon_service_mode``,
``ms_mon_client_mode``). It is strictly opt-in: default mode lists
are unchanged. Operators enable it by including it in the mode
list, e.g.::

  ms_cluster_mode = secure-psp secure crc

The msgr2 mode-list semantics pick the highest mutual mode. A peer
that does not understand ``secure-psp`` (older Ceph, or a daemon
without the local PSP setup) will simply not offer it; the
negotiation falls back to whatever else is mutual (typically
``secure``). This is the natural backward-compatibility property.

Wire format
-----------

The on-wire msgr2 connection-mode value is ``0x3`` for
``secure-psp`` (``0x1`` and ``0x2`` for ``crc`` and ``secure``
respectively, unchanged). Older peers that do not know ``0x3`` will
not advertise it as supported and the mode-list intersection drops
it before connection establishment, so there is no negotiation
failure surface for mixed-version clusters.

msgr2 frame format inside a ``secure-psp`` connection is unchanged:
the underlying TCP byte stream carries plain msgr2 frames; the
kernel/NIC adds the UDP+PSP envelope per packet as the bytes leave
the host. When the userspace AEAD path is in effect (the data-path
is currently in-process aesgcm), the frame payload is additionally
encrypted by ``crypto_onwire``; when the kernel-offload path lands,
that in-process AEAD is skipped for ``secure-psp`` connections.

Mode negotiation
----------------

``secure-psp`` participates in the existing AuthRegistry mode
negotiation (``AuthRegistry::pick_mode``,
``AuthRegistry::get_supported_modes``). ``AuthRegistry::is_secure_mode``
treats both ``CEPH_CON_MODE_SECURE`` and ``CEPH_CON_MODE_SECURE_PSP``
as secure modes.

``AuthConnectionMeta`` exposes two predicates:

``is_mode_secure()``
  True for both ``CEPH_CON_MODE_SECURE`` and
  ``CEPH_CON_MODE_SECURE_PSP``. Existing code that asked "is this
  connection running an in-process AEAD" sees both modes
  identically; this is correct while ``secure-psp`` still uses the
  in-process AEAD data path.

``is_mode_secure_psp()``
  True only for ``CEPH_CON_MODE_SECURE_PSP``. Used by telemetry and
  by the future kernel-offload branch to distinguish PSP-protected
  connections from secure-aesgcm.

When the kernel-offload data path lands, ``is_mode_secure()`` will
narrow to "in-process AEAD only" (i.e. only secure-aesgcm), and a
separate predicate will gate the in-process AEAD calls in
ProtocolV2 / crypto_onwire.

Kernel interface (forward-looking)
----------------------------------

PSP key management is split between a per-NIC master device key
(rotated by management daemons via the kernel PSP genetlink family;
not under application control) and per-connection rx/tx
associations bound to individual sockets via ``rx-assoc`` and
``tx-assoc`` netlink commands.

The kernel documentation is explicit that there is no standard
mechanism for two peers to exchange the per-connection PSP key
material; that exchange is the application's responsibility. The
in-tree implementation provides a NetlinkBackend abstraction in
``src/msg/async/PSPNetlink.h`` that wraps the per-socket
``rx-assoc`` / ``tx-assoc`` calls and a per-NIC capability query.
The default (stub) backend reports unsupported on every call; a
deterministic mock backend exists for CI tests; the real
libmnl/libnl-genl backend lands together with the in-band key
exchange and the data-path branch that bypasses the in-process AEAD.

Counter surface
---------------

``msgr_psp_connections`` (u64 counter)
  Number of msgr2 connections whose negotiated mode is
  ``secure-psp``. Increments in ``ProtocolV2::ready`` when the
  connection becomes live. Remains a valid measure of PSP usage
  after the kernel-offload path engages.

Additional counters land with the kernel-offload path:

``msgr_psp_attach_failed``
  Netlink rx/tx-assoc rejections (NIC context-table full, firmware
  rejection, etc.) that force the connection to tear down and
  reconnect at a lower mode.

``msgr_psp_fallback_to_aesgcm``
  Connections that advertised ``secure-psp`` but ultimately ran
  ``secure-aesgcm`` because the attach failed and the next mode in
  the list was secure-aesgcm.

``msgr_psp_active_assocs``
  Currently-installed PSP tx-assoc slots on this Worker; a gauge
  for monitoring NIC context-table consumption.

Limitations
-----------

* Hardware/kernel scope: PSP HW offload is currently in mlx5
  ConnectX-7-and-above on Linux v6.18+. Clusters on other NICs or
  earlier kernels keep ``secure-aesgcm`` as the only secure-mode
  option.
* No DRAM-bandwidth benefit until the kernel-offload data path
  lands: at this revision the wire mode is selectable but the
  in-process AEAD still runs, so ``secure-psp`` has the same memory
  bandwidth profile as ``secure-aesgcm``.
* PSP wraps packets in UDP+PSP; intermediate middleboxes that do
  not permit UDP encapsulation on the relevant ports will block
  PSP-protected traffic. Datacenter networks under operator
  control are the intended deployment.
* ``secure-psp`` is excluded from MSG_ZEROCOPY today (the same
  exclusion that already applies to ``secure``); when the
  kernel-offload data path lands, only the in-process-AEAD mode
  will be excluded.

References
----------

* Linux kernel PSP documentation: ``Documentation/networking/psp.rst``
* :ref:`msgr2-protocol` — msgr2 wire and mode negotiation
* :ref:`msgr2-zerocopy` — the substrate this layers on for the
  plaintext data path
* ``src/include/ceph_fs.h`` — ``CEPH_CON_MODE_SECURE_PSP``
* ``src/auth/Auth.h`` — ``AuthConnectionMeta::is_mode_secure*``
* ``src/auth/AuthRegistry.cc`` — ``_parse_mode_list``
* ``src/msg/async/PSPNetlink.h`` — netlink backend abstraction
* ``src/msg/async/ProtocolV2.cc`` — mode-aware connection setup

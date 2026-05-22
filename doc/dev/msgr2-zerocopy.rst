.. _msgr2-zerocopy:

msgr2 zero-copy send (Linux MSG_ZEROCOPY)
=========================================

The Posix messenger stack can opt each TCP socket into the Linux
``MSG_ZEROCOPY`` send path, eliminating the kernel-side payload copy
that ``sendmsg(2)`` would otherwise do from the user buffer into the
socket buffer. The benefit is realized only over a real NIC: on
loopback / veth the kernel always copies even with ``MSG_ZEROCOPY``,
and posts ``SO_EE_CODE_ZEROCOPY_COPIED`` on the completion to
indicate so.

This is purely a send-side optimization for the plaintext data path.
Secure-mode connections are deliberately excluded: the per-frame
AEAD already copies the payload during encryption (in
``crypto_onwire.cc``), so there is no copy left to elide and pinning
the buffer would only add lifetime risk.

Configuration
-------------

Two options control the behavior, both opt-in and default off:

``ms_tcp_zerocopy`` (bool, default ``false``)
  Enable ``SO_ZEROCOPY`` on TCP sockets at connect/accept and pass
  ``MSG_ZEROCOPY`` to qualifying sends. Only sockets established
  *after* the option is set are affected; existing connections
  continue on the plain copying path.

``ms_tcp_zerocopy_min_size`` (size, default ``16K``)
  Minimum per-``sendmsg`` size below which the send goes through the
  plain copying path. ``MSG_ZEROCOPY`` pins user pages and incurs a
  per-send completion notification; below the threshold the copy is
  cheaper than the pinning + notification overhead. Read once at
  socket creation; runtime changes affect only new connections.

Wire behavior
-------------

The wire format is unchanged. ``MSG_ZEROCOPY`` is a kernel-side
transport optimization that the receiving peer cannot detect or
distinguish from a normal send. Mixed-mode clusters interoperate
freely.

Implementation overview
-----------------------

The opt-in lives in ``NetHandler::set_socket_options``: when
``ms_tcp_zerocopy`` is enabled it ``setsockopt(SO_ZEROCOPY, 1)`` at
the three caller sites (accept, connect, server-bind). The result is
stored on ``PosixConnectedSocketImpl`` so the per-send code path can
fast-path on it. Failure of the setsockopt is non-fatal: the
connection falls back to the plain copying mode.

Per-send decision is made in ``PosixConnectedSocketImpl::send``:
each ``do_sendmsg`` is sent with ``MSG_ZEROCOPY`` iff the socket has
``SO_ZEROCOPY`` enabled, the per-chunk byte length is at or above
``ms_tcp_zerocopy_min_size``, and the connection is not in secure
mode (the secure-mode exclusion is decided in ``ProtocolV2::ready``
via ``set_zerocopy_eligible(false)``).

Buffer-pin lifetime
~~~~~~~~~~~~~~~~~~~

``MSG_ZEROCOPY`` does not retain a copy of the user payload; the
kernel/NIC reads from the user pages directly and only releases
those pages when it posts a completion on ``MSG_ERRQUEUE``. Until
the completion arrives, the buffer must not be freed, mutated, or
recycled.

The implementation handles this by maintaining a per-connection
FIFO of ``ZeroCopyPin`` records on ``PosixConnectedSocketImpl``,
each holding a refcounted ``bufferlist`` (the sent prefix of
``outgoing_bl``) plus the highest kernel completion ID it consumed.
Each ``do_sendmsg(MSG_ZEROCOPY)`` allocates one new kernel ID; the
pin's ``last_id`` records the highest ID for that send. The deque
is single-threaded (worker thread only) so no locking is needed.

CRC-cache hazard
~~~~~~~~~~~~~~~~

The per-``raw`` CRC cache (``buffer::raw::last_crc_offset`` /
``last_crc_val``) caches per-buffer CRC values. If an external
pointer wrote into a still-pending send buffer, the messenger would
recompute the wire CRC, hit a stale cache, and ship a CRC that no
longer matches the bytes on the wire (the receiver would reject the
frame as an opaque connection reset rather than a clean integrity
error). The refcounted-bufferlist pin in ``ZeroCopyPin`` is the
structural guard against this class of bug: while a send is
pending, no callers can free or rebuild the underlying ``raw``.

Completion drain
~~~~~~~~~~~~~~~~

Kernel completions arrive on the existing socket fd as
``MSG_ERRQUEUE`` notifications surfaced through ``EPOLLERR``.
``PosixConnectedSocketImpl::drain_zerocopy_completions`` does a
``recvmsg(MSG_ERRQUEUE)`` loop, parses ``sock_extended_err`` cmsgs
with ``ee_origin == SO_EE_ORIGIN_ZEROCOPY``, advances the
``done_max`` watermark using wrap-safe monotone u32 comparison, and
retires pins from the front of the FIFO whose ``last_id`` is below
the watermark. The drain is called opportunistically from
``AsyncConnection::_try_send`` and exhaustively from
``PosixConnectedSocketImpl::close`` / ``shutdown_socket``; the
``EPOLLERR``-driven event from ``EventEpoll`` is routed to the
existing read handler so no new fd is needed.

``SO_EE_CODE_ZEROCOPY_COPIED`` on a completion indicates the kernel
fell back to a copy for that send (commonly: loopback, NIC without
TX zerocopy support, or transient resource pressure). The pin is
still retired identically; the ``msgr_zerocopy_fallback`` counter
increments so the fraction can be monitored.

``ENOBUFS`` from ``sendmsg(MSG_ZEROCOPY)`` indicates the
``net.core.optmem_max`` budget is exhausted; the send retries
without ``MSG_ZEROCOPY`` (transparent fallback) and is accounted as
a normal copying send.

Counter surface
---------------

The ``Worker`` perfcounter logger gains the following ``l_msgr_*``
counters:

``msgr_send_bytes_copied`` (u64 counter, bytes)
  Bytes that went through the kernel send copy.

``msgr_send_bytes_zerocopy`` (u64 counter, bytes)
  Bytes sent via ``MSG_ZEROCOPY``.

``msgr_send_iov_segments`` (u64 counter histogram)
  Per-``sendmsg`` iovec segment count vs bytes. Useful for
  characterizing scatter/gather shape.

``msgr_zerocopy_submitted`` (u64 counter)
  Number of zero-copy sends submitted (one per pin, the lifecycle
  unit; not per kernel sendmsg, which may issue multiple ids per
  send on partial completion).

``msgr_zerocopy_completed`` (u64 counter)
  Completions drained from the error queue.

``msgr_zerocopy_fallback`` (u64 counter)
  Sends where the kernel set ``SO_EE_CODE_ZEROCOPY_COPIED`` and
  copied anyway.

``msgr_zerocopy_pinned_bytes`` (u64 gauge, bytes)
  Bytes currently pinned awaiting completion. Must reach zero at
  teardown if the lifetime accounting is correct.

The natural lifetime invariant is
``msgr_zerocopy_submitted == msgr_zerocopy_completed`` and
``msgr_zerocopy_pinned_bytes == 0`` at connection teardown. The
socket forces this on every close path (including those that bypass
``AsyncConnection::shutdown_socket``) by crediting any residual pins
as completed and zeroing the pinned gauge before ``close(2)``.

Non-Linux behavior
------------------

``MSG_ZEROCOPY`` and ``SO_ZEROCOPY`` are Linux-only (>= 4.14). The
implementation is wrapped in ``#ifdef SO_ZEROCOPY`` throughout; on
Windows and macOS the code compiles to the plain copying path and
all the ``zerocopy_*`` counters remain at zero.

Limitations
-----------

* The benefit is unmeasurable on loopback / veth: the kernel always
  copies and posts ``SO_EE_CODE_ZEROCOPY_COPIED``. A real NIC is
  required to actually elide the copy.
* Secure mode (``ms_*_mode = secure``) is excluded; the userspace
  AEAD already touches every byte and there is no copy left to
  elide.
* CRC mode is the only mode that benefits.
* ``MSG_ZEROCOPY`` notification overhead at small frame sizes can
  exceed the saving; ``ms_tcp_zerocopy_min_size`` keeps small
  control frames on the copying path.

References
----------

* Kernel documentation: ``Documentation/networking/msg_zerocopy.rst``
  in the Linux source tree.
* ``src/msg/async/PosixStack.cc`` — ``PosixConnectedSocketImpl``
  send path, pin FIFO, errqueue drain.
* ``src/msg/async/AsyncConnection.cc`` — send orchestration,
  opportunistic drain, copy/zero-copy byte accounting.
* ``src/msg/async/Stack.h`` — perfcounter enum and registration.
* ``src/msg/async/net_handler.cc`` — ``SO_ZEROCOPY`` setsockopt
  opt-in.
* ``src/msg/async/ProtocolV2.cc`` — secure-mode exclusion.

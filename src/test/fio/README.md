FIO
===

Ceph uses the fio workload generator and benchmarking utility.
(https://github.com/axboe/fio.git)

FIO tool is automatically fetched to build/src/fio, and build if necessary.

RBD
---

The fio engine for rbd is located in the fio tree itself, so you'll need to
build it from source.

If you install the ceph libraries to a location that isn't in your
LD_LIBRARY_PATH, be sure to add it:

    export LD_LIBRARY_PATH=/path/to/install/lib

To build fio with rbd:

    ./configure --extra-cflags="-I/path/to/install/include -L/path/to/install/lib"
    make

If configure fails with "Rados Block Device engine   no", see config.log for
details and adjust the cflags as necessary.

If ceph was compiled with tcmalloc, it may be necessary to compile fio with:
    make EXTLIBS=tcmalloc
Otherwise fio might crash in malloc_usable_size().

To view the fio options specific to the rbd engine:

    ./fio --enghelp=rbd

See examples/rbd.fio for an example job file. To run:

    ./fio examples/rbd.fio

ObjectStore
-----------

This fio engine allows you to mount and use a ceph object store directly,
without having to build a ceph cluster or start any daemons.

Because the ObjectStore is not a public-facing interface, we build it inside
of the ceph tree and load libfio_ceph_objectstore.so into fio as an external
engine.

To build fio_ceph_objectstore run:
```
  ./do_cmake.sh -DWITH_FIO=ON
  cd build
  make fio_ceph_objectstore
```
This will fetch FIO to build/src/fio directory,
compile fio tool and libfio_ceph_objectstore.so.

If you install the ceph libraries to a location that isn't in your
LD_LIBRARY_PATH, be sure to add it:

    export LD_LIBRARY_PATH=/path/to/install/lib

To view the fio options specific to the objectstore engine:

    ./fio --enghelp=libfio_ceph_objectstore.so

The conf= option requires a ceph configuration file (ceph.conf). Example job
and conf files for each object store are provided in the same directory as
this README.

To run:

    ./fio /path/to/job.fio

RADOS
-----

By default FIO can be compiled with support for RADOS.
When ceph is installed in your system default compilation of FIO includes RADOS ioengine.
If you installed ceph in any other place (cmake -DCMAKE_INSTALL_PREFIX=${CEPH_INSTALL_ROOT} ..) you can build FIO following way:

    LIBS="-lrados -ltcmalloc" LDFLAGS="-L${CEPH_INSTALL_ROOT}/lib" EXTFLAGS="-I${CEPH_INSTALL_ROOT}/include" \
    rados=yes ./configure
    LIBS="-lrados -ltcmalloc" LDFLAGS="-L${CEPH_INSTALL_ROOT}/lib" EXTFLAGS="-I${CEPH_INSTALL_ROOT}/include" \
    rados=yes make

"-ltcmalloc" is necessary if ceph was compiled with tcmalloc.

Messenger
---------

This fio engine allows you to test CEPH messenger transport layer, without
any disk activities involved.

To build fio_ceph_messenger:
```
  ./do_cmake.sh -DWITH_FIO=ON
  cd build
  make fio_ceph_messenger
```
If you install the ceph libraries to a location that isn't in your
LD_LIBRARY_PATH, be sure to add it:

    export LD_LIBRARY_PATH=/path/to/install/lib

To view the fio options specific to the messenger engine:

    ./fio --enghelp=libfio_ceph_messenger.so

The ceph_conf_file= option requires a ceph configuration file (ceph.conf),
see ceph-messenger.conf and ceph-messenger.fio for details.

To run:

    ./fio ./ceph-messenger.fio

### Phase 0 instrumentation and modes

The engine carries copy-accounting instrumentation and several payload
modes used by the messenger zero-copy investigation. Counters are
emitted in the engine teardown dump (look for the `PERFCOUNTERS`,
`PERF HISTOGRAMS`, and `BUFFER CRC CACHE` marker blocks):

- `l_msgr_send_bytes_copied` — payload bytes that went through the
  kernel copy on send (today: all of them).
- `l_msgr_send_iov_segments` — histogram of iovec elements per
  `send()`; this is the headline send-shape counter.
- `buffer_cached_crc` / `buffer_missed_crc` — bufferlist CRC-cache
  hits/misses (`buffer_track_crc` is enabled by the engine).

Payload-shape options (default off == single contiguous non-owning
ptr, original behavior, no allocation or memcpy in the harness):

- `payload_frags=N` — split each send payload into N non-owning
  bufferptrs carved from fio's I/O buffer, to mimic the
  RGW-multipart / EC-scatter shape that drives multi-segment iovec
  assembly. Shifts the `l_msgr_send_iov_segments` histogram.
- `payload_frag_unalign=1` — skew the fragment boundaries off page
  alignment.

EC read send-shape models (companions; run back to back and diff the
`l_msgr_send_iov_segments` histogram — that delta is the EC
horizontal-gather reply-fragmentation component):

    ./fio ./ceph-messenger-ec-r1.fio   # single-shard / client-scatter
    ./fio ./ceph-messenger-ec-r2.fio   # full-stripe horizontal gather

### Data-integrity gate (verify_echo)

`verify_echo=1` round-trips the payload and the **engine self-checks**
it: the receiver echoes the bytes it actually received, and the sender
compares crc32c + length of the echo against a snapshot taken just
before send. A mismatch fails the io_u (`EILSEQ`) so fio aborts. This
catches corrupted/stale sends — iovec mis-assembly, fragment-boundary
bugs, truncation/partial send, and (in Phase 1) a buffer
retired/mutated before the kernel finished sending or recycled by
another in-flight request.

    ./fio ./ceph-messenger-verify.fio

Why not fio's `verify=crc32c`: fio's verify is medium/offset-keyed
(write a pattern to offset X, later read offset X back from the
medium), but this engine is a storageless request/reply pipe keyed by
an io_u pointer abused as the object name — it has no offset→data map,
and `FIO_UNIDIR` disables fio's post-write verify phase. fio-native
verify fails even on a correct build ("bad magic header 0"). The
engine-side crc compare needs no medium and is the actual Phase-1
buffer-lifetime gate.

`verify_echo` adds a full reply payload and a crc scan, so it **must
not be used for throughput/bandwidth runs** — the memory-traffic
numbers would be polluted. Bandwidth baselines use `ceph-messenger.fio`
and the EC R1/R2 variants (all with `verify_echo` off).

#### Broken-build gate procedure (env-gated, no rebuild)

The gate proves the job actually catches a bad send. It is built into
the engine and toggled by env vars — no code edit or rebuild:

- `CEPH_MSGR_VERIFY_ECHO_CORRUPT=1` — mutate one payload byte after
  the integrity snapshot but before the send (and invalidate the CRC
  cache so it round-trips; see the finding below). Models the Phase-1
  defect class: a buffer changed out from under an in-flight send.
- `CEPH_MSGR_VERIFY_ECHO_DEBUG=1` — print `sent` vs `echoed`
  len/crc32c for every reply (diagnostic).

Pass:

    ./fio ./ceph-messenger-verify.fio
    # runs to completion, err=0, no "verify_echo MISMATCH"

Fail (gate fires):

    CEPH_MSGR_VERIFY_ECHO_CORRUPT=1 ./fio ./ceph-messenger-verify.fio
    # engine prints "fio: verify_echo MISMATCH io=... sent crc=A
    # echoed crc=B" and fio aborts with an io_u error (EILSEQ)

Record both outcomes as the gate evidence.

Finding (why `invalidate_crc()` is in the corrupt path): a naive
in-place mutation of a pending send buffer is **masked by the
bufferlist per-raw CRC cache**. `buflist.crc32c()` (the integrity
snapshot) caches the crc on the non-owning `raw` wrapping the fio
buffer; an external pointer write does *not* invalidate it, so the
messenger ships a *stale* data-CRC while the wire carries the mutated
bytes, and the receiver rejects the frame — the corruption is caught,
but as a connection CRC error with no round trip, not a legible
content mismatch. The self-test calls
`req_msg->get_data().invalidate_crc()` so the messenger re-CRCs the
mutated bytes, the frame round-trips, and the echo self-check reports
it cleanly. Corollary for MSG_ZEROCOPY: any code path that mutates or
recycles a buffer while a send is still pending must
invalidate/guard the CRC cache, or corruption surfaces as opaque
connection resets.

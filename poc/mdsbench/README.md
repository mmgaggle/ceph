# CephFS stable-write benchmark (proof of concept)

These are the tools and raw results behind the commits on this branch. They
measure NFS-Ganesha-style stable writes through libcephfs: `ceph_ll_write()`
followed by `ceph_ll_fsync()`, as FSAL_CEPH issues for a FILE_SYNC write or a
COMMIT. This directory is for reference, not for merging.

## What is on this branch

On top of main at `9e49f5cb896`:

1. ceph/ceph#70356 (Greg Farnum, "RADOS fsync"), 18 commits cherry-picked
   unchanged from PR head `74ecf78c9ecf`.
2. `osdc/Objecter: decode stat() from the stat op's own output`. This fixes
   crash recovery under #70356, which otherwise recovers the change_attr as
   the file size.
3. `osdc/Journaler: optionally coalesce flushes while writes are in flight`,
   a group-commit prototype behind `journaler_max_inflight_flushes`, which
   defaults to 0 (off).
4. `client: send uncached writes without holding client_lock`, behind
   `client_write_submit_unlocked`, which defaults to true, with a new test.
5. This directory.

The `src/` tree matches what was built and measured.

## Files

| File | Purpose |
|---|---|
| `cephfs_syncbench.c` | The driver. N threads over M libcephfs mounts, each doing a write followed by `ceph_ll_fsync(fh, syncdataonly)`. `--async QD` uses non-blocking writes with the fsync flag; `--fscrypt` runs in an encrypted directory. |
| `harness.py` | Runs the driver for one configuration and appends one JSON record to `results.jsonl`. Each record has ops/s, latency percentiles, mds_lock utilisation and phases, journal latency, events per journal write, and per-thread CPU for the driver, MDS and OSDs. |
| `mutexprof.c` | An LD_PRELOAD pthread mutex profiler. It reports hold and wait time per mutex, and attributes hold time to call sites by sampled backtrace. |
| `symbolize.py`, `sites.py` | Summarise mutexprof output. |
| `cluster.sh` | Starts a fresh vstart cluster: 3 OSDs, memstore or bluestore, N active MDS ranks, debug logging off, MDS phase tracker on. |
| `restart_mds.sh` | Restarts the MDS daemons after an MDS-only rebuild. |
| `crashcheck.c`, `crashcheck.sh` | Crash-recovery check: write, `kill -9` the client, evict it, then stat from another mount. |
| `clientlock.sh` | Compares 1 mount with 8 mounts with #70356 on, at data pool size 3 and size 1. |
| `real_ab.sh` | Runs the unit tests, then the final A/B of `client_write_submit_unlocked`. |
| `appendbench.py` | RADOS append throughput into one object versus spread over N objects, at a fixed queue depth. One object is the shape of the MDS journal stream; N objects is the shape of data writes. |
| `results.jsonl` | Every run, one JSON record each. |

## Environment used

- **Host:** bravo, Fedora 44, 16 cores / 32 threads, 123 GiB RAM, one consumer NVMe.
- **Container:** builds and runs inside rootless podman (`ubuntu:24.04`, host networking). This checkout is at `/work/ceph` and this directory at `/work/bench`. The scripts hardcode those paths.
- **Build:** Ninja, `RelWithDebInfo`, `WITH_TESTS=ON`, `WITH_RADOSGW=OFF`, dashboard frontend off. Targets: `vstart`, `ceph_test_client`, `ceph_test_client_fscrypt`.
- **Cluster:** all daemons and the client share the host. Memstore stands in for fast media and needs `memstore_page_set=true` (see Gotchas).

Build the tools inside the container:

```
mkdir -p inc
ln -s /work/ceph/src/include/cephfs inc/
ln -s /work/ceph/src/include/rados inc/
gcc -O2 -g -Wall -D_FILE_OFFSET_BITS=64 -o cephfs_syncbench cephfs_syncbench.c \
    -Iinc -L/work/ceph/build/lib -lcephfs -lpthread -Wl,-rpath,/work/ceph/build/lib
gcc -O2 -g -Wall -D_FILE_OFFSET_BITS=64 -o crashcheck crashcheck.c \
    -Iinc -L/work/ceph/build/lib -lcephfs -Wl,-rpath,/work/ceph/build/lib
gcc -O2 -g -Wall -shared -fPIC -o libmutexprof.so mutexprof.c -ldl -lpthread
```

Two of these settings are deliberate:

- The headers are staged under `inc/`, because `-I src/include` shadows the system `utime.h`.
- The libcephfs clients need `-D_FILE_OFFSET_BITS=64`.

Example run:

```
./cluster.sh memstore 1
python3 harness.py --threads 64 --mounts 1 --sdo 0 --client-oc 0 --label demo
python3 harness.py --threads 8 --async 8 --client-oc 0 \
    --set client_write_submit_unlocked=false --label demo-async
```

## Run tags in results.jsonl

| Tag prefix | Experiment |
|---|---|
| `e1-`, `e2-` | First latency and scaling sweeps on memstore. `e2-` ran without `memstore_page_set` and plateaued at about 8K ops/s because of memstore itself; discarded. |
| `bs-` | Bluestore scaling on main. |
| `bsgc0..8-` | Bluestore, group-commit cap not actually applied; usable only as five baseline repeats. |
| `bsgcvN-` | Bluestore, `journaler_max_inflight_flushes=N`. |
| `msgcN-` | Memstore, `journaler_max_inflight_flushes=N`. |
| `msk1-`, `msk1hi-`, `msk0-` | Memstore. Group commit on (`k1`) across 1 vs many mounts and up to 192 threads; `k0` is the `syncdataonly=1` reference. |
| `mm2k0-`, `mm2k1-` | Two active MDS ranks with pinned directories, group commit off / on. |
| `prms-`, `prbs-` | #70356 with `client_fsync_to_rados` false / true, memstore / bluestore. |
| `cl-s3-`, `cl-s1-` | client_lock check with #70356 on, data pool size 3 / 1. |
| `sites-` | client_lock profile with per-call-site attribution. |
| `ab-`, `ab2-` | An earlier prototype of the unlocked submit, gated by an environment variable. It is not on this branch. |
| `real-` | Final A/B of `client_write_submit_unlocked` (`real_ab.sh`). |

## Gotchas

- **Memstore:**
  - Without `memstore_page_set=true`, appends to one large object slow down as the object grows. That throttles the MDS journal artificially.
  - At pool size 1, OSDs fill after many runs and writes fail with -28. Start a fresh cluster, or use `--file-mb 32`.
- **Container:**
  - It has no init process, so killed daemons stay as zombies. Check `/proc/<pid>/stat`, not `kill -0`.
  - Outside `build/`, the `ceph` CLI needs `-c /work/ceph/build/ceph.conf`. Without it, commands can fail silently.

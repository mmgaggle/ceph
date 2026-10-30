#!/bin/bash
# Fresh vstart cluster for the sync-write benchmark (run inside the container).
#   cluster.sh [memstore|bluestore] [num_active_mds]
set -euo pipefail
STORE=${1:-memstore}
NMDS=${2:-1}
cd /work/ceph/build
export PATH=$PWD/bin:$PATH

../src/stop.sh > /dev/null 2>&1 || true
rm -rf out dev asok ceph.conf keyring /tmp/ceph-asok.* /tmp/syncbench.*.log

extra=()
case $STORE in
memstore) extra+=(--memstore -o "memstore_device_bytes = 34359738368" -o "memstore_page_set = true") ;;
bluestore) extra+=(--bluestore) ;;
*) echo "unknown store $STORE" >&2; exit 1 ;;
esac

MON=1 OSD=3 MDS=$NMDS CEPH_MAX_MDS=$NMDS MGR=1 RGW=0 NFS=0 \
  ../src/vstart.sh -n -x --without-dashboard "${extra[@]}" > /work/vstart.log 2>&1

ceph config set mds mds_enable_phase_tracker true
for s in ms mds mds_log mds_locker mds_balancer osd objecter bluestore bluefs \
         rocksdb bdev memstore journal monc mgrc auth client objectcacher \
         finisher heartbeatmap perfcounter; do
  ceph config set global debug_$s 0/0
done
ceph config set osd osd_debug_op_order false
ceph config set osd osd_debug_misdirected_ops false
# ceph.conf entries outrank the config db; override at runtime too.
for d in $(ceph osd ls | sed 's/^/osd./') $(ceph fs dump -f json 2>/dev/null |
    python3 -c 'import json,sys; print(" ".join("mds."+i["name"] for f in json.load(sys.stdin)["filesystems"] for i in f["mdsmap"]["info"].values()))'); do
  ceph tell "$d" config set debug_ms 0/0 > /dev/null
  ceph tell "$d" config set debug_osd 0/0 > /dev/null || true
  ceph tell "$d" config set debug_mds 0/0 > /dev/null || true
done

for _ in $(seq 120); do
  n=$(ceph fs dump -f json 2>/dev/null |
      python3 -c 'import json,sys; print(sum(1 for f in json.load(sys.stdin)["filesystems"] for i in f["mdsmap"]["info"].values() if i["state"]=="up:active"))')
  [ "$n" -ge "$NMDS" ] && break
  sleep 1
done
ceph -s
ceph fs status

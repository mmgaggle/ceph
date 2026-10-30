#!/bin/bash
# clientlock.sh: with client_fsync_to_rados on, is the per-Client lock the
# limit? Compare one mount against several, profile client_lock, and repeat
# with a size-1 data pool so the OSDs cost a third as much per write.
set -u
cd /work/bench
C="/work/ceph/build/bin/ceph -c /work/ceph/build/ceph.conf"
./cluster.sh memstore 1 > /work/clientlock-cluster.log 2>&1 || { echo cluster failed; exit 1; }
$C config set client client_fsync_to_rados true
$C config get client client_fsync_to_rados
data=$($C fs ls -f json | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["data_pools"][0])')
for size in 3 1; do
  if [ "$size" = 1 ]; then
    $C config set global mon_allow_pool_size_one true
    $C osd pool set "$data" size 1 --yes-i-really-mean-it
    sleep 5
  fi
  $C osd pool get "$data" size
  python3 harness.py --label cl-s$size --threads 64 --mounts 1 --sdo 0 --client-oc 0 --prof
  python3 harness.py --label cl-s$size --threads 16,64,128 --mounts 1,8 --sdo 0 --client-oc 0
done

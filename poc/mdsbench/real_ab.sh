#!/bin/bash
# real_ab.sh: unit tests, then A/B client_write_submit_unlocked=false/true on a
# fresh memstore cluster (client_fsync_to_rados on, size-1 data pool).
set -u
cd /work/bench
B=/work/ceph/build
C="$B/bin/ceph -c $B/ceph.conf"
./cluster.sh memstore 1 > /work/real_ab-cluster.log 2>&1 || { echo cluster failed; exit 1; }

echo "== unit tests"
cd $B
for bin in ceph_test_client ceph_test_client_fscrypt; do
  for oc in false true; do
    CEPH_CONF=$B/ceph.conf timeout 1800 bin/$bin --client_oc=$oc \
      --gtest_filter='*ConcurrentWrite*:TestClient.Llreadv*:TestClient.SyncIO*-*QuotaFull*' \
      > /work/unit-$bin-oc$oc.log 2>&1
    echo "$bin client_oc=$oc rc=$? $(grep -E '^\[  (PASSED|FAILED)' /work/unit-$bin-oc$oc.log | tr '\n' ' ')"
    grep -E '^\[  FAILED  \]' /work/unit-$bin-oc$oc.log | head -5
  done
done
cd /work/bench

$C config set client client_fsync_to_rados true
data=$($C fs ls -f json | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["data_pools"][0])')
$C config set global mon_allow_pool_size_one true
$C osd pool set "$data" size 1 --yes-i-really-mean-it
sleep 5
echo "fsync_to_rados=$($C config get client client_fsync_to_rados) $($C osd pool get "$data" size)"

H="python3 harness.py --file-mb 32 --mounts 1 --sdo 0 --client-oc 0"
ab() {  # label, harness args...
  local label=$1; shift
  $H --label "$label-off" --set client_write_submit_unlocked=false "$@"
  $H --label "$label-on" --set client_write_submit_unlocked=true "$@"
}
echo "== sync"
for rep in 1 2; do ab real-sync-r$rep --threads 64; done
ab real-sync --threads 128
echo "== async"
for rep in 1 2; do ab real-async-r$rep --threads 8 --async 8; done
ab real-async --threads 16 --async 8
echo "== fscrypt"
ab real-enc --threads 64 --fscrypt
ab real-enc-async --threads 8 --async 8 --fscrypt
echo "== profile"
$H --label real-prof-on --set client_write_submit_unlocked=true --threads 64 --prof
f=prof/$(ls prof | grep real-prof-on | tail -1)
python3 sites.py "$f"
python3 symbolize.py "$f" | head -4
$C df | sed -n 1,4p

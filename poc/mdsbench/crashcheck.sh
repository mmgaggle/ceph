#!/bin/bash
# crashcheck.sh LABEL RADOS_FSYNC(true|false) NWRITES IOSIZE [PUNCH_OFF PUNCH_LEN]
# Write+fsync, kill -9 the writer, evict its session, stat from a new mount.
set -u
label=$1 rf=$2; shift 2
cd /work/ceph/build
C="bin/ceph -c /work/ceph/build/ceph.conf"
$C config set client client_fsync_to_rados "$rf" > /dev/null 2>&1
path=/crash/$label-$RANDOM
out=$(mktemp)
/work/bench/crashcheck write /work/ceph/build/ceph.conf "$path" "$@" > "$out" 2>&1 &
pid=$!
for _ in $(seq 120); do grep -q READY "$out" && break; sleep 0.5; done
id=$(awk '/CLIENT_ID/ {print $2}' "$out")
kill -9 $pid
wait $pid 2> /dev/null
$C tell mds.a client evict id="$id" > /dev/null 2>&1
echo "== $label (client_fsync_to_rados=$rf) $path"
grep SEEN "$out"
timeout 120 /work/bench/crashcheck stat /work/ceph/build/ceph.conf "$path" 2>&1 | grep -v -E "WARNING|ms_handle_reset"
rm -f "$out"

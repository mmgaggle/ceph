#!/bin/bash
# Restart the vstart MDS daemons on the current build/bin/ceph-mds.
# The container has no init, so an exited daemon lingers as a zombie;
# treat state Z as gone.
set -u
cd /work/ceph/build
export PATH=$PWD/bin:$PATH
alive() { [ -e /proc/$1/stat ] && [ "$(awk '{print $3}' /proc/$1/stat)" != Z ]; }
names=()
for d in dev/mds.*; do
  n=${d#dev/mds.}; names+=("$n")
  pid=$(cat "out/mds.$n.pid" 2>/dev/null || echo 0)
  if [ "$pid" -gt 0 ] && alive "$pid"; then
    kill "$pid"
    for _ in $(seq 60); do alive "$pid" || break; sleep 0.5; done
  fi
done
for n in "${names[@]}"; do
  ceph-mds -i "$n" -c /work/ceph/build/ceph.conf
done
want=${#names[@]}
max=$(ceph fs get a -f json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["mdsmap"]["max_mds"])')
for _ in $(seq 120); do
  n=$(ceph fs dump -f json 2>/dev/null | python3 -c 'import json,sys; print(sum(1 for f in json.load(sys.stdin)["filesystems"] for i in f["mdsmap"]["info"].values() if i["state"]=="up:active"))')
  [ "$n" -ge "$max" ] && break
  sleep 1
done
for n in "${names[@]}"; do
  ceph tell mds.$n config set debug_mds 0/0 > /dev/null 2>&1
  ceph tell mds.$n config set debug_ms 0/0 > /dev/null 2>&1
done
ceph fs status 2>/dev/null | grep -E "active|standby"

#!/usr/bin/env python3
"""Append throughput into one RADOS object vs. spread over N objects, at a
fixed queue depth -- the shape of the MDS journal stream vs. data writes."""
import sys
import threading
import time

import rados

pool, nobj, qd, size, secs = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), float(sys.argv[5])
cluster = rados.Rados(conffile="/work/ceph/build/ceph.conf")
cluster.connect()
io = cluster.open_ioctx(pool)
names = [f"appendbench.{i}" for i in range(nobj)]
payload = b"j" * size
lock = threading.Condition()
state = {"inflight": 0, "done": 0, "lat": 0.0}
deadline = time.monotonic() + secs


def issue(i):
    t0 = time.monotonic()

    def cb(_):
        with lock:
            state["inflight"] -= 1
            state["done"] += 1
            state["lat"] += time.monotonic() - t0
            lock.notify()
    io.aio_write(names[i % nobj], payload, offset=(i // nobj) * size, oncomplete=cb)


i = 0
start = time.monotonic()
while time.monotonic() < deadline:
    with lock:
        while state["inflight"] >= qd:
            lock.wait()
        state["inflight"] += 1
    issue(i)
    i += 1
with lock:
    while state["inflight"]:
        lock.wait()
el = time.monotonic() - start
print(f"objects={nobj} qd={qd} size={size}: {state['done'] / el:.0f} writes/s, "
      f"mean lat {state['lat'] / state['done'] * 1000:.2f} ms")
for n in names:
    try:
        io.remove_object(n)
    except rados.ObjectNotFound:
        pass
io.close()
cluster.shutdown()

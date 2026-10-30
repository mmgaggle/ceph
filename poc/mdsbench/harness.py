#!/usr/bin/env python3
"""Drive cephfs_syncbench against a vstart cluster and attribute the cost.

Per run: start the driver, wait for READY, warm up, snapshot MDS state
(mds_lock counters, perf dump, per-thread CPU), open the window with SIGUSR1,
close it with SIGUSR2, snapshot again, stop the driver, and append one JSON
record to results.jsonl.
"""
import argparse
import glob
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import time

BUILD = "/work/ceph/build"
CONF = f"{BUILD}/ceph.conf"
BENCH = "/work/bench"
DRIVER = f"{BENCH}/cephfs_syncbench"
SHIM = f"{BENCH}/libmutexprof.so"
HZ = os.sysconf("SC_CLK_TCK")


def asok(path, prefix, **kw):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect(path)
    s.sendall(json.dumps({"prefix": prefix, "format": "json", **kw}).encode() + b"\0")
    hdr = b""
    while len(hdr) < 4:
        hdr += s.recv(4 - len(hdr))
    n = struct.unpack(">I", hdr)[0]
    buf = b""
    while len(buf) < n:
        buf += s.recv(n - len(buf))
    s.close()
    return json.loads(buf)


def mds_sockets():
    out = {}
    for p in glob.glob("/tmp/ceph-asok.*/mds.*.asok") + glob.glob(f"{BUILD}/asok/mds.*.asok"):
        name = os.path.basename(p)[:-5]
        if name not in out or os.path.getmtime(p) > os.path.getmtime(out[name]):
            out[name] = p
    return out


def active_mds(socks):
    act = {}
    for name, p in socks.items():
        try:
            st = asok(p, "status")
        except OSError:
            continue
        if st.get("state") == "up:active":
            act[name] = p
    return act


def pid_of(name):
    with open(f"{BUILD}/out/{name}.pid") as f:
        return int(f.read().strip())


def threads_cpu(pid):
    """{comm: cpu_ticks} summed over threads with the same name."""
    out = {}
    for t in glob.glob(f"/proc/{pid}/task/*/stat"):
        try:
            data = open(t).read()
        except OSError:
            continue
        comm = data[data.index("(") + 1:data.rindex(")")]
        rest = data[data.rindex(")") + 2:].split()
        out[comm] = out.get(comm, 0) + int(rest[11]) + int(rest[12])
    return out


def host_ticks():
    f = open("/proc/stat").readline().split()[1:]
    v = list(map(int, f))
    idle = v[3] + v[4]
    return sum(v) - idle, sum(v)


def osd_pids():
    return [pid_of(os.path.basename(p)[:-4]) for p in glob.glob(f"{BUILD}/out/osd.*.pid")]


def snapshot(mds, driver_pid):
    snap = {"t": time.monotonic(), "mds": {}, "driver": threads_cpu(driver_pid),
            "host": host_ticks(),
            "osd_ticks": sum(sum(threads_cpu(p).values()) for p in osd_pids())}
    for name, p in mds.items():
        ph = asok(p, "dump phase times")
        snap["mds"][name] = {
            "phase": ph.get("phase_times", ph),
            "perf": asok(p, "perf dump"),
            "cpu": threads_cpu(pid_of(name)),
        }
    return snap


def cpu_delta(a, b, secs, top=6):
    d = {k: (b.get(k, 0) - a.get(k, 0)) / HZ / secs * 100 for k in b}
    d = {k: round(v, 1) for k, v in sorted(d.items(), key=lambda kv: -kv[1]) if v >= 0.5}
    return {"total_pct": round(sum(d.values()), 1), "top": dict(list(d.items())[:top])}


def perf_avg(a, b, sect, key):
    try:
        x, y = a[sect][key], b[sect][key]
        n = y["avgcount"] - x["avgcount"]
        return {"n": n, "avg_ms": round((y["sum"] - x["sum"]) / n * 1000, 3) if n else None}
    except (KeyError, TypeError):
        return None


def perf_rate(a, b, sect, key, secs):
    try:
        return round((b[sect][key] - a[sect][key]) / secs, 1)
    except (KeyError, TypeError):
        return None


def summarize(a, b, secs):
    out = {"driver_cpu": cpu_delta(a["driver"], b["driver"], secs),
           "osd_cpu_pct": round((b["osd_ticks"] - a["osd_ticks"]) / HZ / secs * 100, 1),
           "host_busy_frac": round((b["host"][0] - a["host"][0]) / (b["host"][1] - a["host"][1]), 3),
           "mds": {}}
    for name in b["mds"]:
        pha, phb = a["mds"][name]["phase"], b["mds"][name]["phase"]
        if not phb.get("enabled"):
            sys.exit(f"{name}: set mds_enable_phase_tracker=true")
        win = phb["elapsed_sec"] - pha["elapsed_sec"]
        la, lb = pha["mds_lock"], phb["mds_lock"]
        acq = lb["acquisitions"] - la["acquisitions"]
        held = lb["held_sec"] - la["held_sec"]
        wait = lb["wait_sec"] - la["wait_sec"]
        before = {x["phase"]: x for x in pha["phases"]}
        phases = []
        for x in phb["phases"]:
            y = before.get(x["phase"], {"total_sec": 0, "count": 0})
            dt, dn = x["total_sec"] - y["total_sec"], x["count"] - y["count"]
            if dn:
                phases.append((x["phase"], dt, dn))
        phases.sort(key=lambda t: -t[1])
        pa, pb = a["mds"][name]["perf"], b["mds"][name]["perf"]
        out["mds"][name] = {
            "lock_util": round(held / win, 3),
            "lock_acq_per_sec": round(acq / win),
            "lock_wait_us_avg": round(wait / acq * 1e6, 1) if acq else None,
            "lock_hold_us_avg": round(held / acq * 1e6, 2) if acq else None,
            "phases": {n: {"pct_wall": round(dt / win * 100, 1), "per_sec": round(dn / win),
                           "mean_us": round(dt / dn * 1e6, 1)} for n, dt, dn in phases[:6]},
            "cpu": cpu_delta(a["mds"][name]["cpu"], b["mds"][name]["cpu"], secs),
            "jlat": perf_avg(pa, pb, "mds_log", "jlat"),
            "caps_per_sec": perf_rate(pa, pb, "mds", "handle_client_caps", secs),
            "caps_dirty_per_sec": perf_rate(pa, pb, "mds", "handle_client_caps_dirty", secs),
            "journal_events_per_sec": perf_rate(pa, pb, "mds_log", "evadd", secs),
        }
    return out


def run_one(args, threads, mounts, sdo, tag, nosync=False, append=False, prof=False,
            pin=0, client_oc=-1, iosize=4096, qd=0, fscrypt=False):
    mds = active_mds(mds_sockets())
    if not mds:
        sys.exit("no active MDS")
    rundir = f"/bench/{tag}"
    cmd = [DRIVER, "--conf", CONF, "--dir", rundir, "--threads", str(threads),
           "--mounts", str(mounts), "--syncdataonly", str(sdo), "--iosize", str(iosize),
           "--file-mb", str(args.file_mb), "--client-oc", str(client_oc),
           "--pin-ranks", str(pin)]
    if nosync:
        cmd.append("--nosync")
    if qd:
        cmd += ["--async", str(qd)]
    if fscrypt:
        cmd.append("--fscrypt")
    for kv in args.set:
        cmd += ["--set", kv]
    if append:
        cmd.append("--append")
    env = dict(os.environ)
    prof_out = None
    if prof:
        prof_out = f"{BENCH}/prof/{tag}.txt"
        os.makedirs(os.path.dirname(prof_out), exist_ok=True)
        cmd += ["--prof-out", prof_out]
        env["LD_PRELOAD"] = SHIM
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, env=env, cwd=BUILD)
    line = p.stdout.readline().strip()
    if line != "READY":
        p.kill()
        sys.exit(f"{tag}: driver did not become ready: {line!r}")
    time.sleep(args.warmup)
    a = snapshot(mds, p.pid)
    p.send_signal(signal.SIGUSR1)
    time.sleep(args.duration)
    p.send_signal(signal.SIGUSR2)
    b = snapshot(mds, p.pid)
    p.send_signal(signal.SIGTERM)
    res = json.loads(p.stdout.readline())
    p.wait(timeout=120)
    rec = {"tag": tag, "prof": prof_out, **res, **summarize(a, b, b["t"] - a["t"])}
    with open(f"{BENCH}/results.jsonl", "a") as f:
        f.write(json.dumps(rec) + "\n")
    m = next(iter(rec["mds"].values()))
    print(f"{tag:28s} ops/s={rec['ops_per_sec']:>9.0f} op_p50={rec['op_us']['p50']:>6}us "
          f"p99={rec['op_us']['p99']:>6}us  mds_lock_util="
          + ",".join(f"{v['lock_util']:.2f}" for v in rec["mds"].values())
          + f"  mds_cpu={m['cpu']['total_pct']}%  drv_cpu={rec['driver_cpu']['total_pct']}%"
          f"  osd_cpu={rec['osd_cpu_pct']}%  host={rec['host_busy_frac']:.2f}  jlat={m['jlat'] and m['jlat']['avg_ms']}ms"
          f"  top_phase={next(iter(m['phases'].items()), None)}",
          flush=True)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warmup", type=float, default=5)
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--file-mb", type=int, default=64)
    ap.add_argument("--threads", default="1")
    ap.add_argument("--mounts", default="1", help="comma list; 'T' means one per thread")
    ap.add_argument("--sdo", default="0")
    ap.add_argument("--client-oc", type=int, default=-1)
    ap.add_argument("--nosync", action="store_true")
    ap.add_argument("--append", action="store_true")
    ap.add_argument("--prof", action="store_true")
    ap.add_argument("--pin", type=int, default=0)
    ap.add_argument("--iosize", type=int, default=4096)
    ap.add_argument("--label", default="r")
    ap.add_argument("--async", dest="qd", type=int, default=0)
    ap.add_argument("--fscrypt", action="store_true")
    ap.add_argument("--set", action="append", default=[], help="client conf key=value")
    args = ap.parse_args()
    for sdo in [int(x) for x in args.sdo.split(",")]:
        for m in args.mounts.split(","):
            for t in [int(x) for x in args.threads.split(",")]:
                mounts = t if m == "T" else int(m)
                tag = (f"{args.label}-t{t}-m{mounts}-sdo{sdo}"
                       f"{'-nosync' if args.nosync else ''}{'-app' if args.append else ''}"
                       f"{'-oc%d' % args.client_oc if args.client_oc >= 0 else ''}"
                       f"{'-pin%d' % args.pin if args.pin else ''}{'-prof' if args.prof else ''}"
                       f"{'-aq%d' % args.qd if args.qd else ''}{'-enc' if args.fscrypt else ''}"
                       f"-{int(time.time()) % 100000}")
                run_one(args, t, mounts, sdo, tag, nosync=args.nosync, append=args.append,
                        prof=args.prof, pin=args.pin, client_oc=args.client_oc,
                        iosize=args.iosize, qd=args.qd, fscrypt=args.fscrypt)


if __name__ == "__main__":
    main()

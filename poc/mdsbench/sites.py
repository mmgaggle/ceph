#!/usr/bin/env python3
"""Summarize mutexprof's sampled stacks for the hottest mutex: share of hold
and wait time per calling stack, collapsed to the first few Ceph frames."""
import collections
import sys

from symbolize import frames

SKIP = ("std::", "__gthread", "__gnu_cxx", "ceph::mutex_debug", "pthread_", "??")


def stack(fr):
    out = []
    for f in fr:
        for name in frames(f) or (f,):
            n = name.split("(")[0]
            if n.startswith(SKIP) or "libmutexprof" in n or n in out:
                continue
            out.append(n)
    return out[:4]


def main(path):
    lines = open(path).read().splitlines()
    window = int(lines[0].split()[1])
    every = None
    agg = collections.defaultdict(lambda: [0, 0, 0])
    for ln in lines:
        if ln.startswith("# sites"):
            every = int(ln.split("sample_every")[1].split(":")[0])
        if not ln.startswith("site "):
            continue
        parts = ln.split()
        n, hold, wait = map(int, parts[1:4])
        key = " <- ".join(stack(parts[4:]))
        a = agg[key]
        a[0] += n; a[1] += hold; a[2] += wait
    tot_hold = sum(v[1] for v in agg.values()) or 1
    print(f"sampled 1/{every}; hold% is share of sampled client_lock hold time")
    print(f"{'hold%':>6} {'us/hold':>8} {'wait_us':>8} {'acq/s':>8}  stack")
    for k, (n, hold, wait) in sorted(agg.items(), key=lambda kv: -kv[1][1])[:12]:
        print(f"{hold / tot_hold * 100:6.1f} {hold / n / 1e3:8.2f} {wait / n / 1e3:8.1f} "
              f"{n * every / window * 1e9:8.0f}  {k}")


if __name__ == "__main__":
    main(sys.argv[1])

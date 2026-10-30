#!/usr/bin/env python3
"""Turn a mutexprof dump into a table: hold and wait time as a share of the
window, acquisition rate, and the call site that first took each mutex."""
import functools
import subprocess
import sys


@functools.lru_cache(maxsize=None)
def frames(site):
    if "+0x" not in site:
        return ()
    path, off = site.rsplit("+", 1)
    out = subprocess.run(["addr2line", "-f", "-C", "-i", "-e", path, hex(int(off, 16) - 1)],
                         capture_output=True, text=True).stdout.splitlines()
    return tuple(out[i] for i in range(0, len(out), 2))


def where(site):
    fs = frames(site)
    if not fs:
        return site
    mine = [f for f in fs if not f.startswith(("std::", "__gthread", "__gnu_cxx"))]
    short = lambda f: f.split("(")[0]
    return short(mine[0]) if mine else short(fs[-1])


def main(path, top=15):
    lines = open(path).read().splitlines()
    window = int(lines[0].split()[1])
    print(f"window {window / 1e9:.1f}s")
    print(f"{'hold%':>7} {'wait%':>8} {'acq/s':>9} {'cont%':>6}  first-site / contended-site")
    for ln in lines[2:2 + top]:
        key, hold, wait, acq, cont, s1, s2 = ln.split()
        hold, wait, acq, cont = map(int, (hold, wait, acq, cont))
        print(f"{hold / window * 100:7.1f} {wait / window * 100:8.1f} {acq / window * 1e9:9.0f} "
              f"{cont / acq * 100 if acq else 0:6.1f}  {where(s1)}"
              + (f"  /  {where(s2)}" if s2 != "0x0" and where(s2) != where(s1) else ""))


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 15)

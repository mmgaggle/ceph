# P models of Ceph protocols

These are [P](https://p-org.github.io/P/) models of correctness arguments
in Ceph. Each one states properties and models the code as it is. Some
configurations remove a mechanism the code relies on, or break an
assumption. Each of those must produce a counterexample. That shows the
model can see the failure, and makes the configuration a regression test
for the mechanism.

| Model | Covers | Properties |
|---|---|---|
| [`rbd_image`](rbd_image/README.md) | librbd clients that share an image. The exclusive lock: acquire, request, release, break, blocklist, lost watches, re-acquire. Writes under the lock. Snapshot create, remove, protect and unprotect, with operations sent to the lock owner. Clone v1 and v2, flatten, reads through to the parent. | A write comes from the lock holder. What a snapshot shows never changes. A parent snapshot is not removed under a child. A child finds its parent's data. A snapshot create is not answered EEXIST for the snapshot it made. Every action is answered. No snapshot id leaks. |

## Running

The models are not part of the Ceph build. They need the .NET 8 SDK and
the P tool, not the C++ toolchain.

Install the .NET 8 SDK (a distribution package, or `brew install
dotnet@8` on macOS), then P:

```
dotnet tool install --global P

./run.sh <model> [schedules] [jobs]       # every case against <model>/expect.txt
./deep.sh <model> <test case> [schedules] # one case under random, PCT and POS
```

The scripts find P in `~/.dotnet/tools`, and a Homebrew `dotnet@8` by
themselves. Otherwise, set `DOTNET_ROOT` to the directory that holds
`dotnet`. `run.sh` runs `jobs` cases at a time. `rbd_image` has 38
cases, at 20,000 schedules each.

If any of P's summaries reports a bug, `run.sh` counts the case as
violated. `p check -tc` matches test names by prefix and runs every
match, so no test name can be a prefix of another.

## What the models found

`rbd_image` finds six gaps on main, detailed in its README:

- **A new lock owner writes with a stale snap context.** If the owner
  saw no HeaderUpdate, it does not refresh the header on acquire. The
  previous owner can add a snapshot just before it is fenced, or just
  before it dies. That snapshot is not in the new owner's snap context.
  The new owner's writes then change what the snapshot shows.
- **A snapshot create that is retried after a lock owner announced
  itself is answered EEXIST**, for the snapshot it created. The owner
  answers the retry from its record of completed requests. The requester
  then waits for a completion that was already sent. After ten minutes
  the owner forgets the request and runs it again.
- **Protect and unprotect race each other and a clone v1.** Without
  journaling they run with no lock, and `set_protection_status` is a
  blind write. An unprotect that lost its scan to a concurrent protect
  still writes UNPROTECTED under a child, and the snapshot can then be
  removed.
- **Without the exclusive-lock feature, a snapshot is not a point in
  time.** Another client's writes with the old snap context land in it.
  A flatten without the lock can also leave a child's snapshot that
  reads a parent snapshot that no longer counts it.
- **A flatten that dies leaves a child that cannot be opened.** It
  detaches the child from the parent, which can remove the parent's
  trashed snapshot, before it clears the child's parent link.
- **`rbd rm` removes the header after it let go of the lock.** A peer
  that opens the image in between can snapshot and clone it, and the
  clone loses its parent.

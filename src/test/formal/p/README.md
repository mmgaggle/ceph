# P models of RGW protocols

These are [P](https://p-org.github.io/P/) models of correctness arguments
in RGW. Each one states properties and models the code as it is. Some
configurations remove a mechanism the code relies on, or break an
assumption; each of those must produce a counterexample. That shows the
model can see the failure, and makes the configuration a regression test
for the mechanism.

| Model | Covers | Properties |
|---|---|---|
| [`rgw_overwrite`](rgw_overwrite/README.md) | PutObject, DeleteObject, CopyObject with a shared tail, and multipart completion over existing keys: head-object races, the bucket index entry, `cls_refcount`, part re-uploads, abort, lifecycle's abort, GC | no head's data is deleted; the index matches the head; nothing leaks; every request answered |

## Running

The models are not part of the Ceph build: they need the .NET 8 SDK and
the P tool, not the C++ toolchain.

```
brew install dotnet@8
export DOTNET_ROOT=/opt/homebrew/opt/dotnet@8/libexec
dotnet tool install --global P

./run.sh <model> [schedules]             # every case against <model>/expect.txt
./deep.sh <model> <test case> [schedules] # one case under random, PCT and POS
```

`run.sh` counts a case as violated if *any* of P's summaries reports a bug.
`p check -tc` matches test names by prefix and runs every match, so no
test name may be a prefix of another.

## What the models found

`rgw_overwrite` finds seven gaps on main, detailed in its README:

- **The bucket index can keep a stale entry.** A stale or canceled
  completion still overwrites the entry's version. Three overlapping
  PutObjects can leave the index listing the wrong one, and a
  delete-put-delete sequence can leave it listing a deleted key.
- **Lifecycle's abort can delete a completing upload's data.** It aborts
  without the completion lock that AbortMultipartUpload takes.
- **A completion that leaves its meta object behind can lose the object's
  data later.** A later abort, or a retried completion, sends the
  completed object's parts to GC.
- **A completion that loses the head race leaks its parts.**
- **DeleteObject no longer checks that it removes the head it read.** A
  delete racing an overwrite leaks the new object's tail.
- **A copy that loses the head race leaks the source's tail**, through a
  reference no head carries.
- **A copy onto itself can write a deleted tail back into the head.** An
  overwrite between the copy's read and its write loses the object's data.

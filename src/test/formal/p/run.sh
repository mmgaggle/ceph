#!/bin/bash
# Check every test case of a P model against its expected outcome.
# usage: tasks/p/run.sh <model-dir> [schedules]      (default 20000)
# <model-dir>/expect.txt lists "<test case> holds" or
# "<test case> violated <text the failure message must contain>".
set -u
dir=$(cd "$1" && pwd); N=${2:-20000}
export DOTNET_ROOT=${DOTNET_ROOT:-/opt/homebrew/opt/dotnet@8/libexec}
export PATH=/opt/homebrew/opt/dotnet@8/bin:$PATH:$HOME/.dotnet/tools
export DOTNET_CLI_TELEMETRY_OPTOUT=1
cd "$dir"
p compile > PGenerated.compile.log 2>&1 || { tail -20 PGenerated.compile.log; exit 1; }
rc=0
while read -r tc want text; do
  [ -z "$tc" ] || [ "${tc:0:1}" = "#" ] && continue
  rm -rf "PCheckerOutput/$tc"
  out=$(p check -tc "$tc" -s "$N" --sch-pct 3 --max-steps 5000 -o "PCheckerOutput/$tc" 2>&1)
  # P can run more than one batch and print a summary for each: any bug
  # in any of them is a violation
  if echo "$out" | grep -qE "Checker found a bug|Found [1-9][0-9]* bugs?"; then got=violated
  elif echo "$out" | grep -q "Found 0 bugs"; then got=holds; else got=error; fi
  why=$(grep -h -m1 "<ErrorLog>" PCheckerOutput/$tc/BugFinding/*.txt 2>/dev/null | sed 's/.*<ErrorLog> //' | cut -c1-140)
  mark=ok
  if [ "$got" != "$want" ]; then mark=UNEXPECTED; rc=1
  elif [ "$want" = violated ] && [ -n "$text" ] && ! echo "$why" | grep -qF "$text"; then mark=WRONG-FAILURE; rc=1; fi
  printf "%-34s %-9s %-13s %s\n" "$tc" "$got" "$mark" "$why"
done < expect.txt
exit $rc

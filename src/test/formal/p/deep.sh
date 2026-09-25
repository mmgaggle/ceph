#!/bin/bash
# deep.sh <model-dir> <test case> [schedules]: one test case under random, PCT and POS
set -u
dir=$(cd "$1" && pwd); tc=$2; N=${3:-100000}
export DOTNET_ROOT=${DOTNET_ROOT:-/opt/homebrew/opt/dotnet@8/libexec}
export PATH=/opt/homebrew/opt/dotnet@8/bin:$PATH:$HOME/.dotnet/tools
export DOTNET_CLI_TELEMETRY_OPTOUT=1
cd "$dir"
for strat in "--sch-random" "--sch-pct 5" "--sch-pos"; do
  s=$(date +%s)
  out=$(p check -tc "$tc" -s "$N" $strat --max-steps 5000 -o "PCheckerOutput/deep-$tc" 2>&1)
  bugs=$(echo "$out" | grep -oE 'Found [0-9]+ bugs?' | awk '{n+=$2} END {print n+0}')
  sched=$(echo "$out" | grep -oE 'Explored [0-9]+ schedules' | awk '{n+=$2} END {print n+0}')
  echo "$(basename $dir) $tc $strat: $bugs bug(s) in $sched schedules, $(( $(date +%s)-s ))s"
done

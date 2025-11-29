#!/bin/bash
set -e

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit
fi

pname=$1
outfile=$2
duration=${3:-10}

echo "pname=$pname outfile=$outfile duration=$duration"

perf record -F 99 -p "$(pgrep -n $pname)" -g -- sleep $duration
echo "script"
perf script > $outfile.perf

#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <process_name>" >&2
    exit 1
fi

PROC_NAME="$1"
INTERVAL=1

# Wait until process appears
while ! pgrep -x "$PROC_NAME" > /dev/null; do
    sleep 1
done

# Get most recent matching PID
PARENT_PID=$(pgrep -nx "$PROC_NAME")
PARENT_NAME=$(ps -p "$PARENT_PID" -o comm=)

# Print CSV header
echo "timestamp,parent_pid,parent_name,child_type,child_count"

while ps -p "$PARENT_PID" > /dev/null 2>&1; do
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

    # Get thread names (skip header)
    THREAD_NAMES=$(ps -T -p "$PARENT_PID" -o comm= | tail -n +2)

    if [ -n "$THREAD_NAMES" ]; then
        echo "$THREAD_NAMES" \
            | awk -v ts="$TIMESTAMP" -v pid="$PARENT_PID" -v pname="$PARENT_NAME" '
                { count[$0]++ }
                END {
                    for (name in count) {
                        printf "%s,%s,%s,%s,%d\n", ts, pid, pname, name, count[name]
                    }
                }'
    else
        echo "$TIMESTAMP,$PARENT_PID,$PARENT_NAME,NONE,0"
    fi

    sleep "$INTERVAL"
done


#!/bin/bash

if [ $# -lt 1 ]; then
    echo "$0 <cmd-to-run>"
    exit 1
fi

while :; do
    clear
    ts=$(date +%s%N)
    eval "${@}" 
    ms=$((($(date +%s%N) - ${ts})/1000000))
    tput bold; echo -e -n "\nExit code: ${?}"; tput sgr0
    echo -e -n "\t\ttime: ${ms}ms"
    inotifywait -q -r -e close_write $(pwd) || break
    sleep 1.0
done

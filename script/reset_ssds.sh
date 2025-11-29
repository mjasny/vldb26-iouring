#!/bin/bash

set -euo pipefail
DEV=${1:-/dev/nvme1n1}      # whole controller, not nvme0n1
SANACT=2                  # 2 = block-erase, 1 = crypto-erase, 4 = overwrite

if [ ! -b "$DEV" ]; then
    echo "Device $DEV does not exist, skipping."
    exit 1 
fi

if mount | grep -q "$DEV"; then
    echo "Device $DEV is mounted, skipping."
    exit 1 
fi

sudo nvme sanitize --sanact="$SANACT" "$DEV"

echo "Waiting for sanitize to finish..."
while true; do
    read -r sstat sprog <<<"$(sudo nvme sanitize-log "$DEV" \
        | awk -F':' '
              /Sanitize Status/   {gsub(/^[ \t]+/,"",$2); ss=$2}
              /Sanitize Progress/ {gsub(/^[ \t]+/,"",$2); sp=$2}
              END {print ss, sp}')"

    # progress is a 0-65535 counter; convert to percent
    prog_pct=$(awk "BEGIN{printf \"%.1f\", ($sprog/65535)*100}")

    if [[ $sstat == 0x* ]]; then
        sstat_val=$(( 16#${sstat#0x} ))
    else
        sstat_val=$(( sstat ))
    fi

    gde=$(( (sstat_val & 0x100) >> 8 ))   # bit 8 → Global Data Erased flag
    code=$((  sstat_val & 0x0FF ))        # low-byte status code (0-3 today)

    # https://manpages.debian.org/testing/nvme-cli/nvme-sanitize-log.1.en.html
    case $code in
        0) code_txt="never sanitized" ;;
        1) code_txt="completed successfully" ;;
        2) code_txt="in progress" ;;
        3) code_txt="failed" ;;
        *) code_txt="reserved/unknown" ;;
    esac
    [[ $gde -eq 1 ]] && code_txt+=" (GDE=1)"

    #printf "\rSSTAT=%s  progress=%s%%  [%s] " "$sstat" "$prog_pct" "$code_txt"
    printf "SSTAT=%s  progress=%s%%  [%s] \n" "$sstat" "$prog_pct" "$code_txt"

    # --- decide what to do -----------------------------------------------------
    case $code in
        2)  sleep 1 ;;                               # still running
        1)  echo -e "\n✔ sanitize completed OK";  break ;;
        0)  if (( gde )); then
            echo -e "\n✔ drive is already blank (Global Data Erased bit set)"
            exit 0
        else
            echo -e "\n⚠ sanitize never started";  exit 1
            fi ;;
        3)  echo -e "\n✘ sanitize failed";             exit 2 ;;
        *)  echo -e "\n✘ unknown sanitize status";     exit 3 ;;
    esac
done


echo "blkdiscard $DEV..."
#sudo blkdiscard "$DEV"
MAX_RETRIES=3
SLEEP_SEC=2
for attempt in $(seq 1 $MAX_RETRIES); do
    if sudo blkdiscard "$DEV"; then
        break
    else
        echo "blkdiscard failed on attempt $attempt"
        if [ "$attempt" -lt "$MAX_RETRIES" ]; then
            echo "Retrying in $SLEEP_SEC seconds..."
            sleep $SLEEP_SEC
        else
            echo "blkdiscard failed after $MAX_RETRIES attempts"
            exit 1
        fi
    fi
done
echo "blkdiscard finished."


echo "TODO: Pre-Write SSD if you have read-only workloads"


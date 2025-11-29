#!/bin/bash

set -euo pipefail
DEV=${1:-/dev/nvme1n1}      # whole controller, not nvme0n1


if [ ! -b "$DEV" ]; then
    echo "Device $DEV does not exist, skipping."
    exit 1 
fi

if mount | grep -q "$DEV"; then
    echo "Device $DEV is mounted, skipping."
    exit 1 
fi


echo "Starting write to $DEV..."
sudo dd if=/dev/zero of="$DEV" bs=1G status=progress oflag=direct #count=2048


echo "Writes done"



#!/usr/bin/env bash
# delete-ntuple-rules.sh
#
# Deletes all ntuple rules for a given interface (default: ens3np0)
# First verifies that ntuple is enabled.

set -euo pipefail

iface="${1:-ens3np0}"

# Check if ntuple is on
if ! sudo ethtool -k "$iface" | grep -q 'ntuple.*on'; then
    sudo ethtool -K "$iface" ntuple on
    echo "ntuple was not enabled on interface '$iface'. Enabled now."
    exit 0
fi

echo "Deleting ntuple rules for interface '$iface'..."

# List and delete each filter
sudo ethtool -n "$iface" | awk '/Filter:/ {print $2}' | while read -r id; do
    echo "Deleting filter ID $id..."
    sudo ethtool -N "$iface" delete "$id"
done

echo "Done."


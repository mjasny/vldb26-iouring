#!/bin/bash
set -euo pipefail
set -x


if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <setup|teardown> <iface>"
    exit 1
fi

action="$1"
iface="$2"
setup() {
    sudo ethtool -K "$iface" rx-gro-hw on
    sudo ethtool -G "$iface" tcp-data-split on
    sudo ethtool -X "$iface" equal 1

    echo "Setup complete."
}

teardown() {
    #sudo ethtool -K "$iface" rx-gro-hw off
    sudo ethtool -X "$iface" default
    sudo ethtool -G "$iface" tcp-data-split auto

    echo "Teardown complete."
}

case "$action" in
    setup)
        setup
        ;;
    teardown)
        teardown
        ;;
    *)
        echo "Unknown action: $action"
        echo "Usage: $0 <setup|teardown> <iface>"
        exit 1
        ;;
esac


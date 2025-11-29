#!/bin/bash

# Usage: ./run-server.sh user@host

set -e

REMOTE_HOST="$1"

if [ -z "$REMOTE_HOST" ]; then
    echo "Usage: $0 user@host"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_DISTEXPRUNNER="$SCRIPT_DIR/../distexprunner"

if [ ! -d "$LOCAL_DISTEXPRUNNER" ]; then
    echo "Error: distexprunner directory not found at $LOCAL_DISTEXPRUNNER"
    exit 1
fi

echo "Syncing distexprunner/ to $REMOTE_HOST:~/distexprunner"
rsync -avPz --delete "$LOCAL_DISTEXPRUNNER/" "$REMOTE_HOST:~/distexprunner/"

echo "Launching distexp-server on $REMOTE_HOST"
ssh -t "$REMOTE_HOST" "
  set -e

  if [ -f \$HOME/.nix-profile/etc/profile.d/nix.sh ]; then
      . \$HOME/.nix-profile/etc/profile.d/nix.sh
  elif [ -f /etc/profile.d/nix.sh ]; then
      . /etc/profile.d/nix.sh
  elif ! command -v nix-shell >/dev/null 2>&1; then
      echo 'Error: nix-shell not found.'
      exit 1
  fi

  cd ~/distexprunner
  screen -wipe || true

  if screen -list | grep -q distexp-server; then
      echo 'Reattaching to existing screen session: distexp-server'
      screen -r distexp-server
  else
      echo 'Starting new screen session: distexp-server'
      nix-shell shell.nix --run \"screen -S distexp-server bash -c 'distexp-server -vv -rf || exec bash'\"
  fi
"

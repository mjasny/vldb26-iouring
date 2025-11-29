#!/usr/bin/env bash
set -e
#
# sudo mdadm --detail /dev/md0 => Chunk Size
# sudo mdadm --create /dev/md0 --level=0 --raid-devices=4 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1 /dev/nvme5n1
# sudo mkfs.xfs -d su=512k,sw=4 /dev/md0
#
# sudo umount /mnt/raid
# sudo mdadm --stop /dev/md0
# sudo mdadm --create /dev/md0 --level=0 --raid-devices=7 /dev/nvme{2,3,4,5,6,7,8}n1 
# sudo mkfs.xfs -f -d su=512k,sw=7 /dev/md0
#
# sudo mkdir -p /mnt/raid
# sudo mount -o noatime,nodiratime /dev/md0 /mnt/raid
# sudo chmod 777 /mnt/raid/
# rsync -a --info=progress2 pgdata/ /mnt/raid/pgdata/
#
# sudo mkfs.ext4 -E stride=128,stripe-width=896 /dev/md0
# stride = chunk size / block size = 512KB / 4KB = 128 stripe-width = stride x number of disks = 128 x 7 = 896
# sudo mount -o noatime,nodiratime /dev/md0 /mnt/raid

# sudo mkfs.ext4 /dev/nvme2n1
# sudo mount -o noatime,nodiratime /dev/nvme2n1 /mnt/raid



set -euo pipefail

############################################################################
# Adjust these constants once for your environment
############################################################################
PG_ROOT=$(readlink -f "../../postgres/build/install/usr/local/pgsql")      # pg_ctl, psql
PGDATA="$PWD/pgdata"                 # data directory
CONF_FILE="$PGDATA/postgresql.conf"            # full path to postgresql.conf
CSV_FILE="results2.csv"                         # output file (in CWD)


if [[ "$(hostname)" != "x1" ]]; then
  PGDATA="/mnt/raid/pgdata"
  CONF_FILE="$PGDATA/postgresql.conf"    
fi

echo "PGDATA: $PGDATA"

############################################################################
# Derived paths - do NOT edit below this line unless you know what you're doing
############################################################################
export LD_LIBRARY_PATH="$PG_ROOT/lib:$PG_ROOT/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"
PG_CTL="$PG_ROOT/bin/pg_ctl"
PSQL="$PG_ROOT/bin/psql"

############################################################################
# Input parameters (validated lightly)
############################################################################
if (( $# != 5 )); then
  echo "Usage: $0 <io_method> <iodirect_flag> <tag> <max-workers>" >&2
  exit 1
fi
IO_METHOD="$1"            # e.g. io_uring
IO_DIRECT="$2"            # e.g. off  OR  'data,wal'
TAG="$3"
MAX_WORKERS="$4"
IO_DEPTH="$5"

############################################################################
# 1. Stop the Postgres server
############################################################################

function stop_server() {
    echo "Stopping PostgreSQL..."
    if "$PG_CTL" -D "$PGDATA" -U postgres status >/dev/null 2>&1; then
      # Instance is up -> stop it and let set -e abort on errors
      "$PG_CTL" -D "$PGDATA"  -U postgres -l logfile stop
    else
      # Instance is already down -> just note it and move on
      echo "PostgreSQL is not running - nothing to stop."
    fi
}

stop_server


############################################################################
# 2. Update postgresql.conf
############################################################################

if [ ! -d "$PGDATA" ]; then
    echo "Initializing PostgreSQL data directory at $PGDATA..."
    $PG_ROOT/bin/initdb -D "$PGDATA" --username=postgres || {
        echo "Error: Failed to initialize the data directory."
        exit 1
    }
    echo "Database initialized."
fi

echo "Updating $CONF_FILE (io_method=$IO_METHOD, debug_io_direct=$IO_DIRECT)..."

# Ensure the lines exist (append if missing), then edit in-place with sed
grep -q '^debug_io_direct' "$CONF_FILE" || echo "debug_io_direct = ''" | tee -a "$CONF_FILE" >/dev/null
grep -q '^io_method'       "$CONF_FILE" || echo "io_method = sync"   | tee -a "$CONF_FILE" >/dev/null
grep -q '^max_parallel_workers_per_gather' "$CONF_FILE" || echo "max_parallel_workers_per_gather = 0" | tee -a "$CONF_FILE" >/dev/null
grep -q '^effective_io_concurrency' "$CONF_FILE" || echo "effective_io_concurrency = 16" | tee -a "$CONF_FILE" >/dev/null

# Replace values - quotes are required for debug_io_direct unless it is 'off'
if [[ "$IO_DIRECT" == "off" ]]; then
  sed -Ei "s|^debug_io_direct\s*=.*|debug_io_direct = ''|" "$CONF_FILE"
else
  sed -Ei "s|^debug_io_direct\s*=.*|debug_io_direct = '$IO_DIRECT'|" "$CONF_FILE"
fi
sed -Ei "s|^io_method\s*=.*|io_method = $IO_METHOD|" "$CONF_FILE"

sed -Ei "s|^max_parallel_workers_per_gather\s*=.*|max_parallel_workers_per_gather = $MAX_WORKERS|" "$CONF_FILE"

sed -Ei "s|^effective_io_concurrency\s*=.*|effective_io_concurrency = $IO_DEPTH|" "$CONF_FILE"


############################################################################
# 3 & 4. Drop the page cache (needs root)
############################################################################
echo "Dropping Linux page cache..."
sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'

############################################################################
# 5. Start the Postgres server
############################################################################
echo "Starting PostgreSQL..."
"$PG_CTL" -D "$PGDATA" -U postgres -l logfile start

echo "Waiting for PostgreSQL to become ready..."
tries=0
while ! $PG_ROOT/bin/pg_isready -U postgres -q ; do #-D "$PGDATA"
  if [ "$tries" -ge 30 ]; then
    echo "PostgreSQL failed to start within timeout."
    exit 1
  fi
  sleep 0.5
  tries=$((tries + 1))
done



echo "Ensuring test table exists..."

#NUM_ROWS=100000000 # 3458 MB
NUM_ROWS=1000000000 # 34 GB

TABLE_EXISTS=$("$PSQL" -h /tmp -U postgres -d postgres -tAc "SELECT 1 FROM pg_tables WHERE tablename = 'test';")
if [ "$TABLE_EXISTS" != "1" ]; then
  echo "'test' table not found - creating and populating it with $NUM_ROWS rows..."
  "$PSQL" -h /tmp -U postgres -d postgres <<SQL
CREATE TABLE test(id int);
INSERT INTO test SELECT * FROM generate_series(0, $NUM_ROWS);
SQL
else
  echo "'test' table already exists."
fi

"$PSQL" -h /tmp -U postgres -d postgres <<'SQL'
\dt+
SHOW io_method;
SHOW debug_io_direct;
SQL

#read -p "Press Enter"

############################################################################
# 6. Run the benchmark query and capture timing
############################################################################
echo "Running benchmark query..."
# Capture the timing line (e.g. "Time: 1052.991 ms ...")
QUERY="SELECT COUNT(*) FROM test;"
#QUERY="SELECT SUM(id) FROM test;"

QUERY_OUTPUT=$("$PSQL" -h /tmp -U postgres -d postgres -c "\timing on" -c "$QUERY")
echo $QUERY_OUTPUT
TIME_MS=$(echo "$QUERY_OUTPUT" | grep -oP 'Time:\s+\K[0-9.]+(?=\s+ms)' | head -1)

############################################################################
# 7 & 8. Append to CSV
############################################################################
echo "Recording result..."
if [[ ! -f $CSV_FILE ]]; then
  echo "tag,method,iodirect,ms" > "$CSV_FILE"
fi

stop_server

# -- normalise IO_DIRECT for CSV -------------------------------------------
# Strip *all* whitespace characters; if nothing remains, we call it "off"
if [[ -z "${IO_DIRECT//[[:space:]]/}" ]]; then     # '', " ", "\t", etc.
  IO_DIRECT_CSV="off"
else
  IO_DIRECT_CSV="${IO_DIRECT//,/+}"     # commas -> plus
fi

echo "$TAG,$IO_METHOD,$IO_DIRECT_CSV,$TIME_MS" >> "$CSV_FILE"

echo "Done - recorded $TIME_MS ms"
echo "Results accumulated in $(realpath "$CSV_FILE")"


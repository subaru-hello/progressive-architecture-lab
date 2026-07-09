#!/bin/bash
# Entrypoint for db-replica container.
# We bypass the official postgres entrypoint, so we must drop to the `postgres`
# user ourselves (postgres refuses to run as root) and own PGDATA before use.
# If PGDATA is empty, run pg_basebackup from db-primary to initialize the standby.
# The -R flag writes standby.signal and primary_conninfo automatically.
# On subsequent restarts (PGDATA already populated), skip straight to postgres.
set -e

# Anonymous volume mounts as root:root — postgres user needs to own it.
mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

# Wait until primary is accepting connections before attempting basebackup.
until pg_isready -h db-primary -U replicator -q; do
  echo "replica: waiting for db-primary to be ready..."
  sleep 2
done

if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
  echo "replica: PGDATA is empty — running pg_basebackup from db-primary"
  gosu postgres pg_basebackup \
    -R \
    -h db-primary \
    -U replicator \
    -D "$PGDATA" \
    --checkpoint=fast \
    --wal-method=stream
  echo "replica: basebackup complete, starting standby"
else
  echo "replica: PGDATA already populated, starting standby"
fi

exec gosu postgres postgres

#!/bin/bash
# Runs inside db-primary container via /docker-entrypoint-initdb.d/
# Creates the replication role and adds pg_hba entry for streaming replication.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '${REPL_PASSWORD}';
EOSQL

# Allow the replicator role to connect for replication from any container in the compose network.
# Using scram-sha-256 to match postgres 16 defaults.
echo "host replication replicator 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"

pg_ctl reload -D "$PGDATA"

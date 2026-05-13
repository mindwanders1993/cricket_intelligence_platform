#!/bin/sh
# Generates a libpq passfile so pgAdmin can auto-connect to PostgreSQL
# without prompting the user. Reads POSTGRES_PASSWORD from the container env.
#
# Format: hostname:port:database:username:password  (libpq spec)
set -e

PGPASS_PATH=/pgpassfile

echo "postgres:5432:*:postgres:${POSTGRES_PASSWORD}" > "${PGPASS_PATH}"
chmod 600 "${PGPASS_PATH}"
chown pgadmin:root "${PGPASS_PATH}" 2>/dev/null || true

exec /entrypoint.sh "$@"

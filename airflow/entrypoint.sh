#!/usr/bin/env bash
set -e

# 1) Initialize the metadata DB (no-op if already done)
airflow db init

# 2) Create the admin user if it doesn’t exist yet
airflow users create \
  --username admin \
  --firstname Taylor \
  --lastname Merwin \
  --role Admin \
  --email change-email@example.com \
  --password ChangeMe123 \
  || true

# 3) Hand off to the original Airflow entrypoint
exec /entrypoint "$@"
#!/usr/bin/env bash
set -e


airflow db init


airflow users create \
  --username admin \
  --firstname Taylor \
  --lastname Merwin \
  --role Admin \
  --email change-email@example.com \
  --password ChangeMe123 \
  || true


exec /entrypoint "$@"
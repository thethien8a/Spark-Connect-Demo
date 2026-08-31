#!/bin/sh
set -eu

: "${SOURCE_APP_USER:?SOURCE_APP_USER is required}"
: "${SOURCE_APP_PASSWORD:?SOURCE_APP_PASSWORD is required}"
: "${DEBEZIUM_USER:?DEBEZIUM_USER is required}"
: "${DEBEZIUM_PASSWORD:?DEBEZIUM_PASSWORD is required}"

psql \
  -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=app_user="$SOURCE_APP_USER" \
  --set=app_password="$SOURCE_APP_PASSWORD" \
  --set=cdc_user="$DEBEZIUM_USER" \
  --set=cdc_password="$DEBEZIUM_PASSWORD" <<'SQL'
SELECT format(
    'CREATE ROLE %I WITH LOGIN PASSWORD %L',
    :'app_user',
    :'app_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'app_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN PASSWORD %L',
    :'app_user',
    :'app_password'
)
\gexec

SELECT format(
    'CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
    :'cdc_user',
    :'cdc_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
    :'cdc_user',
    :'cdc_password'
)
\gexec

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO %I',
    current_database(),
    :'app_user'
)
\gexec

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO %I',
    current_database(),
    :'cdc_user'
)
\gexec

SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'app_user')
\gexec

SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'cdc_user')
\gexec

SELECT format(
    'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %I',
    :'app_user'
)
\gexec

SELECT format(
    'GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I',
    :'cdc_user'
)
\gexec

SELECT format(
    'GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO %I',
    :'app_user'
)
\gexec

SELECT format('GRANT USAGE ON TYPE public.order_status TO %I', :'app_user')
\gexec

SELECT format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
    || 'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I',
    CURRENT_USER,
    :'app_user'
)
\gexec

SELECT format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
    || 'GRANT SELECT ON TABLES TO %I',
    CURRENT_USER,
    :'cdc_user'
)
\gexec

SELECT format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
    || 'GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO %I',
    CURRENT_USER,
    :'app_user'
)
\gexec
SQL

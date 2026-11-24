-- Read-only role for the Grafana dashboard. It can never write to the output DB.
-- The password is a local-dev default; override it in production.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro';
    END IF;
END
$$;
GRANT CONNECT ON DATABASE compliance TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;

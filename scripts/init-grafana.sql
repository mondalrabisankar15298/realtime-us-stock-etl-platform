-- Grafana Database Initialization
-- Since POSTGRES_DB=grafana, we're already in the grafana database
-- Just create the user and set permissions

-- Create grafana user if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana') THEN
        CREATE USER grafana WITH PASSWORD 'grafana';
    END IF;
END $$;

-- Grant schema privileges in the current database (grafana)
GRANT ALL ON SCHEMA public TO grafana;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO grafana;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO grafana;

-- Grant connect privilege on the database
GRANT CONNECT ON DATABASE grafana TO grafana;

-- Also grant connect privilege on stockdata database for Grafana datasource
GRANT CONNECT ON DATABASE stockdata TO grafana;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Grafana database initialized successfully!';
    RAISE NOTICE '🔗 Grafana user granted access to stockdata database';
END $$;

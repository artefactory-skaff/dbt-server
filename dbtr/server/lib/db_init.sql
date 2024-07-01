CREATE TABLE IF NOT EXISTS Lock (
    holder TEXT,
    created_at REAL,
    updated_at REAL,
    run_id TEXT
);

CREATE TABLE IF NOT EXISTS Runs (
    run_id TEXT PRIMARY KEY,
    run_status TEXT,
    start_time REAL,
    end_time REAL
);

CREATE TABLE IF NOT EXISTS RunConfiguration (
    run_id TEXT PRIMARY KEY,
    run_conf_version INTEGER,
    project TEXT,
    server_url TEXT,
    cloud_provider TEXT,
    provider_config TEXT,
    requester TEXT,
    dbt_runtime_config TEXT,
    schedule_cron TEXT,
    schedule_name TEXT,
    schedule_description TEXT
);

-- Time Travel / Snapshot Queries

-- Delta Lake: Query as of specific version
SELECT count(*) AS total_trips
FROM demo_delta.yellow_trips_silver VERSION AS OF 0;

-- Delta Lake: Query as of timestamp
SELECT count(*) AS total_trips
FROM demo_delta.yellow_trips_silver TIMESTAMP AS OF '2025-01-01 00:00:00';

-- Delta Lake: View table history
DESCRIBE HISTORY demo_delta.yellow_trips_silver;


-- Iceberg: Query as of snapshot ID
SELECT count(*) AS total_trips
FROM demo_iceberg.yellow_trips_silver VERSION AS OF 1234567890;

-- Iceberg: Query as of timestamp
SELECT count(*) AS total_trips
FROM demo_iceberg.yellow_trips_silver FOR SYSTEM_TIME AS OF '2025-01-01 00:00:00';

-- Iceberg: View table snapshots
SELECT * FROM demo_iceberg.yellow_trips_silver.snapshots;

-- Iceberg: View table history
SELECT * FROM demo_iceberg.yellow_trips_silver.history;


-- Hudi: Query as of specific commit time (limited support)
-- Hudi time travel via query options:
-- SELECT * FROM hudi_table WHERE _hoodie_commit_time <= '20250101000000'
SELECT count(*) AS total_trips
FROM demo_hudi.yellow_trips_silver
WHERE _hoodie_commit_time <= '20250101000000';


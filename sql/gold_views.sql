-- Gold layer views aggregating silver tables per format

-- Delta Lake
CREATE OR REPLACE VIEW demo_delta.v_trips AS
SELECT 
    PULocationID, 
    date_trunc('month', tpep_pickup_datetime) AS month,
    avg(trip_distance) AS avg_dist, 
    avg(total_amount) AS avg_total
FROM demo_delta.yellow_trips_silver
GROUP BY 1, 2;

-- Apache Iceberg
CREATE OR REPLACE VIEW demo_iceberg.v_trips AS
SELECT 
    PULocationID, 
    date_trunc('month', tpep_pickup_datetime) AS month,
    avg(trip_distance) AS avg_dist, 
    avg(total_amount) AS avg_total
FROM demo_iceberg.yellow_trips_silver
GROUP BY 1, 2;

-- Apache Hudi
CREATE OR REPLACE VIEW demo_hudi.v_trips AS
SELECT 
    PULocationID, 
    date_trunc('month', tpep_pickup_datetime) AS month,
    avg(trip_distance) AS avg_dist, 
    avg(total_amount) AS avg_total
FROM demo_hudi.yellow_trips_silver
GROUP BY 1, 2;


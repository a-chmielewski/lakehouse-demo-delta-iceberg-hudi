-- Gold layer views aggregating silver tables per format
-- Athena-compatible syntax with quoted identifiers for case-sensitive columns

-- Delta Lake
CREATE OR REPLACE VIEW delta_demo.v_trips AS
SELECT 
    "PULocationID", 
    DATE_TRUNC('month', "tpep_pickup_datetime") AS month,
    AVG("trip_distance") AS avg_dist, 
    AVG("total_amount") AS avg_total
FROM delta_demo.yellow_trips_silver
GROUP BY "PULocationID", DATE_TRUNC('month', "tpep_pickup_datetime");

-- Apache Iceberg
CREATE OR REPLACE VIEW iceberg_demo.v_trips AS
SELECT 
    "PULocationID", 
    DATE_TRUNC('month', "tpep_pickup_datetime") AS month,
    AVG("trip_distance") AS avg_dist, 
    AVG("total_amount") AS avg_total
FROM iceberg_demo.yellow_trips_silver
GROUP BY "PULocationID", DATE_TRUNC('month', "tpep_pickup_datetime");

-- Apache Hudi
CREATE OR REPLACE VIEW hudi_demo.v_trips AS
SELECT 
    "PULocationID", 
    DATE_TRUNC('month', "tpep_pickup_datetime") AS month,
    AVG("trip_distance") AS avg_dist, 
    AVG("total_amount") AS avg_total
FROM hudi_demo.yellow_trips_silver
GROUP BY "PULocationID", DATE_TRUNC('month', "tpep_pickup_datetime");


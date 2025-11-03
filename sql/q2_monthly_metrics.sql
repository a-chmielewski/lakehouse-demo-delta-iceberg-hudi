SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       avg(trip_distance) AS avg_dist,
       avg(total_amount) AS avg_total,
       count(*) AS n
FROM demo_delta.yellow_trips_silver
GROUP BY 1
ORDER BY 1;

SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       avg(trip_distance) AS avg_dist,
       avg(total_amount) AS avg_total,
       count(*) AS n
FROM demo_iceberg.yellow_trips_silver
GROUP BY 1
ORDER BY 1;

SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       avg(trip_distance) AS avg_dist,
       avg(total_amount) AS avg_total,
       count(*) AS n
FROM demo_hudi.yellow_trips_silver
GROUP BY 1
ORDER BY 1;

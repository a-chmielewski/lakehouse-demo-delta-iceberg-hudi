SELECT PULocationID, count(*) AS trips
FROM demo_delta.yellow_trips_silver
GROUP BY 1
ORDER BY trips DESC
LIMIT 10;

SELECT PULocationID, count(*) AS trips
FROM demo_iceberg.yellow_trips_silver
GROUP BY 1
ORDER BY trips DESC
LIMIT 10;

SELECT PULocationID, count(*) AS trips
FROM demo_hudi.yellow_trips_silver
GROUP BY 1
ORDER BY trips DESC
LIMIT 10;

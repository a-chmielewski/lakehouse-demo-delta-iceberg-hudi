import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

bucket = os.environ["S3_BUCKET"]
bronze = f"s3a://{bucket}/{os.environ['BRONZE_PREFIX']}/tlc/yellow/"
glue_db = os.environ["GLUE_DB_ICEBERG"]
table = f"glue.{glue_db}.yellow_trips_silver"   # Iceberg catalog named 'glue'

spark = (SparkSession.builder
    .appName("iceberg_silver")
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue","org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue.warehouse", f"s3a://{bucket}/warehouse/iceberg")
    .config("spark.sql.catalog.glue.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate())

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue.{glue_db}")

# Create table with partition transforms + sort order
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table} (
  VendorID INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
  passenger_count INT, trip_distance DOUBLE, RatecodeID INT, store_and_fwd_flag STRING,
  PULocationID INT, DOLocationID INT, payment_type INT, fare_amount DOUBLE, extra DOUBLE,
  mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE,
  total_amount DOUBLE, congestion_surcharge DOUBLE
)
USING ICEBERG
PARTITIONED BY (years(tpep_pickup_datetime), months(tpep_pickup_datetime))
TBLPROPERTIES('write.upsert.enabled'='true')
""")

bronze_df = spark.read.parquet(bronze)
bronze_df.createOrReplaceTempView("bronze")

# Upsert via MERGE (Spark 3.5 + Iceberg)
spark.sql(f"""
MERGE INTO {table} t
USING (
  SELECT
    *, concat(PULocationID, '_', cast(tpep_pickup_datetime as string)) as trip_id
  FROM bronze
) s
ON t.PULocationID = s.PULocationID AND t.tpep_pickup_datetime = s.tpep_pickup_datetime
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Example schema evolution: add nullable column
spark.sql(f"ALTER TABLE {table} ADD COLUMN payment_type_desc STRING")

spark.stop()

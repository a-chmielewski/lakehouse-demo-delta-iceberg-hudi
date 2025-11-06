import os
from pyspark.sql import SparkSession

# --- Config from environment (set via spark.emr-serverless.*Env.*) ---
bucket   = os.environ["S3_BUCKET"]
bronze   = f"s3a://{bucket}/{os.environ['BRONZE_PREFIX']}/tlc/yellow/"
glue_db  = os.environ["GLUE_DB_ICEBERG"]               # e.g. "iceberg_demo"
catalog  = "glue"                                      # Iceberg catalog name
table_id = f"{catalog}.{glue_db}.yellow_trips_silver"  # e.g. glue.iceberg_demo.yellow_trips_silver

# --- Spark session with Iceberg catalog ---
spark = (
    SparkSession.builder
        .appName("iceberg_silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", f"s3a://{bucket}/silver/iceberg/warehouse")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
)

# ----------------------------------------------------------------------
# 1) Ensure namespace exists — without IF NOT EXISTS
# ----------------------------------------------------------------------
try:
    spark.sql(f"CREATE NAMESPACE {catalog}.{glue_db}")
except Exception as e:
    if "already exists" not in str(e).lower():
        raise

# ----------------------------------------------------------------------
# 2) Create table once (schema matches NYC Yellow Taxi bronze)
# ----------------------------------------------------------------------
create_sql = f"""
CREATE TABLE {table_id} (
  VendorID INT,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INT,
  trip_distance DOUBLE,
  RatecodeID INT,
  store_and_fwd_flag STRING,
  PULocationID INT,
  DOLocationID INT,
  payment_type INT,
  fare_amount DOUBLE,
  extra DOUBLE,
  mta_tax DOUBLE,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  improvement_surcharge DOUBLE,
  total_amount DOUBLE,
  congestion_surcharge DOUBLE,
  Airport_fee DOUBLE,
  cbd_congestion_fee DOUBLE
)
USING ICEBERG
PARTITIONED BY (months(tpep_pickup_datetime))
TBLPROPERTIES (
  'write.upsert.enabled' = 'true',              -- harmless for overwrite path; enables MERGE later if you want
  'format-version' = '2'                        -- v2 tables recommended
)
"""
try:
    spark.sql(create_sql)
except Exception as e:
    # Tolerate table already created
    if "already exists" not in str(e).lower():
        raise

# ----------------------------------------------------------------------
# 3) Read bronze and materialize a view; select only the columns we expect
# ----------------------------------------------------------------------
bronze_df = spark.read.parquet(bronze).select(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
)

bronze_df = bronze_df.filter("tpep_pickup_datetime IS NOT NULL")

bronze_df.createOrReplaceTempView("bronze_selected")

# ----------------------------------------------------------------------
# 4) Load into Iceberg silver (full refresh)
# ----------------------------------------------------------------------
spark.sql(f"""
INSERT OVERWRITE {table_id}
SELECT
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  Airport_fee,
  cbd_congestion_fee
FROM bronze_selected
""")

# ----------------------------------------------------------------------
# 5) Example schema evolution (tolerate "already exists")
# ----------------------------------------------------------------------
try:
    spark.sql(f"ALTER TABLE {table_id} ADD COLUMN payment_type_desc STRING")
except Exception as e:
    if "already exists" not in str(e).lower():
        raise

spark.stop()

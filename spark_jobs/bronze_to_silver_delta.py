import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from delta.tables import DeltaTable  # type: ignore
from pyspark.sql.utils import AnalysisException

bucket = os.environ["S3_BUCKET"]
bronze = f"s3a://{bucket}/{os.environ['BRONZE_PREFIX']}/tlc/yellow/"
silver_loc = f"s3a://{bucket}/{os.environ['SILVER_PREFIX']}/delta/yellow/"
glue_db = os.environ["GLUE_DB_DELTA"]
table = f"{glue_db}.yellow_trips_silver"
warehouse = f"s3a://{bucket}/warehouse/delta"  # warehouse base for managed DBs

spark = (
    SparkSession.builder
    .appName("delta_silver")
    # Delta integration
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Point Spark's Hive catalog at AWS Glue
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    # Put warehouse on S3 (so CREATE DATABASE without LOCATION won’t fall back to file:)
    .config("spark.sql.warehouse.dir", warehouse)
    .getOrCreate()
)

# 1) Read bronze parquet recursively
df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(bronze)
)

# 2) Synthetic key (demo)
df = df.withColumn(
    "trip_id",
    expr("concat_ws('-', cast(PULocationID as string), cast(tpep_pickup_datetime as string), cast(rand()*1e6 as int))")
)

def ensure_delta_path_and_table():
    # Create the Glue database (explicit LOCATION on S3)
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {glue_db} "
        f"LOCATION '{warehouse}/{glue_db}.db'"
    )

    # Create the Delta table at path if it's not initialized yet
    path_has_delta = True
    try:
        _ = DeltaTable.forPath(spark, silver_loc)
    except Exception:
        path_has_delta = False

    if not path_has_delta:
        (
            df.limit(0)
              .write
              .format("delta")
              .mode("overwrite")
              .save(silver_loc)
        )

    # Register (idempotent) the external Delta table in Glue at that path
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} "
        f"USING delta LOCATION '{silver_loc}'"
    )

ensure_delta_path_and_table()

# 3) Upsert (MERGE)
delta_tbl = DeltaTable.forPath(spark, silver_loc)
(delta_tbl.alias("t")
 .merge(df.alias("b"), "t.trip_id = b.trip_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

# 4) Example delete (privacy)
delta_tbl.delete("total_amount < 0")

spark.stop()

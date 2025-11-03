import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from deltalake import DeltaTable

bucket = os.environ["S3_BUCKET"]
bronze = f"s3a://{bucket}/{os.environ['BRONZE_PREFIX']}/tlc/yellow/"
silver_loc = f"s3a://{bucket}/{os.environ['SILVER_PREFIX']}/delta/yellow/"
glue_db = os.environ["GLUE_DB_DELTA"]
table = f"{glue_db}.yellow_trips_silver"

spark = (SparkSession.builder
         .appName("delta_silver")
         .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

df = spark.read.parquet(bronze)
# Create a synthetic key for demo
df = df.withColumn("trip_id", expr("concat_ws('-', cast(PULocationID as string), cast(tpep_pickup_datetime as string), cast(rand()*1e6 as int))"))

# Create Delta if missing
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table}
USING delta
LOCATION '{silver_loc}'
AS SELECT * FROM (
  SELECT * FROM parquet.`{bronze}`
) WHERE 1=0
""")

# Upsert (MERGE) – simulate late updates by just merging the whole bronze again
delta_tbl = DeltaTable.forPath(spark, silver_loc)
(delta_tbl.alias("t")
 .merge(df.alias("b"), "t.trip_id = b.trip_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

# Example delete (privacy) – drop negative fares
delta_tbl.delete("total_amount < 0")

# Optimize & vacuum can be run in maintenance job; stop here
spark.stop()

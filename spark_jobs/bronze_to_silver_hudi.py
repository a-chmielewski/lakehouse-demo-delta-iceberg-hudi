import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, year, month, col

bucket = os.environ["S3_BUCKET"]
bronze = f"s3a://{bucket}/{os.environ['BRONZE_PREFIX']}/tlc/yellow/"
silver = f"s3a://{bucket}/{os.environ['SILVER_PREFIX']}/hudi/yellow/"
glue_db = os.environ["GLUE_DB_HUDI"]
table = "yellow_trips_silver"

spark = (
    SparkSession.builder
    .appName("hudi_silver")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# Read bronze (all partitions)
df = (spark.read
        .option("recursiveFileLookup", "true")
        .parquet(bronze))

# Ensure timestamps are zone-aware 'timestamp' (Hudi/Avro doesn’t support TimestampNTZ)
df = (df
      .withColumn("tpep_pickup_datetime",  col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
      # rebuild partitions if missing
      .withColumn("pickup_year",  year(col("tpep_pickup_datetime")))
      .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
)

# Synthetic key
df = df.withColumn(
    "trip_id",
    expr("concat_ws('_', cast(PULocationID as string), cast(tpep_pickup_datetime as string))")
)

# Hudi options — NOTE: hive_style_partitioning is FALSE to match existing table config
hudi_options = {
    "hoodie.table.name": table,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",

    "hoodie.datasource.write.recordkey.field": "trip_id",
    "hoodie.datasource.write.precombine.field": "tpep_pickup_datetime",
    "hoodie.datasource.write.partitionpath.field": "pickup_year,pickup_month",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.hive_style_partitioning": "false",  # <— align with existing

    # Glue/HMS sync
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.database": glue_db,
    "hoodie.datasource.hive_sync.table": table,
    "hoodie.datasource.hive_sync.partition_fields": "pickup_year,pickup_month",
    "hoodie.datasource.hive_sync.support_timestamp": "true"
    # Do NOT set partition_extractor_class unless you flip to hive_style=true
}

(df.write
   .format("hudi")
   .options(**hudi_options)
   .mode("append")
   .save(silver))

spark.stop()

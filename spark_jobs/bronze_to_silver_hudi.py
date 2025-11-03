import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

bucket = os.environ["S3_BUCKET"]
bronze = f"s3a://{bucket}/{os.environ['BRONZE_PREFIX']}/tlc/yellow/"
silver = f"s3a://{bucket}/{os.environ['SILVER_PREFIX']}/hudi/yellow/"
glue_db = os.environ["GLUE_DB_HUDI"]
table = f"{glue_db}.yellow_trips_silver"

spark = (SparkSession.builder
    .appName("hudi_silver")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .getOrCreate())

df = spark.read.parquet(bronze).withColumn(
    "trip_id", expr("concat(PULocationID, '_', cast(tpep_pickup_datetime as string))")
)

(hudi_options := {
    "hoodie.table.name": "yellow_trips_silver",
    "hoodie.datasource.write.recordkey.field": "trip_id",
    "hoodie.datasource.write.precombine.field": "tpep_pickup_datetime",
    "hoodie.datasource.write.partitionpath.field": "pickup_year,pickup_month",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": glue_db,
    "hoodie.datasource.hive_sync.table": "yellow_trips_silver",
    "hoodie.datasource.hive_sync.mode":"hms",
})

(df.write.format("hudi")
   .options(**hudi_options)
   .mode("append")
   .save(silver))

spark.stop()

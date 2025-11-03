import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from common_schemas import TAXI_SCHEMA

bucket = os.environ["S3_BUCKET"]
raw_prefix = os.environ["RAW_PREFIX"]
bronze_prefix = os.environ["BRONZE_PREFIX"]

def spark():
    return SparkSession.builder.appName("ingest_raw").getOrCreate()

def main():
    s = spark()
    src = f"s3a://{bucket}/{raw_prefix}/"  # raw/tlc/yellow/YYYY/MM/*.csv
    df = s.read.option("header", True).schema(TAXI_SCHEMA).csv(src)
    df = df.dropna(subset=["tpep_pickup_datetime","total_amount"]) \
           .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
           .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
    # Write Parquet bronze (neutral for all formats)
    dest = f"s3a://{bucket}/{bronze_prefix}/tlc/yellow/"
    (df.write.mode("overwrite")
        .partitionBy("pickup_year","pickup_month")
        .parquet(dest))
    s.stop()

if __name__ == "__main__":
    main()

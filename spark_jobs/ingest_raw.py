import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_timestamp, coalesce, trim, when, regexp_replace, count

bucket = os.environ["S3_BUCKET"]
raw_prefix = os.environ["RAW_PREFIX"]          # e.g. raw/tlc/yellow
bronze_prefix = os.environ["BRONZE_PREFIX"]    # e.g. bronze

def spark():
    return (SparkSession.builder
            .appName("ingest_raw")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate())

def main():
    s = spark()
    src = f"s3a://{bucket}/{raw_prefix}/"  # raw/tlc/yellow/YYYY/MM/*.parquet

    # Read ONLY parquet (recursing into YYYY/MM/)
    df_raw = (s.read
                .option("recursiveFileLookup", "true")
                .parquet(src))

    print("RAW count:", df_raw.count())
    df_raw.printSchema()

    # ensure types even if some months are strings
    pickup_ts = coalesce(
        col("tpep_pickup_datetime").cast("timestamp"),
        to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss.SSS")
    )

    total_amt_str = regexp_replace(col("total_amount").cast("string"), ",", "")
    total_amt_null_if_blank = when(trim(total_amt_str) == "", None).otherwise(total_amt_str)
    total_amt_null_if_nan   = when(total_amt_null_if_blank.isin("NaN", "nan"), None).otherwise(total_amt_null_if_blank)
    total_amt_num = coalesce(col("total_amount").cast("double"), total_amt_null_if_nan.cast("double"))

    df = (df_raw
            .withColumn("tpep_pickup_datetime", pickup_ts)
            .withColumn("total_amount", total_amt_num))

    # Diagnostics
    diag = (df.select(
        count(when(col("tpep_pickup_datetime").isNull(), 1)).alias("null_pickup_ts"),
        count(when(col("total_amount").isNull(), 1)).alias("null_total_amount"))
        .collect()[0])
    print("Nulls -> pickup_ts:", diag["null_pickup_ts"], " total_amount:", diag["null_total_amount"])

    # Filter after casting and add partitions
    df = (df.filter(col("tpep_pickup_datetime").isNotNull() & col("total_amount").isNotNull())
            .withColumn("pickup_year",  year(col("tpep_pickup_datetime")))
            .withColumn("pickup_month", month(col("tpep_pickup_datetime"))))

    print("After cast+filter:", df.count())

    # Write Parquet bronze
    dest = f"s3a://{bucket}/{bronze_prefix}/tlc/yellow/"
    (df.write.mode("overwrite")
       .partitionBy("pickup_year","pickup_month")
       .parquet(dest))

    s.stop()

if __name__ == "__main__":
    main()

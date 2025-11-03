import os
from pyspark.sql import SparkSession

bucket = os.environ["S3_BUCKET"]
silver_loc = f"s3a://{bucket}/{os.environ['SILVER_PREFIX']}/delta/yellow/"
glue_db = os.environ["GLUE_DB_DELTA"]
table = f"{glue_db}.yellow_trips_silver"

spark = (SparkSession.builder
         .appName("delta_maintenance")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
         .getOrCreate())

# Compact small files
spark.sql(f"OPTIMIZE {table}")

# VACUUM old files (168 hours = 7 days retention)
spark.sql(f"VACUUM {table} RETAIN 168 HOURS")

spark.stop()


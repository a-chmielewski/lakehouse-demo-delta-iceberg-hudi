import os
from pyspark.sql import SparkSession

bucket = os.environ["S3_BUCKET"]
glue_db = os.environ["GLUE_DB_ICEBERG"]
table = f"glue.{glue_db}.yellow_trips_silver"

spark = (SparkSession.builder
    .appName("iceberg_maintenance")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue.warehouse", f"s3a://{bucket}/warehouse/iceberg")
    .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate())

# Rewrite data files to optimize file size
spark.sql(f"CALL glue.system.rewrite_data_files(table => '{table}')")

# Rewrite manifests to optimize metadata
spark.sql(f"CALL glue.system.rewrite_manifests(table => '{table}')")

# Expire old snapshots (7 days retention)
spark.sql(f"""
CALL glue.system.expire_snapshots(
    table => '{table}',
    older_than => TIMESTAMP '{os.popen('date -u -d "7 days ago" +"%Y-%m-%d %H:%M:%S"').read().strip()}',
    retain_last => 5
)
""")

spark.stop()


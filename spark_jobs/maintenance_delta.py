import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

bucket = os.environ["S3_BUCKET"]
silver_loc = f"s3a://{bucket}/{os.environ['SILVER_PREFIX']}/delta/yellow/"
glue_db = os.environ["GLUE_DB_DELTA"]
table = f"{glue_db}.yellow_trips_silver"
opt_where = os.environ.get("OPTIMIZE_WHERE", "").strip() 

spark = (
    SparkSession.builder
    .appName("delta_maintenance")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.databricks.delta.optimize.maxFileSize", "134217728")  # 128 MB
    .getOrCreate()
)

def run_sql(sql_table_stmt: str, sql_path_stmt: str):
    try:
        return spark.sql(sql_table_stmt)
    except AnalysisException:
        return spark.sql(sql_path_stmt)

# OPTIMIZE (optionally with predicate)
opt_table = f"OPTIMIZE {table} {opt_where}".strip()
opt_path  = f"OPTIMIZE delta.`{silver_loc}` {opt_where}".strip()
run_sql(opt_table, opt_path)

# VACUUM (7 days)
run_sql(
    f"VACUUM {table} RETAIN 168 HOURS",
    f"VACUUM delta.`{silver_loc}` RETAIN 168 HOURS",
)

spark.stop()

import os
from pyspark.sql import SparkSession

bucket = os.environ["S3_BUCKET"]
silver = f"s3a://{bucket}/{os.environ['SILVER_PREFIX']}/hudi/yellow/"
glue_db = os.environ["GLUE_DB_HUDI"]
table = f"{glue_db}.yellow_trips_silver"

spark = (SparkSession.builder
    .appName("hudi_maintenance")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate())

# Clean old file versions (168 hours = 7 days retention)
spark.sql(f"""
CALL run_clean(
    table => '{table}',
    retain_commits => 10,
    clean_policy => 'KEEP_LATEST_FILE_VERSIONS'
)
""")

# Compaction for MOR tables (skip if using COW)
# Uncomment if table type is MERGE_ON_READ:
# spark.sql(f"""
# CALL run_compaction(
#     table => '{table}',
#     operation => 'schedule_and_execute'
# )
# """)

spark.stop()


import os
import sys
import time
import boto3
from pathlib import Path

athena = boto3.client("athena")
s3_output = os.environ.get("ATHENA_OUTPUT_BUCKET", f"s3://{os.environ['S3_BUCKET']}/athena-results/")

def run_query(sql, database="default"):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": s3_output},
        ResultReuseConfiguration={
            "ResultReuseByAgeConfiguration": {
                "Enabled": True,
                "MaxAgeInMinutes": 60
            }
        }
    )
    query_id = response["QueryExecutionId"]
    
    # Poll for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(0.5)
    
    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        print(f"Query failed: {reason}")
        return None
    
    stats = status["QueryExecution"]["Statistics"]
    bytes_scanned = stats.get("DataScannedInBytes", 0)
    exec_time_ms = stats.get("EngineExecutionTimeInMillis", 0)
    
    print(f"Query ID: {query_id}")
    print(f"Data Scanned: {bytes_scanned / (1024**3):.4f} GB (${bytes_scanned / (1024**4) * 5:.6f})")
    print(f"Execution Time: {exec_time_ms / 1000:.2f}s")
    print(f"Result Reused: {status['QueryExecution'].get('ResultReuseInformation', {}).get('ReusedPreviousResult', False)}")
    
    return query_id

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python athena_run.py <sql_file> [database]")
        sys.exit(1)
    
    sql_file = Path(sys.argv[1])
    database = sys.argv[2] if len(sys.argv) > 2 else "default"
    
    if not sql_file.exists():
        print(f"File not found: {sql_file}")
        sys.exit(1)
    
    sql = sql_file.read_text()
    
    # Split and run multiple queries if separated by semicolons
    queries = [q.strip() for q in sql.split(";") if q.strip()]
    
    total_bytes = 0
    total_time = 0
    
    for i, query in enumerate(queries, 1):
        print(f"\n--- Query {i}/{len(queries)} ---")
        query_id = run_query(query, database)
        if query_id:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            stats = status["QueryExecution"]["Statistics"]
            total_bytes += stats.get("DataScannedInBytes", 0)
            total_time += stats.get("EngineExecutionTimeInMillis", 0)
    
    if len(queries) > 1:
        print(f"\n=== TOTAL ===")
        print(f"Total Data Scanned: {total_bytes / (1024**3):.4f} GB (${total_bytes / (1024**4) * 5:.6f})")
        print(f"Total Execution Time: {total_time / 1000:.2f}s")


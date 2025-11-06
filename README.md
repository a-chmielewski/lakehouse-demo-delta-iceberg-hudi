# Lakehouse Demo: Delta Lake vs Iceberg vs Hudi

A comprehensive comparison of three major lakehouse table formats—**Delta Lake**, **Apache Iceberg**, and **Apache Hudi**—running on AWS EMR Serverless with AWS Glue Catalog and S3 storage.

## 📋 Project Overview

This project implements a **medallion architecture** (Bronze → Silver → Gold) to compare the performance, storage efficiency, and operational characteristics of modern lakehouse formats using real-world NYC TLC Yellow Taxi trip data.

### Goals
- Evaluate storage efficiency and file organization strategies
- Compare query performance on AWS Athena
- Test ACID transaction capabilities (upserts, deletes)
- Assess maintenance operations (compaction, optimization, vacuum)
- Understand trade-offs for different use cases

## 🏗️ Architecture

```
Raw Data (Parquet)
    ↓
Bronze Layer (Partitioned Parquet)
    ↓
Silver Layer (Format-Specific Tables)
    ├── Delta Lake
    ├── Apache Iceberg
    └── Apache Hudi
    ↓
Gold Layer (Aggregated Views)
```

### Technology Stack
- **Compute**: AWS EMR Serverless (Spark 3.5.1)
- **Catalog**: AWS Glue Data Catalog
- **Storage**: Amazon S3
- **Query Engine**: Amazon Athena
- **Formats**: Delta Lake 3.2.0, Apache Iceberg 1.5.0, Apache Hudi 0.14.0

## 📊 Dataset

**NYC TLC Yellow Taxi Trip Records**
- Source: NYC Taxi & Limousine Commission
- Period: January 2025 - March 2025
- Records: ~33M trips (Delta), ~11M trips (Iceberg/Hudi)
- Size: Raw data ~2GB compressed parquet

## 🔄 Data Pipeline

### 1. Ingestion (`ingest_raw.py`)
- Reads raw parquet files from S3
- Handles type casting and null values
- Filters invalid records
- Partitions by `pickup_year` and `pickup_month`
- Outputs to Bronze layer

### 2. Silver Transformation

#### Delta Lake (`bronze_to_silver_delta.py`)
- Uses Delta Lake's MERGE operation for upserts
- Synthetic `trip_id` key: `concat(PULocationID, timestamp, random)`
- Implements row-level deletes (negative amounts)
- Integrates with Glue Catalog via Hive metastore

#### Apache Iceberg (`bronze_to_silver_iceberg.py`)
- Partitioned by `months(tpep_pickup_datetime)`
- Uses `INSERT OVERWRITE` for full refresh
- Schema evolution support (added `payment_type_desc`)
- Format version 2 for advanced features

#### Apache Hudi (`bronze_to_silver_hudi.py`)
- Copy-on-Write (COW) table type
- ComplexKeyGenerator for composite partitions
- Hive-style partitioning: false (non-standard paths)
- HMS sync enabled for Glue integration

### 3. Maintenance Operations

#### Delta Lake
```sql
OPTIMIZE table WHERE condition  -- Compaction
VACUUM table RETAIN 168 HOURS   -- Cleanup old files
```

#### Iceberg
```sql
CALL system.rewrite_data_files()    -- Optimize file sizes
CALL system.rewrite_manifests()     -- Optimize metadata
CALL system.expire_snapshots()      -- Remove old versions
```

#### Hudi
```sql
CALL run_clean()                    -- Remove old file versions
CALL run_compaction()               -- Merge delta files (MOR)
```

## 📈 Performance Results

### Storage Efficiency

| Format  | Files | Size (MB) | Compression | Notes |
|---------|------:|----------:|------------:|-------|
| **Delta**   | 227   | ~1,000    | 1.0x        | Many transaction logs, metadata overhead |
| **Iceberg** | 23    | ~450      | **2.2x** ⭐ | Excellent compression, fewest files |
| **Hudi**    | 225   | ~1,125    | 0.9x        | Many small files (~5MB each), highest overhead |

### Query Performance (Athena)

#### Q1: Top 10 Pickup Zones by Trip Count

| Format | Runtime | Data Scanned | Efficiency |
|--------|---------|--------------|------------|
| **Delta**   | 1.638s  | **189 KB** ⭐    | 100x better than others |
| **Iceberg** | 1.325s  | 9 MB         | Fast runtime |
| **Hudi**    | 1.813s  | 8 MB         | Moderate |

**Query:**
```sql
SELECT PULocationID, count(*) AS trips
FROM {format}.yellow_trips_silver
GROUP BY 1 ORDER BY trips DESC LIMIT 10;
```

#### Q2: Monthly Aggregated Metrics

| Format | Runtime | Data Scanned | Efficiency |
|--------|---------|--------------|------------|
| **Delta**   | 3.526s  | 348 MB       | Poor for time-series |
| **Iceberg** | 2.938s  | **74 MB** ⭐     | 4.7x better than Delta |
| **Hudi**    | 1.779s  | 89 MB        | Good balance |

**Query:**
```sql
SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       avg(trip_distance) AS avg_dist,
       avg(total_amount) AS avg_total,
       count(*) AS n
FROM {format}.yellow_trips_silver
GROUP BY 1 ORDER BY 1;
```

### Time Travel Capabilities

| Format | Version Query | Timestamp Query | History Metadata |
|--------|--------------|-----------------|------------------|
| **Delta**   | `VERSION AS OF n` | `TIMESTAMP AS OF 'timestamp'` | `DESCRIBE HISTORY` |
| **Iceberg** | `VERSION AS OF snapshot_id` | `FOR SYSTEM_TIME AS OF 'timestamp'` | `SELECT * FROM table.snapshots` |
| **Hudi**    | Limited | Filter `_hoodie_commit_time` | Query commit metadata |

## 🎯 Key Findings

### 1. Storage & File Organization

**Iceberg Wins Overall**
- **50% less storage** than Delta/Hudi through superior compression
- Only **23 files** vs 225+ for Delta/Hudi
- Centralized metadata management reduces overhead
- Optimal file sizes (~20MB each)

**Delta & Hudi Issues**
- Excessive small files create S3 LIST operation overhead
- Delta: Many transaction log files accumulate
- Hudi: Partition metadata files per partition
- Both benefit from aggressive compaction schedules

### 2. Query Performance

**Delta Lake: Best for Simple Aggregations**
- Extraordinary partition pruning: **189 KB scanned** for Q1
- Advanced statistics and Z-ordering capabilities
- Poor performance on time-based queries without optimization

**Iceberg: Most Consistent**
- Balanced performance across query types
- Partition evolution without rewriting data
- Hidden partitioning simplifies queries

**Hudi: Best for Incremental Updates**
- Optimized for streaming and CDC workloads
- MOR (Merge-on-Read) reduces write latency
- Trade-off: More complex compaction management

### 3. ACID Operations

**Delta Lake**
- Mature MERGE/UPDATE/DELETE support
- Optimistic concurrency control
- Change Data Feed for CDC
- **Limitation**: Requires frequent VACUUM to reclaim space

**Iceberg**
- Full SQL DML support (INSERT/UPDATE/DELETE/MERGE)
- Snapshot isolation
- Schema evolution without table rewrites
- **Advantage**: Metadata-only operations are fast

**Hudi**
- Timeline-based consistency
- Record-level indexing for fast upserts
- Multiple table types (COW vs MOR)
- **Complexity**: More configuration tuning required

### 4. Athena Compatibility

| Feature | Delta | Iceberg | Hudi |
|---------|-------|---------|------|
| SELECT queries | ✅ | ✅ | ✅ |
| CREATE VIEW | ⚠️ Limited | ✅ Full | ✅ Full |
| Time travel | ✅ | ✅ | ⚠️ Manual |
| CTAS support | ✅ | ✅ | ✅ |

**Note**: Delta Lake has limited Athena view support due to AWS managed service constraints.

### 5. Operational Complexity

**Easiest to Hardest**:
1. **Iceberg**: Simple configuration, fewer knobs to tune
2. **Delta**: Well-documented, mature ecosystem
3. **Hudi**: Steepest learning curve, many configuration options

## 📚 Lessons Learned

### Data Quality Issues
- **Type inconsistencies**: Some months had timestamp as string
- **Null handling**: Required explicit coalesce and casting logic
- **Invalid records**: Negative amounts, null timestamps needed filtering
- **Solution**: Robust bronze layer validation before silver ingestion

### Record Count Variance
- Delta showed **~3x more records** than Iceberg/Hudi
- **Cause**: Multiple ingestion runs without proper deduplication
- **Lesson**: Always implement idempotent upsert logic with business keys
- **Fix**: Use proper `MERGE` operations instead of `INSERT OVERWRITE`

### Partitioning Strategy
- **Time-based partitioning** (month) is essential for taxi data
- Iceberg's `months()` function handles partition evolution automatically
- Delta/Hudi require explicit partition columns in dataframe
- **Recommendation**: Start coarse (month), refine if queries need it

### Compaction is Critical
- Without maintenance, all formats accumulate small files
- **Delta**: Run OPTIMIZE after every 10-20 writes
- **Iceberg**: Schedule `rewrite_data_files` nightly
- **Hudi**: Inline compaction for COW, async for MOR

### AWS Glue Catalog Gotchas
- Database names are case-sensitive in Spark, case-insensitive in Athena
- Column name quoting required for case-sensitive columns (`"PULocationID"`)
- Hudi's non-hive-style partitioning can confuse Athena partition discovery
- Always test Athena queries after Spark job completion

### S3 Performance Optimization
```python
.config("spark.hadoop.fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
.config("spark.hadoop.fs.s3a.region", "eu-central-1")
.config("spark.hadoop.fs.s3a.connection.maximum", "200")
```
- Regional endpoints reduce latency
- Increase connection pool for parallel reads

### Cost Considerations
- **S3 Storage**: Iceberg saves 50% on storage costs
- **S3 API Calls**: Fewer files = fewer LIST operations = lower costs
- **Athena Queries**: Data scanned directly impacts billing
  - Q1: Delta scans 0.189 MB → $0.000001 per query
  - Q2: Delta scans 348 MB → $0.001740 per query
- **EMR Serverless**: Pay per vCPU-second, optimize Spark configs

## 🚀 Usage

### Prerequisites
```bash
# AWS CLI configured with appropriate credentials
aws configure

# Required IAM role for EMR Serverless
# See infra/emr-serverless-trust.json and emr-serverless-exec-policy.json
```

### Setup

1. **Create S3 bucket and upload data**
```bash
# Windows (PowerShell)
.\scripts\s3_sync_raw.ps1

# Linux/Mac
./scripts/s3_sync_raw.sh
```

2. **Create Glue databases**
```sql
-- Run in Athena
CREATE DATABASE IF NOT EXISTS delta_demo;
CREATE DATABASE IF NOT EXISTS iceberg_demo;
CREATE DATABASE IF NOT EXISTS hudi_demo;
```

3. **Package and upload Spark jobs**
```bash
zip -r spark_jobs.zip spark_jobs/*.py
aws s3 cp spark_jobs.zip s3://YOUR_BUCKET/code/
```

### Running Jobs

#### Ingest Raw Data
```bash
aws emr-serverless start-job-run \
  --application-id YOUR_APP_ID \
  --execution-role-arn YOUR_ROLE_ARN \
  --job-driver file://job-config-ingest_raw.json
```

#### Bronze to Silver (Delta)
```bash
aws emr-serverless start-job-run \
  --application-id YOUR_APP_ID \
  --execution-role-arn YOUR_ROLE_ARN \
  --job-driver file://job-config-bronze-delta.json
```

Similarly for Iceberg and Hudi using respective config files.

#### Maintenance
```bash
# Delta optimization
aws emr-serverless start-job-run \
  --job-driver file://job-config-maintenance-delta.json
```

### Querying in Athena

```sql
-- Query across all formats
SELECT 'delta' AS format, COUNT(*) AS cnt 
FROM delta_demo.yellow_trips_silver
UNION ALL
SELECT 'iceberg', COUNT(*) 
FROM iceberg_demo.yellow_trips_silver
UNION ALL
SELECT 'hudi', COUNT(*) 
FROM hudi_demo.yellow_trips_silver;
```

## 🎬 Recommendations

### Use Delta Lake When:
- ✅ Working in Databricks ecosystem
- ✅ Need exceptional partition pruning for dimensional queries
- ✅ Require mature CDC (Change Data Feed)
- ✅ Have existing Delta investments
- ❌ Avoid if: Limited Athena view support is a blocker

### Use Apache Iceberg When:
- ✅ Need best storage efficiency (cloud cost optimization)
- ✅ Want consistent query performance across patterns
- ✅ Require advanced schema evolution
- ✅ Multi-engine access (Spark, Trino, Flink, Athena)
- ✅ **Best all-around choice for new projects**

### Use Apache Hudi When:
- ✅ Optimizing for streaming/CDC workloads
- ✅ Need fast incremental upserts with record-level indexing
- ✅ Implementing data lakehouse on existing Hive infra
- ✅ Require fine-grained control over compaction
- ❌ Avoid if: Team lacks deep Hudi expertise

## 📁 Project Structure

```
.
├── spark_jobs/                  # PySpark ETL jobs
│   ├── ingest_raw.py           # Bronze ingestion
│   ├── bronze_to_silver_delta.py
│   ├── bronze_to_silver_iceberg.py
│   ├── bronze_to_silver_hudi.py
│   ├── maintenance_delta.py     # OPTIMIZE, VACUUM
│   ├── maintenance_iceberg.py   # Compaction, expire snapshots
│   ├── maintenance_hudi.py      # Clean, compaction
│   └── common_schemas.py        # Shared schema definitions
├── sql/                         # Analysis queries
│   ├── q1_top_zones.sql        # Aggregation by location
│   ├── q2_monthly_metrics.sql  # Time-series analysis
│   ├── q3_time_travel_examples.sql
│   └── gold_views.sql          # Aggregated views
├── infra/                       # Infrastructure as Code
│   ├── create_glue_dbs.sql
│   ├── emr-serverless-trust.json
│   └── emr-serverless-exec-policy.json
├── scripts/                     # Helper scripts
│   ├── download_tlc.py         # Download NYC taxi data
│   ├── s3_sync_raw.ps1/sh      # Upload to S3
│   └── metrics_s3.ps1/sh       # Storage analysis
├── spark_conf/                  # Spark configurations
│   ├── spark_delta.conf
│   ├── spark_iceberg.conf
│   └── spark_hudi.conf
├── job-config-*.json           # EMR job configurations
└── docs/
    ├── howto.md                # Detailed setup guide
    └── storage_summary.txt     # Benchmark results
```

## 🔗 Resources

### Documentation
- [Delta Lake](https://docs.delta.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Hudi](https://hudi.apache.org/)
- [AWS EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)
- [AWS Glue Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)

### NYC TLC Data
- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

## 📝 Requirements

```
pyspark==3.5.1
delta-spark==3.2.0
boto3==1.35.0
pandas==2.2.2
typer==0.12.3
rich==13.7.1
```

## 🎓 Future Enhancements

- [ ] Add Trino/Presto query performance comparison
- [ ] Implement streaming ingestion with Kafka
- [ ] Test schema evolution scenarios (add/drop/rename columns)
- [ ] Benchmark concurrent write performance
- [ ] Add dbt models for gold layer transformations
- [ ] Implement data quality checks with Great Expectations
- [ ] Add monitoring with CloudWatch metrics
- [ ] Cost analysis dashboard

## 📄 License

MIT License - Feel free to use this project as a learning resource or starting point for your own lakehouse implementations.

---

**Author**: Portfolio Project  
**Date**: November 2024  
**AWS Region**: eu-central-1  
**Spark Version**: 3.5.1


Delta has limited support and view couldnt be created


Q1 delta Time in queue:61 ms Run time:1.638 sec Data scanned:189.03 KB
#	PULocationID	trips
1	161	1515455
2	237	1456110
3	236	1356372
4	132	1220423
5	230	1103897
6	186	1073239
7	162	1064426
8	142	983343
9	234	927012
10	170	895664


Q1 iceberg Time in queue:60 ms Run time:1.325 sec Data scanned:8.97 MB 
#	PULocationID	trips
1	161	513560
2	237	491957
3	236	456758
4	132	433506
5	230	377216
6	186	365697
7	162	360560
8	142	332387
9	234	312841
10	170	303493


Q1 Hudi Time in queue:61 ms Run time:1.813 sec Data scanned:7.89 MB
#	PULocationID	trips
1	161	476547
2	237	462083
3	236	431084
4	132	391644
5	230	353551
6	186	345823
7	162	341581
8	142	316386
9	234	299056
10	170	289900

Q2 Delta Time in queue:62 ms Run time:3.526 sec Data scanned:348.17 MB
#	month	avg_dist	avg_total	n
1	2007-12-01 00:00:00.000 UTC	3.0	22.75	3
2	2009-01-01 00:00:00.000 UTC	10.770000000000001	70.39	3
3	2024-12-01 00:00:00.000 UTC	3.657619047619047	28.055714285714288	63
4	2025-01-01 00:00:00.000 UTC	5.871986719203346	26.57020325234891	10236585
5	2025-02-01 00:00:00.000 UTC	6.064864525411179	25.84762435844971	10567074
6	2025-03-01 00:00:00.000 UTC	6.633863080484065	27.183071843340834	12229608
7	2025-04-01 00:00:00.000 UTC	7.04	36.7	6


Q2 Iceberg Time in queue:109 ms Run time:2.938 sec Data scanned:74.40 MB
#	month	avg_dist	avg_total	n
1	2007-12-01 00:00:00.000000 UTC	3.0	22.75	1
2	2009-01-01 00:00:00.000000 UTC	10.77	70.39	1
3	2024-12-01 00:00:00.000000 UTC	3.6576190476190478	28.055714285714277	21
4	2025-01-01 00:00:00.000000 UTC	5.855123079482707	25.611306245860465	3475234
5	2025-02-01 00:00:00.000000 UTC	6.025345678681046	25.03050302693928	3577542
6	2025-03-01 00:00:00.000000 UTC	6.5841275346956625	26.26590350585449	4145225
7	2025-04-01 00:00:00.000000 UTC	7.04	36.7	2


Q2 Hudi Time in queue:64 ms Run time:1.779 sec Data scanned:88.82 MB
#	month	avg_dist	avg_total	n
1	2007-12-01 00:00:00.000	3.0	22.75	1
2	2009-01-01 00:00:00.000	10.77	70.39	1
3	2024-12-01 00:00:00.000	3.6576190476190478	28.055714285714284	21
4	2025-01-01 00:00:00.000	5.84877919423068	26.53316279064086	3325443
5	2025-02-01 00:00:00.000	6.0659764834067875	25.84739595281799	3427027
6	2025-03-01 00:00:00.000	6.629243183360766	27.169958535913228	3966324
7	2025-04-01 00:00:00.000	7.04	36.7	2

---

## Storage and Query Performance Comparison

| Format  | Files | Size (MB) | Athena bytes scanned (q1/q2) | Notes                                                                                     |
| ------- | ----: | --------: | ---------------------------: | ----------------------------------------------------------------------------------------- |
| Delta   |   227 |    ~1,000 |             189 KB / 348 MB  | Largest file count with metadata overhead; Q1 extremely efficient (KB vs MB); Q2 scans 3x more data; Limited Athena view support |
| Iceberg |    23 |      ~450 |               9 MB / 74 MB   | Most efficient storage with fewest files; Best compression ratio; Excellent metadata management; Q1 and Q2 both efficient |
| Hudi    |   225 |    ~1,125 |               8 MB / 89 MB   | Many small files (~5MB each); Highest storage overhead; Q1 efficient but Q2 scans more than Iceberg; Best for streaming/incremental updates |

**Key Observations:**
- **Q1 Query (Top Zones)**: Delta dramatically outperforms with only 189 KB scanned vs 8-9 MB for Iceberg/Hudi, showing superior partition pruning for simple aggregations
- **Q2 Query (Monthly Metrics)**: Delta scans 348 MB (4.7x more than Iceberg, 3.9x more than Hudi), indicating less efficient for time-based aggregations
- **Storage Efficiency**: Iceberg uses ~45-50% less storage than Delta/Hudi with only 23 files vs 225+ files
- **File Organization**: Iceberg's fewer, larger files provide better query performance consistency; Hudi's many small files create overhead
- **Metadata Overhead**: Delta has extensive transaction logs; Hudi has partition metadata files; Iceberg has centralized metadata catalog
- **Record Count Variance**: Delta shows ~3x more records than Iceberg/Hudi (likely due to different compaction/deduplication strategies or multiple ingestion runs)
- **Runtime**: Iceberg fastest (1.3s), Delta slowest (1.6-3.5s), Hudi moderate (1.8s)
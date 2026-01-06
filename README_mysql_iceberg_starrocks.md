# 📦 MySQL → Iceberg → StarRocks  
## Production-Grade Bulk Historical Data Ingestion

---

## 📌 Objective

Build a **fault-tolerant, restart-safe, scalable** pipeline to ingest **historical (bulk) data** from MySQL into StarRocks using Apache Spark and Apache Iceberg as a staging layer.

This pipeline is **batch-only**, designed for:
- Initial historical loads
- Backfills
- Large datasets (10M → billions of rows)
- Zero data loss
- Safe retries without re-reading MySQL

---

## 🏗️ High-Level Architecture

```
MySQL (OLTP Source)
        ↓
Bulk Extract (Apache Spark - JDBC, partitioned)
        ↓
Iceberg Staging Layer (Parquet + Metadata + Snapshots)
        ↓
StarRocks Warehouse (Raw OLAP Tables)
```

---

## 🧠 Why This Architecture?

### ❌ Why NOT MySQL → StarRocks directly?
- JDBC timeouts on large tables
- No checkpointing
- Full restart on failure
- No retry without re-extracting data
- Tight coupling between OLTP and OLAP

### ✅ Why Iceberg in between?
Iceberg acts as a **durable checkpoint layer**.

| Capability | Why it matters |
|-----------|----------------|
| Snapshots | Restart from last success |
| Atomic commits | No partial data |
| Manifests | Tracks exactly which files belong to a load |
| Metadata | Schema + statistics |
| Append mode | Safe bulk ingestion |

---

## 🔄 End-to-End Flow (Step by Step)

### 1️⃣ Bulk Extract from MySQL (Spark)

**Goals**
- Do not lock MySQL tables
- Read data in parallel
- Avoid memory pressure
- Ensure deterministic extraction

**Best Practices**
- Use partitioned JDBC reads
- Avoid single-threaded full table scans
- Use stable numeric or date columns for partitioning

---

### 2️⃣ Iceberg Staging Layer (Core Safety Net)

**What Iceberg Manages Automatically**
```
table/
 ├── data/              → Parquet data files
 ├── metadata/
 │    ├── v1.metadata.json
 │    ├── v2.metadata.json
 ├── manifests/
 │    ├── snap-00001.avro
 │    ├── snap-00002.avro
```

**Why this matters**
- Each write creates a snapshot
- Failed jobs do not corrupt data
- Exact file-level lineage is preserved

---

### 3️⃣ Data Fidelity Rules (RAW Means RAW)

This stage is a **lossless dump**.

#### ✅ Preserve Exactly
- Column names
- Data types
- Precision & scale
- Nullability
- Row counts

#### ❌ Never Do
- Type casting
- Timezone conversion
- Default value injection
- Deduplication
- Business logic

---

### 4️⃣ Data Type Compatibility

#### Numeric
- Preserve DECIMAL precision exactly
- Never convert DECIMAL → FLOAT

#### Strings
- Use STRING in StarRocks raw tables
- Avoid VARCHAR length constraints

#### Date & Time
- No timezone manipulation
- Treat timestamps as-is

---

### 5️⃣ Audit Columns (Recommended)

Allowed metadata columns:
- `_ingest_time`
- `_source_system`
- `_batch_id`

Used for:
- Debugging
- Replay
- Auditing

---

### 6️⃣ Load into StarRocks (Raw Warehouse)

**Table Characteristics**
- Raw tables only
- No aggregation
- No joins
- No transformations

**Distribution Keys**
- Optional for bulk loads
- Recommended: source primary key
- Avoid low-cardinality columns

---

## 🚨 Failure Scenarios & Recovery

| Failure | Behavior | Recovery |
|------|--------|---------|
| MySQL timeout | Spark task fails | Retry partition |
| Spark crash | Partial files written | No snapshot committed |
| Network failure | Load interrupted | Resume from Iceberg |
| Duplicate run | Re-run batch | Append with audit |

---

## ✅ Operational Guarantees

- No data loss
- Idempotent retries
- Safe restarts
- Loose coupling between OLTP and OLAP
- Production-grade observability

---

## 🧠 Design Principles

- Iceberg is a checkpoint, not just storage
- Raw layer is immutable
- Fail fast, retry safely
- Never trust direct JDBC for big data

---

## 📌 Scope

### Covered
- Historical bulk ingestion
- Fault tolerance
- Iceberg metadata & snapshots
- Production safety

### Not Covered
- Incremental CDC
- Schema drift handling
- Dim / Fact modeling
- Business transformations

---

## 📍 Summary

This architecture is proven for large-scale historical ingestion:

```
MySQL → Spark → Iceberg → StarRocks
```

Iceberg acts as the **contract of truth** between source systems and the warehouse.

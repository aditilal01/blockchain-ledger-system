# 🔗 Blockchain Transaction Ledger System
### End-to-End Data Engineering Capstone Project

[![Python](https://img.shields.io/badge/Python-3.8+-blue)](https://python.org)
[![Pandas](https://img.shields.io/badge/Pandas-2.0-green)](https://pandas.pydata.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-3.x-red)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/PySpark-3.4-orange)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.7-blue)](https://airflow.apache.org)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.9-yellow)](https://duckdb.org)

---

## 📌 Problem Statement

Financial institutions process millions of blockchain transactions daily. Without a robust data pipeline, detecting fraud, generating compliance reports, and analyzing transaction patterns becomes impossible. This project builds a **production-grade data engineering pipeline** that simulates a real blockchain ledger system — from raw transaction generation through to analytics.

---

## 🏗️ Architecture

```
Data Generation → Ingestion → Processing → Storage → Orchestration → Analytics

[Simulator]  →  [CSV/JSON]  →  [Pandas+NumPy]  →  [DuckDB OLAP]
                                     ↓
                              [Kafka Streaming]
                                     ↓
                              [Spark Processing]
                                     ↓
                            [Airflow Orchestration]
                                     ↓
                             [Analytics Report]
```

---

## 📁 Project Structure

```
blockchain_ledger_system/
├── data/
│   ├── raw/                    # Generated CSVs, JSONs
│   ├── processed/              # Cleaned Parquet files
│   └── warehouse/              # OLAP output, DuckDB
├── src/
│   ├── ingestion/
│   │   ├── generate_transactions.py   # Synthetic data generator
│   │   └── ingest_and_clean.py        # Pandas ETL + merge
│   ├── processing/
│   │   ├── process_transactions.py    # Validation, normalization
│   │   └── spark_job.py               # PySpark job
│   ├── streaming/
│   │   ├── kafka_producer.py          # Kafka event producer
│   │   └── kafka_consumer.py          # Kafka consumer
│   ├── orchestration/
│   │   └── blockchain_etl_dag.py      # Airflow DAG
│   └── analytics/
│       └── analytics_report.py        # Final reporting
├── sql/
│   └── schema.sql                     # OLTP + Star Schema + Queries
├── scripts/
│   └── setup_and_run.sh               # Linux automation script
├── logs/                              # Pipeline execution logs
├── docs/                              # Documentation
├── run_pipeline.py                    # Main entry point
└── requirements.txt
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- (Optional) Docker for Kafka
- (Optional) Java 11+ for Spark

### 1. Clone & Setup
```bash
git clone https://github.com/yourusername/blockchain-ledger-system.git
cd blockchain-ledger-system

# Auto-setup (Linux/macOS)
chmod +x scripts/setup_and_run.sh
./scripts/setup_and_run.sh
```

### 2. Manual Run
```bash
pip install -r requirements.txt
python run_pipeline.py --records 100000
```

### 3. Run with Kafka + Spark
```bash
# Start Kafka (Docker)
docker-compose up -d

# Full pipeline
python run_pipeline.py --records 100000
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Generation | Python, hashlib | Synthetic blockchain data |
| Ingestion | Pandas, NumPy | ETL, cleaning, merging |
| Streaming | Apache Kafka | Real-time transactions |
| Batch Processing | PySpark | Large-scale transforms |
| Orchestration | Apache Airflow | Pipeline scheduling |
| Storage (OLTP) | PostgreSQL | Transactional records |
| Storage (OLAP) | DuckDB | Analytics warehouse |
| Automation | Bash | Linux scripting |

---

## 📊 Key Features

- ✅ Generates 100K–1M+ synthetic blockchain transactions
- ✅ Multi-file ingestion (CSV + JSON merge)
- ✅ Memory-optimised Pandas with dtype tuning (~40% savings)
- ✅ Real-time Kafka producer/consumer with offset management
- ✅ PySpark job with partitioning, caching, and Spark SQL
- ✅ Airflow DAG with retries, SLA monitoring, XCom
- ✅ OLTP (3NF) + OLAP (Star Schema) database design
- ✅ Advanced SQL: window functions, CTEs, anomaly detection
- ✅ Statistical anomaly detection (z-score)
- ✅ Full Linux shell automation with permissions and logging

---

## 🧠 Concepts Covered

ETL vs ELT | OLTP vs OLAP | Hadoop/HDFS | Delta Lake | Cloud (AWS/GCP/Azure) | Data Quality | Lakehouse Architecture

---

## 📄 Documentation

See `docs/project_documentation.pdf` for the full 5-page project report.

---

## 👤 Author

**[ADITI LAL]** | Data Engineering Capstone | 


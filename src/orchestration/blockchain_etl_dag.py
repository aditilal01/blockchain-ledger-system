"""
blockchain_etl_dag.py
Blockchain Transaction Ledger System
---------------------------------------------------
Apache Airflow DAG — orchestrates the full ETL pipeline.
Schedule: daily at midnight UTC.
Demonstrates: task dependencies, retries, SLA, monitoring hooks.

SETUP:
  pip install apache-airflow
  export AIRFLOW_HOME=~/airflow
  airflow db init
  airflow webserver -p 8080 &
  airflow scheduler &
  cp this file → $AIRFLOW_HOME/dags/
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ── Default Task Arguments ────────────────────────────────────────────────────
default_args = {
    "owner":            "data_engineering_team",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,      # set True + configure SMTP for real alerts
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "sla":              timedelta(hours=2),   # alert if DAG takes > 2h
}

# ── DAG Definition ────────────────────────────────────────────────────────────
dag = DAG(
    dag_id="blockchain_ledger_etl",
    default_args=default_args,
    description="End-to-end ETL pipeline for Blockchain Transaction Ledger",
    schedule_interval="0 0 * * *",      # daily at midnight UTC (cron)
    catchup=False,                      # don't backfill missed runs
    max_active_runs=1,                  # prevent concurrent runs
    tags=["blockchain", "etl", "fintech"],
)


# ── Python Callables ──────────────────────────────────────────────────────────

def task_generate_data(**context):
    """Generate synthetic transactions for the pipeline date."""
    import sys
    sys.path.insert(0, "/opt/blockchain_ledger_system")
    from src.ingestion.generate_transactions import generate_batch, generate_metadata
    generate_batch(num_records=100_000)
    generate_metadata()
    context["ti"].xcom_push(key="records_generated", value=100_000)


def task_ingest_and_clean(**context):
    from src.ingestion.ingest_and_clean import run_ingestion_pipeline
    run_ingestion_pipeline()


def task_validate_and_process(**context):
    from src.processing.process_transactions import run_processing_pipeline
    run_processing_pipeline()


def task_load_to_warehouse(**context):
    """Load validated Parquet data into the DuckDB warehouse (OLAP layer)."""
    import duckdb, os
    con = duckdb.connect("data/warehouse/ledger.duckdb")
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_transactions AS
        SELECT * FROM read_parquet('data/processed/transactions_validated.parquet')
        WHERE 1=0
    """)
    con.execute("""
        INSERT INTO fact_transactions
        SELECT * FROM read_parquet('data/processed/transactions_validated.parquet')
    """)
    count = con.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    con.close()
    print(f"[✓] Warehouse loaded: {count:,} rows in fact_transactions")
    context["ti"].xcom_push(key="warehouse_rows", value=count)


def task_data_quality_check(**context):
    """Post-load quality checks — fail DAG if thresholds not met."""
    import duckdb
    con = duckdb.connect("data/warehouse/ledger.duckdb")
    result = con.execute("""
        SELECT
            COUNT(*)                                 AS total,
            SUM(CASE WHEN amount <= 0 THEN 1 END)   AS bad_amount,
            SUM(CASE WHEN tx_hash IS NULL THEN 1 END) AS null_hash
        FROM fact_transactions
    """).fetchone()
    con.close()
    total, bad_amount, null_hash = result
    assert bad_amount == 0,  f"DQ FAIL: {bad_amount} rows with amount ≤ 0"
    assert null_hash  == 0,  f"DQ FAIL: {null_hash} rows with null tx_hash"
    print(f"[✓] DQ checks passed: {total:,} rows, 0 bad amounts, 0 null hashes")


def task_run_analytics(**context):
    from src.analytics.analytics_report import run_analytics
    run_analytics()


# ── Tasks ────────────────────────────────────────────────────────────────────

t_generate = PythonOperator(
    task_id="generate_transactions",
    python_callable=task_generate_data,
    provide_context=True,
    dag=dag,
)

t_ingest = PythonOperator(
    task_id="ingest_and_clean",
    python_callable=task_ingest_and_clean,
    provide_context=True,
    dag=dag,
)

t_process = PythonOperator(
    task_id="validate_and_process",
    python_callable=task_validate_and_process,
    provide_context=True,
    dag=dag,
)

t_warehouse = PythonOperator(
    task_id="load_to_warehouse",
    python_callable=task_load_to_warehouse,
    provide_context=True,
    dag=dag,
)

t_dq = PythonOperator(
    task_id="data_quality_check",
    python_callable=task_data_quality_check,
    provide_context=True,
    dag=dag,
)

t_analytics = PythonOperator(
    task_id="run_analytics",
    python_callable=task_run_analytics,
    provide_context=True,
    dag=dag,
)

# Completion log (BashOperator demo)
t_log = BashOperator(
    task_id="log_completion",
    bash_command='echo "[$(date)] Blockchain ETL pipeline completed successfully" >> /opt/blockchain_ledger_system/logs/pipeline.log',
    dag=dag,
)

# ── Task Dependencies (Pipeline DAG) ─────────────────────────────────────────
#
#  t_generate → t_ingest → t_process → t_warehouse → t_dq → t_analytics → t_log
#
t_generate >> t_ingest >> t_process >> t_warehouse >> t_dq >> t_analytics >> t_log

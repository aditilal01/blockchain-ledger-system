"""
spark_job.py
Blockchain Transaction Ledger System
---------------------------------------------------
PySpark job for large-scale transaction processing.
Demonstrates: DataFrame API, Spark SQL, partitioning, caching.

SETUP:
  pip install pyspark
  OR run via: spark-submit src/processing/spark_job.py
"""

import logging
import os

logger = logging.getLogger(__name__)


def run_spark_job(input_path: str = "data/processed/transactions_clean.parquet"):
    """
    Full PySpark pipeline:
      1. Load Parquet data into Spark DataFrame
      2. Register as temp view for Spark SQL
      3. Compute aggregations using DataFrame API
      4. Partition by network + write output
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # ── SparkSession ─────────────────────────────────────────────────────
        spark = (
            SparkSession.builder
            .appName("BlockchainLedgerSystem")
            .config("spark.sql.shuffle.partitions", "8")   # tuned for local
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        print("[✓] SparkSession initialised")

        # ── Load Data ────────────────────────────────────────────────────────
        df = spark.read.parquet(input_path)
        print(f"[✓] Loaded Spark DataFrame: {df.count():,} rows | {len(df.columns)} columns")

        # Cache frequently-reused DataFrame in memory
        df.cache()

        # ── Spark SQL (register temp view) ───────────────────────────────────
        df.createOrReplaceTempView("transactions")

        top_senders = spark.sql("""
            SELECT
                sender,
                COUNT(*)             AS tx_count,
                SUM(amount)          AS total_amount,
                AVG(amount)          AS avg_amount,
                SUM(fee)             AS total_fees
            FROM transactions
            WHERE status = 'CONFIRMED'
            GROUP BY sender
            ORDER BY total_amount DESC
            LIMIT 20
        """)
        print("[✓] Spark SQL — Top 20 senders computed")

        # ── DataFrame API — Window Function ──────────────────────────────────
        window_spec = Window.partitionBy("network").orderBy(F.desc("amount"))
        df_ranked = df.withColumn("rank_in_network", F.rank().over(window_spec))

        # ── Aggregation ───────────────────────────────────────────────────────
        daily_volume = (
            df.withColumn("date", F.to_date("timestamp"))
            .groupBy("date", "network")
            .agg(
                F.sum("amount").alias("daily_volume"),
                F.count("tx_id").alias("tx_count"),
                F.avg("fee").alias("avg_fee"),
            )
            .orderBy("date")
        )

        # ── PySpark Optimisation: Repartition + Write ─────────────────────────
        # Partition output by 'network' for efficient downstream filtering
        output_path = "data/warehouse/spark_output"
        os.makedirs(output_path, exist_ok=True)

        (
            df_ranked
            .repartition("network")               # coalesce partitions by key
            .write
            .mode("overwrite")
            .partitionBy("network")
            .parquet(output_path)
        )
        print(f"[✓] Spark output written → {output_path}/ (partitioned by network)")

        top_senders.show(5)
        daily_volume.show(5)

        spark.stop()
        print("[✓] SparkSession stopped")

    except ImportError:
        print("[!] PySpark not installed. Run: pip install pyspark")
        print("[!] Spark job skipped — using Pandas fallback for analytics.")
        _pandas_fallback(input_path)


def _pandas_fallback(input_path: str):
    """Pandas equivalent for environments without Spark."""
    import pandas as pd
    df = pd.read_parquet(input_path)
    top = (
        df[df["status"] == "CONFIRMED"]
        .groupby("sender")["amount"]
        .sum()
        .nlargest(20)
        .reset_index()
    )
    print("[✓] Pandas fallback — top senders computed")
    print(top.head())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_spark_job()

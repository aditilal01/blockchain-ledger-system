"""
analytics_report.py
Blockchain Transaction Ledger System
---------------------------------------------------
Final analytics layer: generates summary statistics and
prints a simple terminal report. In production this feeds
a dashboard (Grafana / Metabase / Superset).
"""

import os
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def run_analytics(
    input_path: str = "data/processed/transactions_validated.parquet",
):
    if not os.path.exists(input_path):
        print(f"[!] File not found: {input_path}. Run ingestion first.")
        return

    df = pd.read_parquet(input_path)
    print("\n══════════════════════════════════════════════════════════")
    print("   BLOCKCHAIN LEDGER — ANALYTICS REPORT")
    print("══════════════════════════════════════════════════════════")

    # Overview
    print(f"\n  Total Transactions  : {len(df):>12,}")
    print(f"  Confirmed           : {(df['status']=='CONFIRMED').sum():>12,}")
    print(f"  Pending             : {(df['status']=='PENDING').sum():>12,}")
    print(f"  Failed              : {(df['status']=='FAILED').sum():>12,}")

    print(f"\n  Total Volume (BTC)  : {df['amount'].sum():>16,.4f}")
    print(f"  Avg Tx Amount       : {df['amount'].mean():>16,.6f}")
    print(f"  Max Tx Amount       : {df['amount'].max():>16,.6f}")

    # By network
    print("\n── Volume by Network ───────────────────────────────────")
    net = df.groupby("network")["amount"].agg(["sum", "count"]).rename(
        columns={"sum": "volume", "count": "tx_count"}
    )
    print(net.to_string())

    # By tx type
    print("\n── Volume by Transaction Type ──────────────────────────")
    typ = df.groupby("tx_type")["amount"].agg(["sum", "mean", "count"])
    print(typ.to_string())

    # Anomaly summary
    if "is_anomaly" in df.columns:
        anomalies = df["is_anomaly"].sum()
        print(f"\n── Anomaly Detection ───────────────────────────────────")
        print(f"  Flagged anomalies   : {anomalies:>12,} ({anomalies/len(df)*100:.2f}%)")

    print("\n══════════════════════════════════════════════════════════\n")

    # Save summary CSV
    os.makedirs("data/warehouse", exist_ok=True)
    summary = {
        "total_transactions": len(df),
        "total_volume_btc":   round(float(df["amount"].sum()), 4),
        "avg_amount":         round(float(df["amount"].mean()), 6),
        "confirmed_pct":      round((df["status"]=="CONFIRMED").mean() * 100, 2),
    }
    pd.DataFrame([summary]).to_csv("data/warehouse/analytics_summary.csv", index=False)
    print("[✓] Summary saved → data/warehouse/analytics_summary.csv")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_analytics()

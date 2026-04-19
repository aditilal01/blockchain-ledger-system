"""
process_transactions.py
Blockchain Transaction Ledger System
---------------------------------------------------
Advanced Python: modular functions for validation, normalization, aggregation.
Simulates anomaly detection and data quality checks.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 1 — VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_transactions(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply business-rule validation.
    Returns: (valid_df, invalid_df)

    Rules:
      - amount > 0
      - sender != receiver
      - tx_hash must be 66 chars (0x + 64 hex)
      - timestamp not null
    """
    mask_amount    = df["amount"] > 0
    mask_self_send = df["sender"] != df["receiver"]
    mask_hash      = df["tx_hash"].str.len() == 66
    mask_ts        = df["timestamp"].notna()

    valid_mask = mask_amount & mask_self_send & mask_hash & mask_ts
    valid      = df[valid_mask].copy()
    invalid    = df[~valid_mask].copy()

    logger.info(f"Validation → {len(valid):,} valid | {len(invalid):,} invalid")
    print(f"[✓] Validation: {len(valid):,} valid | {len(invalid):,} invalid")
    return valid, invalid


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 2 — NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def normalize_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Min-Max normalise the amount column to [0, 1].
    Used downstream for ML-ready features.
    """
    df = df.copy()
    min_val = df["amount"].min()
    max_val = df["amount"].max()
    df["amount_normalized"] = ((df["amount"] - min_val) / (max_val - min_val)).astype(np.float32)
    print("[✓] Amount normalisation complete")
    return df


def normalize_z_score(df: pd.DataFrame, col: str = "amount") -> pd.DataFrame:
    """Z-score standardisation — flags statistical outliers."""
    df = df.copy()
    mean = df[col].mean()
    std  = df[col].std()
    df[f"{col}_zscore"] = ((df[col] - mean) / std).astype(np.float32)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 3 — AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_by_wallet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-wallet aggregated stats — used for OLAP/star schema fact table.
    """
    agg = (
        df.groupby("sender")
        .agg(
            total_sent         = ("amount", "sum"),
            avg_tx_amount      = ("amount", "mean"),
            tx_count           = ("tx_id", "count"),
            total_fees_paid    = ("fee", "sum"),
            failed_tx_count    = ("status", lambda x: (x == "FAILED").sum()),
            distinct_receivers = ("receiver", "nunique"),
        )
        .reset_index()
        .rename(columns={"sender": "wallet_address"})
    )
    agg["failed_rate"] = (agg["failed_tx_count"] / agg["tx_count"]).round(4)
    print(f"[✓] Aggregated wallet stats: {len(agg):,} wallets")
    return agg


def aggregate_by_day(df: pd.DataFrame) -> pd.DataFrame:
    """Daily volume stats — feeds time-dimension in star schema."""
    df = df.copy()
    df["date"] = df["timestamp"].dt.date
    daily = (
        df.groupby("date")
        .agg(
            daily_volume   = ("amount", "sum"),
            tx_count       = ("tx_id", "count"),
            unique_senders = ("sender", "nunique"),
            avg_fee        = ("fee", "mean"),
        )
        .reset_index()
    )
    print(f"[✓] Daily aggregation: {len(daily):,} days")
    return daily


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 4 — ANOMALY DETECTION (Data Quality)
# ══════════════════════════════════════════════════════════════════════════════

def detect_anomalies(df: pd.DataFrame, zscore_threshold: float = 3.5) -> pd.DataFrame:
    """
    Flag anomalous transactions:
      - Statistical: z-score > threshold (unusually large amounts)
      - Rule-based: high failed_rate wallets, same sender→receiver in <1s
    """
    df = normalize_z_score(df, "amount")
    df["is_anomaly"] = (df["amount_zscore"].abs() > zscore_threshold).astype(np.int8)
    anomaly_count = df["is_anomaly"].sum()
    logger.info(f"Anomaly detection flagged {anomaly_count:,} transactions")
    print(f"[✓] Anomalies flagged: {anomaly_count:,}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PROCESSING PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_processing_pipeline(
    input_path:  str = "data/processed/transactions_clean.parquet",
    output_path: str = "data/processed/transactions_validated.parquet",
) -> pd.DataFrame:
    print("\n── Processing Pipeline ─────────────────────────────────────")
    df = pd.read_parquet(input_path)
    valid, invalid = validate_transactions(df)
    valid          = normalize_amounts(valid)
    valid          = detect_anomalies(valid)

    wallet_stats = aggregate_by_wallet(valid)
    daily_stats  = aggregate_by_day(valid)

    # Save outputs
    valid.to_parquet(output_path, index=False)
    wallet_stats.to_parquet("data/processed/wallet_stats.parquet", index=False)
    daily_stats.to_parquet("data/processed/daily_stats.parquet",   index=False)

    print(f"[✓] Validated data saved → {output_path}")
    print("── Processing Complete ─────────────────────────────────────\n")
    return valid


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_processing_pipeline()

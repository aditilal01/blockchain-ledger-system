"""
ingest_and_clean.py
Blockchain Transaction Ledger System
---------------------------------------------------
Batch ingestion: reads CSV + JSON, cleans data, merges datasets.
Covers: Pandas, NumPy, memory optimization, multi-file merge.
"""

import os
import json
import logging
import numpy as np
import pandas as pd
from typing import Tuple

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# 1. LOAD DATA (Multi-file, optimised dtypes for memory)
# ══════════════════════════════════════════════════════════════════════════════

def load_transactions(csv_path: str) -> pd.DataFrame:
    """
    Load transaction CSV with optimised dtypes.
    Using categorical columns and float32 cuts memory by ~40% vs defaults.
    """
    dtype_map = {
        "tx_id":    np.int32,
        "block_id": np.int32,
        "amount":   np.float32,
        "fee":      np.float32,
        "tx_type":  "category",
        "status":   "category",
        "network":  "category",
    }
    df = pd.read_csv(csv_path, dtype=dtype_map, parse_dates=["timestamp"])
    logger.info(f"Loaded {len(df):,} rows from {csv_path}")
    print(f"[✓] Loaded {len(df):,} transactions | Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
    return df


def load_wallet_metadata(json_path: str) -> pd.DataFrame:
    """Load wallet metadata from JSON into DataFrame."""
    with open(json_path) as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df["risk_score"] = df["risk_score"].astype(np.float32)
    logger.info(f"Loaded {len(df):,} wallet records from {json_path}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 2. CLEAN DATA
# ══════════════════════════════════════════════════════════════════════════════

def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean pipeline:
      1. Drop exact duplicates
      2. Impute missing amounts with median (per tx_type)
      3. Remove zero or negative amounts
      4. Standardise string columns
    """
    original_len = len(df)

    # Duplicate removal
    df = df.drop_duplicates(subset=["tx_hash"])

    # Impute missing amounts with per-type median
    medians = df.groupby("tx_type")["amount"].transform("median")
    df["amount"] = df["amount"].fillna(medians)

    # Remove invalid amounts
    df = df[df["amount"] > 0]

    # Normalise addresses to lowercase
    df["sender"]   = df["sender"].str.lower().str.strip()
    df["receiver"] = df["receiver"].str.lower().str.strip()

    removed = original_len - len(df)
    logger.info(f"Cleaning removed {removed:,} rows")
    print(f"[✓] Cleaned: {removed:,} rows removed | {len(df):,} rows remaining")
    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# 3. MERGE DATASETS
# ══════════════════════════════════════════════════════════════════════════════

def merge_with_metadata(
    transactions: pd.DataFrame, metadata: pd.DataFrame
) -> pd.DataFrame:
    """
    Left-join transaction data with wallet metadata on sender address.
    Result enriches each transaction with KYC status, country, risk_score.
    """
    enriched = transactions.merge(
        metadata.rename(columns={"wallet_address": "sender"}),
        on="sender",
        how="left",
    )
    logger.info("Merged transactions with wallet metadata")
    print(f"[✓] Merged dataset shape: {enriched.shape}")
    return enriched


# ══════════════════════════════════════════════════════════════════════════════
# 4. FEATURE ENGINEERING (NumPy operations)
# ══════════════════════════════════════════════════════════════════════════════

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed columns using NumPy for performance."""
    df["amount_usd"]       = (df["amount"] * 45_000).astype(np.float32)   # mock BTC price
    df["fee_pct"]          = np.round(df["fee"] / df["amount"] * 100, 4).astype(np.float32)
    df["is_large_tx"]      = (df["amount"] > 1.0).astype(np.int8)
    df["hour_of_day"]      = df["timestamp"].dt.hour.astype(np.int8)
    df["day_of_week"]      = df["timestamp"].dt.dayofweek.astype(np.int8)
    df["amount_log"]       = np.log1p(df["amount"]).astype(np.float32)    # log-normalize
    print("[✓] Feature engineering complete")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 5. SAVE PROCESSED DATA
# ══════════════════════════════════════════════════════════════════════════════

def save_processed(df: pd.DataFrame, output_path: str = "data/processed/transactions_clean.parquet"):
    """Save as Parquet for efficient downstream reads (columnar, compressed)."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False, compression="snappy")
    size_mb = os.path.getsize(output_path) / 1e6
    logger.info(f"Saved processed data → {output_path} ({size_mb:.1f} MB)")
    print(f"[✓] Saved processed file → {output_path}  ({size_mb:.1f} MB)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE RUN
# ══════════════════════════════════════════════════════════════════════════════

def run_ingestion_pipeline(
    csv_path:  str = "data/raw/transactions_batch.csv",
    json_path: str = "data/raw/wallet_metadata.json",
    output:    str = "data/processed/transactions_clean.parquet",
) -> pd.DataFrame:
    print("\n── Ingestion & Cleaning Pipeline ──────────────────────────")
    transactions = load_transactions(csv_path)
    metadata     = load_wallet_metadata(json_path)
    cleaned      = clean_transactions(transactions)
    enriched     = merge_with_metadata(cleaned, metadata)
    final        = add_features(enriched)
    save_processed(final, output)
    print("── Pipeline Complete ───────────────────────────────────────\n")
    return final


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_ingestion_pipeline()

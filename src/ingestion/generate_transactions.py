"""
generate_transactions.py
Blockchain Transaction Ledger System
---------------------------------------------------
Generates synthetic blockchain transactions and saves to CSV/JSON.
Simulates 1M+ rows with realistic sender/receiver/amount/hash/timestamp.
"""

import hashlib
import random
import time
import json
import csv
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict

# ── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
WALLETS = [f"0x{hashlib.md5(str(i).encode()).hexdigest()[:40]}" for i in range(500)]
TRANSACTION_TYPES = ["TRANSFER", "SMART_CONTRACT", "MINING_REWARD", "STAKE", "UNSTAKE"]
STATUS_VALUES     = ["CONFIRMED", "PENDING", "FAILED"]


def compute_tx_hash(sender: str, receiver: str, amount: float, timestamp: str) -> str:
    """Generate SHA-256 hash for a transaction (mimics real blockchain hashing)."""
    raw = f"{sender}{receiver}{amount:.8f}{timestamp}{random.getrandbits(32)}"
    return "0x" + hashlib.sha256(raw.encode()).hexdigest()


def generate_transaction(tx_id: int, base_time: datetime) -> Dict:
    """Build a single transaction record."""
    sender    = random.choice(WALLETS)
    receiver  = random.choice([w for w in WALLETS if w != sender])
    amount    = round(random.uniform(0.0001, 500.0), 8)
    fee       = round(amount * random.uniform(0.001, 0.01), 8)
    timestamp = (base_time + timedelta(seconds=tx_id * random.uniform(0.5, 3))).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    tx_hash   = compute_tx_hash(sender, receiver, amount, timestamp)
    block_id  = (tx_id // 10) + 1                        # ~10 tx per block

    # Inject ~3% missing values to simulate real-world dirty data
    if random.random() < 0.03:
        amount = None

    return {
        "tx_id":          tx_id,
        "tx_hash":        tx_hash,
        "block_id":       block_id,
        "sender":         sender,
        "receiver":       receiver,
        "amount":         amount,
        "fee":            fee,
        "tx_type":        random.choice(TRANSACTION_TYPES),
        "status":         random.choices(STATUS_VALUES, weights=[0.85, 0.10, 0.05])[0],
        "timestamp":      timestamp,
        "network":        random.choice(["mainnet", "testnet"]),
    }


def generate_batch(num_records: int = 100_000, output_dir: str = "data/raw") -> str:
    """Generate a batch of transactions and write to CSV."""
    os.makedirs(output_dir, exist_ok=True)
    base_time   = datetime(2024, 1, 1)
    output_file = os.path.join(output_dir, "transactions_batch.csv")

    logger.info(f"Starting generation of {num_records:,} transactions ...")
    start = time.time()

    with open(output_file, "w", newline="") as f:
        writer = None
        for i in range(num_records):
            record = generate_transaction(i + 1, base_time)
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=record.keys())
                writer.writeheader()
            writer.writerow(record)

    elapsed = time.time() - start
    logger.info(f"Generated {num_records:,} records in {elapsed:.2f}s → {output_file}")
    print(f"[✓] Generated {num_records:,} transactions → {output_file}  ({elapsed:.2f}s)")
    return output_file


def generate_metadata(output_dir: str = "data/raw") -> str:
    """Generate wallet metadata as JSON (for multi-file merge demo)."""
    os.makedirs(output_dir, exist_ok=True)
    metadata = [
        {
            "wallet_address": w,
            "kyc_verified":   random.choice([True, False]),
            "country":        random.choice(["IN", "US", "UK", "SG", "AE", "DE"]),
            "risk_score":     round(random.uniform(0, 10), 2),
        }
        for w in WALLETS
    ]
    out = os.path.join(output_dir, "wallet_metadata.json")
    with open(out, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"[✓] Generated wallet metadata → {out}")
    return out


if __name__ == "__main__":
    generate_batch(num_records=100_000)
    generate_metadata()

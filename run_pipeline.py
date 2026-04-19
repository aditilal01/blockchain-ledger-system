"""
run_pipeline.py
Blockchain Transaction Ledger System
---------------------------------------------------
Single entry point to run the full end-to-end pipeline.
Usage:  python run_pipeline.py [--records N] [--skip-spark]
"""

import argparse
import logging
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Blockchain Ledger Pipeline")
    parser.add_argument("--records",     type=int, default=100_000,
                        help="Number of transactions to generate (default: 100,000)")
    parser.add_argument("--skip-spark",  action="store_true",
                        help="Skip PySpark job (use if Spark not installed)")
    parser.add_argument("--skip-kafka",  action="store_true",
                        help="Skip Kafka producer/consumer")
    args = parser.parse_args()

    logger.info("═" * 60)
    logger.info("  BLOCKCHAIN LEDGER — FULL PIPELINE START")
    logger.info("═" * 60)

    # Step 1: Generate data
    logger.info("STEP 1 | Data Generation")
    from src.ingestion.generate_transactions import generate_batch, generate_metadata
    generate_batch(num_records=args.records)
    generate_metadata()

    # Step 2: Ingestion + Cleaning
    logger.info("STEP 2 | Ingestion & Cleaning")
    from src.ingestion.ingest_and_clean import run_ingestion_pipeline
    run_ingestion_pipeline()

    # Step 3: Processing + Validation
    logger.info("STEP 3 | Processing & Validation")
    from src.processing.process_transactions import run_processing_pipeline
    run_processing_pipeline()

    # Step 4: Kafka Streaming Simulation
    if not args.skip_kafka:
        logger.info("STEP 4 | Kafka Streaming (Simulation)")
        from src.streaming.kafka_producer import run_producer
        from src.streaming.kafka_consumer import run_consumer
        run_producer(num_events=200)
        run_consumer(max_messages=200)
    else:
        logger.info("STEP 4 | Kafka skipped")

    # Step 5: Spark Job
    if not args.skip_spark:
        logger.info("STEP 5 | Spark Job")
        from src.processing.spark_job import run_spark_job
        run_spark_job()
    else:
        logger.info("STEP 5 | Spark skipped")

    # Step 6: Analytics
    logger.info("STEP 6 | Analytics Report")
    from src.analytics.analytics_report import run_analytics
    run_analytics()

    logger.info("═" * 60)
    logger.info("  PIPELINE COMPLETE ✓")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()

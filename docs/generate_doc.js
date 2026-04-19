const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  HeadingLevel, AlignmentType, BorderStyle, WidthType, ShadingType,
  LevelFormat, PageNumber, Footer, Header, PageBreak
} = require("docx");
const fs = require("fs");

// ── Helpers ──────────────────────────────────────────────────────────────────
const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: border, bottom: border, left: border, right: border };
const cellMargins = { top: 80, bottom: 80, left: 120, right: 120 };

function h1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    spacing: { before: 300, after: 120 },
    children: [new TextRun({ text, bold: true, size: 32, font: "Arial", color: "1A237E" })],
  });
}
function h2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    spacing: { before: 200, after: 100 },
    children: [new TextRun({ text, bold: true, size: 26, font: "Arial", color: "283593" })],
  });
}
function body(text, opts = {}) {
  return new Paragraph({
    spacing: { before: 60, after: 80 },
    children: [new TextRun({ text, size: 22, font: "Arial", ...opts })],
  });
}
function bullet(text) {
  return new Paragraph({
    numbering: { reference: "bullets", level: 0 },
    spacing: { before: 40, after: 40 },
    children: [new TextRun({ text, size: 22, font: "Arial" })],
  });
}
function code(text) {
  return new Paragraph({
    spacing: { before: 40, after: 40 },
    shading: { fill: "F5F5F5", type: ShadingType.CLEAR },
    indent: { left: 720 },
    children: [new TextRun({ text, size: 18, font: "Courier New", color: "333333" })],
  });
}
function rule() {
  return new Paragraph({
    spacing: { before: 120, after: 120 },
    border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: "E8EAF6", space: 1 } },
    children: [new TextRun("")],
  });
}
function spacer() {
  return new Paragraph({ spacing: { before: 60, after: 60 }, children: [new TextRun("")] });
}

function makeTable(headers, rows, colWidths) {
  const totalWidth = colWidths.reduce((a, b) => a + b, 0);
  return new Table({
    width: { size: totalWidth, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: [
      new TableRow({
        tableHeader: true,
        children: headers.map((h, i) =>
          new TableCell({
            borders,
            width: { size: colWidths[i], type: WidthType.DXA },
            shading: { fill: "1A237E", type: ShadingType.CLEAR },
            margins: cellMargins,
            children: [new Paragraph({
              children: [new TextRun({ text: h, bold: true, color: "FFFFFF", size: 20, font: "Arial" })],
            })],
          })
        ),
      }),
      ...rows.map((row, ri) =>
        new TableRow({
          children: row.map((cell, ci) =>
            new TableCell({
              borders,
              width: { size: colWidths[ci], type: WidthType.DXA },
              shading: { fill: ri % 2 === 0 ? "FFFFFF" : "EEF0FB", type: ShadingType.CLEAR },
              margins: cellMargins,
              children: [new Paragraph({
                children: [new TextRun({ text: String(cell), size: 20, font: "Arial" })],
              })],
            })
          ),
        })
      ),
    ],
  });
}

// ── Document ──────────────────────────────────────────────────────────────────
const doc = new Document({
  numbering: {
    config: [{
      reference: "bullets",
      levels: [{
        level: 0, format: LevelFormat.BULLET, text: "•",
        alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } },
      }],
    }],
  },
  styles: {
    default: { document: { run: { font: "Arial", size: 22 } } },
    paragraphStyles: [
      {
        id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 32, bold: true, font: "Arial", color: "1A237E" },
        paragraph: { spacing: { before: 300, after: 120 }, outlineLevel: 0 },
      },
      {
        id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, font: "Arial", color: "283593" },
        paragraph: { spacing: { before: 200, after: 100 }, outlineLevel: 1 },
      },
    ],
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1440, right: 1300, bottom: 1440, left: 1300 },
      },
    },
    headers: {
      default: new Header({
        children: [new Paragraph({
          border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: "1A237E", space: 1 } },
          spacing: { after: 80 },
          children: [
            new TextRun({ text: "Blockchain Transaction Ledger System", size: 18, font: "Arial", color: "1A237E" }),
            new TextRun({ text: "  |  Data Engineering Capstone Project", size: 18, font: "Arial", color: "888888" }),
          ],
        })],
      }),
    },
    footers: {
      default: new Footer({
        children: [new Paragraph({
          border: { top: { style: BorderStyle.SINGLE, size: 4, color: "E8EAF6", space: 1 } },
          spacing: { before: 60 },
          alignment: AlignmentType.CENTER,
          children: [
            new TextRun({ text: "Page ", size: 18, font: "Arial", color: "888888" }),
            new TextRun({ children: [PageNumber.CURRENT], size: 18, font: "Arial", color: "888888" }),
            new TextRun({ text: " of ", size: 18, font: "Arial", color: "888888" }),
            new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 18, font: "Arial", color: "888888" }),
          ],
        })],
      }),
    },
    children: [
      // ── COVER ──────────────────────────────────────────────────────────────
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { before: 720, after: 160 },
        children: [new TextRun({ text: "BLOCKCHAIN TRANSACTION LEDGER SYSTEM", bold: true, size: 52, font: "Arial", color: "1A237E" })],
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { before: 80, after: 80 },
        children: [new TextRun({ text: "End-to-End Data Engineering Capstone Project", size: 28, font: "Arial", color: "5C6BC0", italics: true })],
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { before: 80, after: 480 },
        children: [new TextRun({ text: "──────────────────────────────────────────────────────", size: 22, color: "E8EAF6" })],
      }),

      makeTable(
        ["Field", "Details"],
        [
          ["Project Title",   "Blockchain Transaction Ledger System"],
          ["Domain",          "FinTech / Blockchain Analytics"],
          ["Type",            "Data Engineering (Batch + Streaming)"],
          ["Tech Stack",      "Python, Pandas, PySpark, Kafka, Airflow, DuckDB, SQL"],
          ["Submitted By",    "[Your Name] — [Roll Number]"],
          ["Institution",     "[College Name]"],
          ["Year",            "2024–2025"],
        ],
        [3200, 5760]
      ),

      new Paragraph({ children: [new PageBreak()] }),

      // ── SECTION 1: PROBLEM STATEMENT ──────────────────────────────────────
      h1("1. Problem Statement"),
      body("Modern financial systems — especially blockchain-based platforms — process millions of transactions per day. Raw transaction data arrives in varied formats, at high velocity, and often contains errors, missing values, or anomalies. Without a structured data pipeline, it is impossible to:"),
      spacer(),
      bullet("Detect fraudulent or anomalous transactions in near real-time"),
      bullet("Generate regulatory compliance reports across time windows"),
      bullet("Understand wallet behaviour, volume trends, and fee patterns"),
      bullet("Store data efficiently for both transactional (OLTP) and analytical (OLAP) workloads"),
      spacer(),
      body("This project solves these problems by building a production-grade, end-to-end data engineering pipeline that simulates a real blockchain ledger — covering data generation, batch ingestion, stream processing, warehousing, orchestration, and analytics."),
      rule(),

      // ── SECTION 2: SOLUTION OVERVIEW ──────────────────────────────────────
      h1("2. Solution Overview"),
      body("The solution is a six-stage pipeline modelled on real-world data engineering systems used in fintech companies:"),
      spacer(),

      makeTable(
        ["Stage", "Component", "Technology"],
        [
          ["1. Generation",    "Synthetic blockchain transactions (1M+ scale)",   "Python, hashlib"],
          ["2. Ingestion",     "Batch CSV + JSON ingest, clean, merge",            "Pandas, NumPy"],
          ["3. Processing",    "Validate, normalise, anomaly detection",           "Python, PySpark"],
          ["4. Streaming",     "Real-time transaction events",                     "Apache Kafka"],
          ["5. Orchestration", "Scheduled DAG with retries and monitoring",       "Apache Airflow"],
          ["6. Analytics",     "OLAP queries, reports, dashboards",                "DuckDB, SQL"],
        ],
        [2200, 4000, 2760]
      ),

      spacer(),
      body("The pipeline is designed following the ETL (Extract-Transform-Load) pattern for batch workloads, and an ELT variant for the cloud/lakehouse layer where raw data lands first and transformations happen in-warehouse."),
      rule(),

      new Paragraph({ children: [new PageBreak()] }),

      // ── SECTION 3: ARCHITECTURE ───────────────────────────────────────────
      h1("3. Architecture & Pipeline Design"),

      h2("3.1  Pipeline Flow Diagram"),
      body("[Screenshot placeholder: pipeline_architecture.png]", { italics: true, color: "888888" }),
      spacer(),
      code("Data Generator  →  Raw CSV/JSON  →  Pandas Cleaning  →  Parquet"),
      code("                                          ↓"),
      code("                          Kafka Producer → Topic → Consumer"),
      code("                                          ↓"),
      code("                              PySpark Batch Job"),
      code("                                          ↓"),
      code("                          DuckDB Star Schema Warehouse"),
      code("                                          ↓"),
      code("                     Airflow DAG (schedules all stages)"),
      code("                                          ↓"),
      code("                          Analytics Report / Dashboard"),
      spacer(),

      h2("3.2  OLTP vs OLAP Design"),
      body("Two separate database schemas are maintained:"),
      spacer(),
      makeTable(
        ["Aspect", "OLTP (PostgreSQL)", "OLAP (DuckDB / Star Schema)"],
        [
          ["Purpose",      "Record real-time transactions",     "Analytical reporting & aggregations"],
          ["Schema",       "3rd Normal Form (3NF)",             "Star Schema (denormalised)"],
          ["Tables",       "wallets, blocks, transactions",     "fact_transactions + 4 dimensions"],
          ["Query type",   "INSERT, UPDATE, point lookups",     "SELECT with GROUP BY, window fns"],
          ["Latency",      "Milliseconds",                      "Seconds (acceptable for analytics)"],
          ["Optimisation", "Row-based storage, indexes",        "Columnar storage, partitioned files"],
        ],
        [1800, 3400, 3760]
      ),

      spacer(),
      h2("3.3  Star Schema (Data Warehouse)"),
      body("The OLAP layer uses a Star Schema with one central fact table and four dimension tables:"),
      spacer(),
      bullet("fact_transactions — amount, fee, status, FK references to all dims"),
      bullet("dim_date — date_key, year, month, quarter, day_of_week"),
      bullet("dim_wallet — wallet_address, kyc_verified, country_code, risk_score"),
      bullet("dim_tx_type — tx_type, category, description"),
      bullet("dim_network — network name, chain_id"),

      rule(),
      new Paragraph({ children: [new PageBreak()] }),

      // ── SECTION 4: IMPLEMENTATION ─────────────────────────────────────────
      h1("4. Implementation Details"),

      h2("4.1  Data Generation (Python + hashlib)"),
      body("The generator creates realistic transactions with 500 unique wallets, randomised amounts (0.0001–500 BTC), SHA-256 transaction hashes, and ~3% missing values to simulate real-world dirty data."),
      code("tx_hash = '0x' + hashlib.sha256(f'{sender}{receiver}{amount}{ts}'.encode()).hexdigest()"),
      spacer(),

      h2("4.2  Batch Ingestion & Cleaning (Pandas + NumPy)"),
      body("Memory-optimised ingestion uses categorical dtypes and float32 to cut RAM usage by ~40% vs defaults. The cleaning pipeline drops duplicates, imputes missing amounts using per-type medians, and removes invalid rows."),
      code("dtype_map = {'tx_type': 'category', 'status': 'category', 'amount': np.float32}"),
      code("df = pd.read_csv(path, dtype=dtype_map, parse_dates=['timestamp'])"),
      spacer(),

      h2("4.3  Kafka Streaming (Producer / Consumer)"),
      body("The Kafka producer partitions events by network (mainnet/testnet) for ordered per-partition delivery. The consumer uses manual offset commits every 100 messages for exactly-once processing semantics."),
      code("producer.send(topic=TOPIC, key=event['network'], value=event)   # partition by key"),
      code("consumer.commit()  # manual offset — called every 100 messages"),
      spacer(),

      h2("4.4  PySpark Processing"),
      body("The Spark job loads the cleaned Parquet file, registers it as a temp view for Spark SQL, computes window-function rankings per network, and writes output partitioned by network for efficient downstream filtering."),
      code("df.cache()   # cache frequently-reused DataFrame"),
      code("df.repartition('network').write.partitionBy('network').parquet(output_path)"),
      spacer(),

      h2("4.5  Apache Airflow DAG"),
      body("The DAG 'blockchain_ledger_etl' runs daily at midnight UTC. It has 7 tasks in sequence with 2 automatic retries per task, a 2-hour SLA alert, and XCom to pass row counts between tasks."),
      code("schedule_interval='0 0 * * *'   # daily cron"),
      code("retries=2, retry_delay=timedelta(minutes=5)"),
      code("sla=timedelta(hours=2)"),
      spacer(),

      h2("4.6  Advanced SQL — Window Functions"),
      body("The warehouse layer supports advanced analytics. Example: running total volume per network using SUM() OVER with UNBOUNDED PRECEDING:"),
      code("SUM(SUM(f.amount)) OVER ("),
      code("    PARTITION BY n.network ORDER BY d.full_date"),
      code("    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
      code(") AS running_volume"),
      rule(),

      new Paragraph({ children: [new PageBreak()] }),

      // ── SECTION 5: ETL vs ELT ─────────────────────────────────────────────
      h1("5. ETL vs ELT — Practical Comparison"),

      makeTable(
        ["Aspect", "ETL (This Project — Batch)", "ELT (Cloud/Lakehouse Variant)"],
        [
          ["Order",       "Extract → Transform → Load",          "Extract → Load → Transform"],
          ["Where",       "Transform in pipeline (Pandas/Spark)", "Transform inside warehouse (SQL/dbt)"],
          ["Best for",    "Known schema, structured data",        "Raw data lakes, schema-on-read"],
          ["Example",     "CSV → clean → Parquet → DuckDB",       "CSV → S3 raw → Redshift → dbt"],
          ["Latency",     "Higher (batch)",                       "Lower (raw lands fast)"],
          ["Flexibility", "Less (schema enforced early)",         "More (schema applied at query time)"],
        ],
        [1800, 3400, 3760]
      ),

      spacer(),
      body("This project implements ETL for the batch pipeline. The Kafka → consumer path is an ELT variant — raw events land in a JSON store first, then are transformed during the Spark/DuckDB stage."),
      rule(),

      // ── SECTION 6: CLOUD & CONCEPTS ───────────────────────────────────────
      h1("6. Cloud Architecture (Conceptual Mapping)"),

      makeTable(
        ["Component", "AWS", "GCP", "Azure"],
        [
          ["Object Storage", "S3",           "Cloud Storage (GCS)", "Azure Blob Storage"],
          ["Data Warehouse", "Redshift",      "BigQuery",            "Synapse Analytics"],
          ["Compute",        "EC2 / EMR",     "Dataproc",            "HDInsight"],
          ["Streaming",      "Kinesis",       "Pub/Sub",             "Event Hubs"],
          ["Orchestration",  "MWAA (Airflow)","Cloud Composer",      "Data Factory"],
        ],
        [2200, 2220, 2220, 2320]
      ),

      spacer(),
      body("In a production cloud deployment, raw CSVs would land in S3/GCS, Spark jobs would run on EMR/Dataproc, and the warehouse would be BigQuery or Redshift. This project simulates all these layers locally using open-source equivalents."),

      h2("6.1  Delta Lake / Lakehouse Concept"),
      body("A Lakehouse combines the flexibility of a data lake (raw files on S3/GCS) with the ACID transactional guarantees of a warehouse. Delta Lake (by Databricks) and Apache Iceberg achieve this by adding a transaction log on top of Parquet files — enabling time-travel queries, schema evolution, and MERGE operations."),
      rule(),

      // ── SECTION 7: DATA QUALITY ───────────────────────────────────────────
      h1("7. Data Quality & Anomaly Detection"),
      body("Data quality is enforced at two stages:"),
      spacer(),
      bullet("Pipeline-level validation (process_transactions.py): business rules — amount > 0, sender != receiver, hash length = 66, timestamp not null."),
      bullet("Statistical anomaly detection: z-score computed per transaction. Any row with |z| > 3.5 standard deviations from the mean is flagged as is_anomaly = 1."),
      bullet("Post-load DQ checks in Airflow: the data_quality_check task asserts zero bad-amount and zero null-hash rows after warehouse load. Pipeline fails if violated."),
      rule(),

      // ── SECTION 8: HADOOP/HDFS CONCEPTS ──────────────────────────────────
      h1("8. Hadoop & HDFS (Conceptual)"),
      body("Hadoop HDFS provides distributed file storage for large-scale data pipelines. Key components:"),
      spacer(),
      bullet("NameNode: Master node — stores filesystem metadata (directory tree, block locations). Single point of truth."),
      bullet("DataNode: Worker nodes — store actual data blocks (default 128 MB each). Replicated 3x for fault tolerance."),
      bullet("In this project: HDFS is conceptually represented by the data/ directory. In production, Parquet files would be stored on HDFS or S3-compatible object storage."),
      bullet("Spark reads from HDFS natively: spark.read.parquet('hdfs://namenode:9000/data/transactions/')"),
      rule(),

      new Paragraph({ children: [new PageBreak()] }),

      // ── SECTION 9: TECH STACK ─────────────────────────────────────────────
      h1("9. Tech Stack & Justification"),

      makeTable(
        ["Technology", "Version", "Role", "Why Chosen"],
        [
          ["Python",          "3.8+",  "Core language",       "Industry standard for data engineering"],
          ["Pandas",          "2.0",   "ETL / Cleaning",      "Fast in-memory tabular processing"],
          ["NumPy",           "1.24",  "Numerical ops",       "Vectorised operations, memory efficiency"],
          ["Apache Kafka",    "3.x",   "Streaming",           "Industry-standard event streaming platform"],
          ["Apache Spark",    "3.4",   "Batch processing",    "Scales to petabyte workloads"],
          ["Apache Airflow",  "2.7",   "Orchestration",       "DAG-based pipeline scheduling & monitoring"],
          ["DuckDB",          "0.9",   "OLAP warehouse",      "In-process analytical SQL, Parquet-native"],
          ["PostgreSQL",      "15",    "OLTP storage",        "ACID-compliant transactional database"],
          ["Bash",            "5.x",   "Linux automation",    "File permissions, logging, CI/CD hooks"],
        ],
        [1900, 900, 1900, 3260]
      ),

      rule(),

      // ── SECTION 10: FEATURES ──────────────────────────────────────────────
      h1("10. Key Features & Unique Points"),

      bullet("Generates 100K–1M+ realistic blockchain transactions with SHA-256 hashing"),
      bullet("Memory-optimised ingestion (categorical dtypes) reduces RAM by ~40%"),
      bullet("Multi-file merge: CSV transaction data joined with JSON wallet metadata"),
      bullet("Kafka partitioning by network key enables ordered, parallel stream processing"),
      bullet("Spark output partitioned by network for efficient predicate pushdown"),
      bullet("Airflow DAG with XCom, retries, SLA, and post-load data quality assertion"),
      bullet("Dual schema design: 3NF OLTP for writes + Star Schema OLAP for analytics"),
      bullet("Advanced SQL: window functions (RANK, SUM OVER), CTEs, LAG for spike detection"),
      bullet("Statistical anomaly detection (z-score > 3.5) flags suspicious transactions"),
      bullet("End-to-end pipeline runnable with a single command: python run_pipeline.py"),

      rule(),

      // ── SECTION 11: FUTURE IMPROVEMENTS ──────────────────────────────────
      h1("11. Future Improvements"),

      bullet("Deploy on AWS: S3 (raw storage) + EMR (Spark) + Redshift (warehouse) + MWAA (Airflow)"),
      bullet("Migrate to Delta Lake for ACID transactions and time-travel queries"),
      bullet("Add a real-time fraud scoring ML model (XGBoost) triggered from the Kafka consumer"),
      bullet("Build a Grafana or Apache Superset dashboard for live analytics"),
      bullet("Implement schema registry (Confluent) for Kafka message validation"),
      bullet("Add dbt (data build tool) for warehouse transformation documentation"),

      rule(),

      // ── SECTION 12: VIVA Q&A ─────────────────────────────────────────────
      h1("12. Viva Preparation — Q&A"),
      spacer(),

      makeTable(
        ["Question", "Crisp Answer"],
        [
          ["What is the pipeline flow?",
           "Generate → Ingest (CSV/JSON) → Clean (Pandas) → Validate → Kafka → Spark → Warehouse (DuckDB) → Analytics. Orchestrated by Airflow daily."],
          ["Why Parquet over CSV?",
           "Parquet is columnar, compressed (Snappy), and ~5-10x faster for analytical reads. It also preserves dtypes and supports predicate pushdown."],
          ["What is a star schema?",
           "One central fact table (transactions) surrounded by dimension tables (date, wallet, type, network). Denormalised for fast GROUP BY aggregations."],
          ["ETL vs ELT?",
           "ETL transforms before loading (done here in Pandas/Spark). ELT loads raw data first, transforms inside the warehouse using SQL/dbt. ELT suits cloud lakehouses."],
          ["Why Kafka partitioning?",
           "Partitioning by network key ensures all mainnet events go to the same partition, preserving event order and enabling parallel consumer groups."],
          ["What is manual offset commit?",
           "Consumer explicitly calls commit() after processing a batch. Prevents message loss and enables exactly-once processing if the consumer crashes mid-batch."],
          ["What does Airflow XCom do?",
           "XCom lets tasks share small values (like row counts) across the DAG. Used here to pass records_generated from the generation task to downstream tasks."],
          ["How does anomaly detection work?",
           "Z-score: z = (value - mean) / std. Any transaction with |z| > 3.5 is statistically anomalous (>3.5 standard deviations from mean). Flagged as is_anomaly = 1."],
          ["OLTP vs OLAP?",
           "OLTP: optimised for writes, 3NF, row storage (PostgreSQL). OLAP: optimised for reads/aggregations, star schema, columnar storage (DuckDB/Redshift)."],
          ["What is a Lakehouse?",
           "Combines data lake (raw files on S3) + warehouse (ACID SQL). Delta Lake adds transaction log on Parquet, enabling MERGE, time-travel, and schema evolution."],
          ["What does NameNode do in HDFS?",
           "NameNode is the master metadata server — tracks which DataNode holds each block. DataNodes store the actual 128 MB data blocks, replicated 3x."],
          ["How did you optimise Pandas memory?",
           "Used category dtype for low-cardinality string columns (tx_type, status, network) and float32 instead of float64 for numeric columns, saving ~40% RAM."],
        ],
        [3600, 5360]
      ),

      spacer(),
      rule(),

      // ── RESUME LINES ─────────────────────────────────────────────────────
      h1("Resume Description"),
      body("Built an end-to-end blockchain transaction data engineering pipeline processing 1M+ records using Python, Pandas, PySpark, Kafka, and Airflow with OLTP/OLAP dual-schema design.", { bold: true }),
      body("Implemented real-time Kafka streaming, Spark batch processing with partitioning/caching, Airflow DAG orchestration, and statistical anomaly detection — deployed on a star-schema DuckDB warehouse."),
    ],
  }],
});

Packer.toBuffer(doc).then(buffer => {
  fs.writeFileSync("docs/project_documentation.docx", buffer);
  console.log("✓ Documentation generated: docs/project_documentation.docx");
});

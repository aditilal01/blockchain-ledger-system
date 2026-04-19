-- ============================================================
-- schema.sql
-- Blockchain Transaction Ledger System
-- ============================================================
-- Contains:
--   PART A: OLTP Schema (transactional, normalized 3NF)
--   PART B: Star Schema (OLAP / data warehouse)
--   PART C: Sample DML (inserts)
-- ============================================================


-- ============================================================
-- PART A — OLTP SCHEMA (3rd Normal Form)
-- Used for: real-time transaction recording, low-latency writes
-- ============================================================

CREATE TABLE wallets (
    wallet_id       SERIAL        PRIMARY KEY,
    wallet_address  VARCHAR(66)   NOT NULL UNIQUE,
    kyc_verified    BOOLEAN       DEFAULT FALSE,
    country_code    CHAR(2),
    risk_score      DECIMAL(5,2)  DEFAULT 0.0,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE blocks (
    block_id        INT           PRIMARY KEY,
    block_hash      VARCHAR(66)   NOT NULL UNIQUE,
    miner_address   VARCHAR(66),
    reward          DECIMAL(18,8) DEFAULT 0.0,
    tx_count        INT           DEFAULT 0,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transactions (
    tx_id           SERIAL        PRIMARY KEY,
    tx_hash         VARCHAR(66)   NOT NULL UNIQUE,
    block_id        INT           REFERENCES blocks(block_id),
    sender_id       INT           REFERENCES wallets(wallet_id),
    receiver_id     INT           REFERENCES wallets(wallet_id),
    amount          DECIMAL(18,8) NOT NULL CHECK (amount > 0),
    fee             DECIMAL(18,8) DEFAULT 0.0,
    tx_type         VARCHAR(20)   DEFAULT 'TRANSFER',
    status          VARCHAR(15)   DEFAULT 'PENDING',
    network         VARCHAR(10)   DEFAULT 'mainnet',
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tx_sender   ON transactions(sender_id);
CREATE INDEX idx_tx_status   ON transactions(status);
CREATE INDEX idx_tx_created  ON transactions(created_at);


-- ============================================================
-- PART B — STAR SCHEMA (OLAP / Data Warehouse)
-- Used for: analytics queries, reporting, dashboards
-- Fact table surrounded by dimension tables
-- ============================================================

-- DIMENSION: Date
CREATE TABLE dim_date (
    date_key    INT         PRIMARY KEY,    -- YYYYMMDD format
    full_date   DATE        NOT NULL,
    year        SMALLINT,
    quarter     SMALLINT,
    month       SMALLINT,
    month_name  VARCHAR(10),
    week        SMALLINT,
    day_of_week SMALLINT,
    is_weekend  BOOLEAN
);

-- DIMENSION: Wallet (sender/receiver)
CREATE TABLE dim_wallet (
    wallet_key      INT         PRIMARY KEY,
    wallet_address  VARCHAR(66) NOT NULL,
    kyc_verified    BOOLEAN,
    country_code    CHAR(2),
    risk_score      DECIMAL(5,2)
);

-- DIMENSION: Transaction Type
CREATE TABLE dim_tx_type (
    tx_type_key INT         PRIMARY KEY,
    tx_type     VARCHAR(20) NOT NULL,
    category    VARCHAR(30),
    description TEXT
);

-- DIMENSION: Network
CREATE TABLE dim_network (
    network_key INT         PRIMARY KEY,
    network     VARCHAR(10) NOT NULL,
    chain_id    INT,
    description TEXT
);

-- FACT: Transactions (central fact table)
CREATE TABLE fact_transactions (
    fact_id         BIGSERIAL     PRIMARY KEY,
    tx_hash         VARCHAR(66)   NOT NULL,
    date_key        INT           REFERENCES dim_date(date_key),
    sender_key      INT           REFERENCES dim_wallet(wallet_key),
    receiver_key    INT           REFERENCES dim_wallet(wallet_key),
    tx_type_key     INT           REFERENCES dim_tx_type(tx_type_key),
    network_key     INT           REFERENCES dim_network(network_key),
    block_id        INT,
    amount          DECIMAL(18,8),
    fee             DECIMAL(18,8),
    amount_usd      DECIMAL(18,2),
    is_anomaly      SMALLINT      DEFAULT 0,
    status          VARCHAR(15)
);

CREATE INDEX idx_fact_date     ON fact_transactions(date_key);
CREATE INDEX idx_fact_sender   ON fact_transactions(sender_key);
CREATE INDEX idx_fact_network  ON fact_transactions(network_key);


-- ============================================================
-- PART C — BASIC SQL QUERIES
-- ============================================================

-- Q1: Total confirmed transactions per day
SELECT
    d.full_date,
    COUNT(*)           AS tx_count,
    SUM(f.amount)      AS total_volume,
    AVG(f.fee)         AS avg_fee
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.status = 'CONFIRMED'
GROUP BY d.full_date
ORDER BY d.full_date;


-- Q2: Top 10 wallets by total send volume
SELECT
    w.wallet_address,
    w.country_code,
    COUNT(f.fact_id)   AS tx_count,
    SUM(f.amount)      AS total_sent,
    SUM(f.fee)         AS total_fees
FROM fact_transactions f
JOIN dim_wallet w ON f.sender_key = w.wallet_key
WHERE f.status = 'CONFIRMED'
GROUP BY w.wallet_address, w.country_code
ORDER BY total_sent DESC
LIMIT 10;


-- ============================================================
-- PART D — ADVANCED SQL (Joins, Subqueries, Window Functions)
-- ============================================================

-- Q3: Window function — running total volume per network per day
SELECT
    d.full_date,
    n.network,
    SUM(f.amount)                              AS daily_volume,
    SUM(SUM(f.amount)) OVER (
        PARTITION BY n.network
        ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                          AS running_volume,
    RANK() OVER (
        PARTITION BY n.network
        ORDER BY SUM(f.amount) DESC
    )                                          AS volume_rank
FROM fact_transactions f
JOIN dim_date    d ON f.date_key    = d.date_key
JOIN dim_network n ON f.network_key = n.network_key
GROUP BY d.full_date, n.network
ORDER BY n.network, d.full_date;


-- Q4: Subquery — flag high-risk senders (risk_score > 7)
SELECT
    f.tx_hash,
    f.amount,
    f.status,
    w.risk_score
FROM fact_transactions f
JOIN dim_wallet w ON f.sender_key = w.wallet_key
WHERE w.wallet_key IN (
    SELECT wallet_key FROM dim_wallet WHERE risk_score > 7.0
)
AND f.status != 'FAILED'
ORDER BY w.risk_score DESC, f.amount DESC;


-- Q5: CTE + window — detect wallets with sudden spike in tx count
WITH daily_counts AS (
    SELECT
        f.sender_key,
        d.full_date,
        COUNT(*) AS daily_tx_count
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY f.sender_key, d.full_date
),
with_lag AS (
    SELECT
        *,
        LAG(daily_tx_count) OVER (
            PARTITION BY sender_key ORDER BY full_date
        ) AS prev_day_count
    FROM daily_counts
)
SELECT
    sender_key,
    full_date,
    daily_tx_count,
    prev_day_count,
    ROUND(
        (daily_tx_count - prev_day_count)::NUMERIC / NULLIF(prev_day_count, 0) * 100,
        2
    ) AS pct_change
FROM with_lag
WHERE prev_day_count IS NOT NULL
  AND daily_tx_count > prev_day_count * 3     -- 300% spike
ORDER BY pct_change DESC;

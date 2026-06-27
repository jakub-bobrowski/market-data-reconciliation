-- Market Data Reconciliation – Database Schema

-- Two raw data tables (CSV source and API source) feed into
-- one reconciliation results table.

-- UNIQUE (symbol, date) prevents duplicates in all three tables.
-- Re-running the pipeline overwrites existing data via UPSERT
-- (ON CONFLICT DO UPDATE) instead of creating duplicates.

-- Table 1: raw data from CSV (stooq.com)
CREATE TABLE raw_stooq (
    id          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    date        DATE           NOT NULL,
    open_price  NUMERIC(18,6),
    high_price  NUMERIC(18,6),
    low_price   NUMERIC(18,6),
    close_price NUMERIC(18,6),
    volume      BIGINT,
    loaded_at   TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, date)
);

-- Table 2: raw data from API (Alpha Vantage)
CREATE TABLE raw_alphavantage (
    id          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    date        DATE           NOT NULL,
    open_price  NUMERIC(18,6),
    high_price  NUMERIC(18,6),
    low_price   NUMERIC(18,6),
    close_price NUMERIC(18,6),
    volume      BIGINT,
    loaded_at   TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, date)
);

-- Table 3: reconciliation results
-- recon_status values:
--   PASS  – close prices and volumes match between both sources
--   FAIL  – difference in close price exceeds 0.01 threshold
--           (accounts for minor rounding differences between providers)
--   BREAK – record exists in one source only (missing date in either CSV or API)
CREATE TABLE reconciliation_results (
    id                  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol              VARCHAR(20)    NOT NULL,
    date                DATE           NOT NULL,
    close_stooq         NUMERIC(18,6),
    close_alphavantage  NUMERIC(18,6),
    close_diff          NUMERIC(18,6),
    volume_stooq        BIGINT,
    volume_alphavantage BIGINT,
    volume_diff         BIGINT,
    recon_status        VARCHAR(10),
    checked_at          TIMESTAMP      DEFAULT NOW(),
    UNIQUE (symbol, date)
);
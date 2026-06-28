"""Data ingestion: CSV loading, API fetching, data normalization, and Supabase upsert.

Pipeline flow for a single instrument (e.g. SPY):
1. load_sample_csv(filepath) or load_uploaded_csv() → raw DataFrame from CSV file
2. normalize_data()                                 → cleaned DataFrame (snake_case columns,
                                                    correct types, symbol column added)
3. get_date_range()                                 → (start_date, end_date) extracted from
                                                    normalized DataFrame, passed to API fetch
4. fetch_api_data()                                 → raw DataFrame from Alpha Vantage API
                                                    filtered to exact CSV date range
5. normalize_data()                                 → same normalization applied to API data
6. upsert_to_supabase()                             → INSERT ... ON CONFLICT (symbol, date)
                                                    DO UPDATE: inserts new rows, overwrites
                                                    existing ones - no duplicates on re-run
"""

from datetime import date
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

# Always finds the data/ folder regardless of where this script is run from.
DATA_DIR = Path(__file__).parent.parent / "data"

# Maps stooq column names to target snake_case schema.
COLUMN_RENAME = {
    "Date": "date",
    "Open": "open_price",
    "High": "high_price",
    "Low": "low_price",
    "Close": "close_price",
    "Volume": "volume",
}

def load_sample_csv(filepath: Path) -> pd.DataFrame:
    """Load a CSV file by explicit path. Returns a raw DataFrame (not yet normalized)."""
    return pd.read_csv(filepath)


def symbol_from_path(filepath: Path) -> str:
    """Extract ticker symbol from a filename stem: 'spy_us_d.csv' → 'SPY'."""
    return filepath.stem.split("_")[0].upper()


def scan_csv_catalog() -> list[dict]:
    """Scan data/ for all CSV files and return one catalog entry per file.

    Each entry: {path, symbol, df (normalized), start, end, label}.
    Adding a new CSV to data/ automatically makes it appear in the UI.
    """
    catalog = []
    for path in sorted(DATA_DIR.glob("*.csv")):
        symbol = symbol_from_path(path)
        df = normalize_data(load_sample_csv(path), symbol, "stooq")
        start, end = get_date_range(df)
        catalog.append({
            "path": path,
            "symbol": symbol,
            "df": df,
            "start": start,
            "end": end,
            "label": f"{symbol} · {start} to {end} ({path.name})",
        })
    return catalog


def normalize_data(df: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    """Normalize a raw OHLCV DataFrame (Date, Open, High, Low, Close, Volume)
    to the target database schema (date, open_price, high_price, low_price,
    close_price, volume, symbol).

    Works for both CSV (source='stooq') and API (source='alphavantage') data
    since both produce the same target schema after renaming.

    Args:
        df:     Raw DataFrame from load_sample_csv(), load_uploaded_csv(), or fetch_api_data().
        symbol: Ticker symbol, e.g. 'SPY'. Stored upper-case in every row.
        source: Data source identifier ('stooq' or 'alphavantage'). Reserved for
                future API-specific column mapping 
    Returns: 
        Normalized DataFrame ready for upsert_to_supabase().
    """
    df = df.copy() 

    # Step 1 – normalize column names: strip whitespace, then title-case (data cleansing):
    # "date" → "Date", "DATE" → "Date", "Date " → "Date"
    df.columns = df.columns.str.strip().str.title()

    # Step 2 – rename to target snake_case schema.
    df = df.rename(columns=COLUMN_RENAME)

    # Select only the 6 required columns in the exact order used by INSERT INTO.
    # This also drops any extra columns the source may include (e.g. "Change").
    df = df[["date", "open_price", "high_price", "low_price", "close_price", "volume"]]

    # Step 3 – cast types to match the database column types.
    # .dt.date strips the time component (DB column is DATE, not TIMESTAMP)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    for col in ("open_price", "high_price", "low_price", "close_price"):
        df[col] = df[col].astype(float) # NUMERIC(18,6) in DB
    
    # Int64 (capital I) is pandas nullable integer – supports NaN unlike plain int64.
    # errors="coerce" turns unparseable values into NaN instead of raising an error.
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    # Step 4 – stamp every row with the symbol (always upper-case for consistency).
    df["symbol"] = symbol.upper()

    return df

def get_date_range(df: pd.DataFrame) -> tuple[date, date]:
    """Return (start_date, end_date) from a normalized DataFrame.

    Used by the Streamlit UI to display the loaded date range and to pass
    the exact date range to fetch_api_data() so the API fetch
    always covers the same dates as the loaded CSV - no artificial BREAKs.
    """
    return df["date"].min(), df["date"].max()


def upsert_to_supabase(
    df: pd.DataFrame, table_name: str, connection_string: str
) -> int:
    """Bulk-upsert a normalized DataFrame into Supabase via psycopg2 execute_values.

    Uses a single INSERT ... ON CONFLICT (symbol, date) DO UPDATE statement
    for all rows at once (bulk insert) instead of one query per row (slow loop).
    Re-running the pipeline for the same symbol and date range overwrites
    existing rows instead of creating duplicates.

    Args:
        df:                Normalized DataFrame (output of normalize_data()).
        table_name:        Target table: 'raw_stooq' or 'raw_alphavantage'.
        connection_string: PostgreSQL URI from .streamlit/secrets.toml.

    Returns:
        Number of rows processed.
    """
    # Select and order columns to exactly match the INSERT INTO column list.
    # Without this explicit selection, column order would depend on how the
    # DataFrame was built – values could end up in the wrong columns.
    cols = ["symbol", "date", "open_price", "high_price", "low_price", "close_price", "volume"]

    # df[cols]       → DataFrame with columns in the correct order
    # .values        → NumPy array of raw values (no column names)
    # .tolist()      → list of lists: [["SPY", date(2026,1,2), 464.78, ...], ...]
    # Each inner list = one row inserted into the database.
    rows = df[cols].values.tolist()

    engine = create_engine(connection_string)

    # raw_connection() gives a bare psycopg2 connection, bypassing SQLAlchemy.
    # Needed because execute_values is a psycopg2 function, not SQLAlchemy.
    with engine.raw_connection() as conn:
        with conn.cursor() as cur:
            # execute_values replaces the single %s placeholder with all rows at once,
            # building one large INSERT instead of 100 separate INSERT statements.
            execute_values(
                cur,
                f"""
                INSERT INTO {table_name} (symbol, date, open_price, high_price,
                                          low_price, close_price, volume)
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open_price  = EXCLUDED.open_price,
                    high_price  = EXCLUDED.high_price,
                    low_price   = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume      = EXCLUDED.volume,
                    loaded_at   = NOW()
                -- EXCLUDED refers to the values that were just attempted to be inserted.
                -- loaded_at = NOW() updates the timestamp on every re-run.
                """,
                rows,
            )
        conn.commit() 

    return len(rows)
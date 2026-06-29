"""Market Data Reconciliation – Streamlit application entry point."""

import pandas as pd
import plotly.express as px
import streamlit as st

from src.ingest import (
    fetch_api_data,
    get_date_bounds,
    get_date_range,
    get_distinct_symbols,
    load_uploaded_csv,
    normalize_data,
    query_ohlcv,
    scan_csv_catalog,
    symbol_from_filename,
    upsert_to_supabase,
)

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Market Data Reconciliation",
    page_icon="📊",
    layout="wide",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📊 Market Data Reconciliation")
    st.divider()
    st.write(
        "A two-source OHLCV reconciliation pipeline. "
        "Verifies consistency of daily market data between a CSV file "
        "and the Alpha Vantage API for the same instrument and date range."
    )

# Read credentials from .streamlit/secrets.toml either locally or in Streamlit Cloud (Settings / Secrets). The file is not checked into GitHub.
conn_str = st.secrets["supabase"]["connection_string"]
api_key = st.secrets["alphavantage"]["api_key"]


# ── Cached loader: avoids re-reading CSVs on every Streamlit rerun ────────────
@st.cache_data
def load_csv_catalog() -> list[dict]:
    """Scan data/ and return one catalog entry per CSV file."""
    return scan_csv_catalog()


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📥 Load & Reconcile", "🔍 Data Explorer", "📖 About & SQL"])


# ── TAB 1: Load & Reconcile ───────────────────────────────────────────────────
with tab1:

    st.subheader("CSV Source")

    csv_mode = st.radio(
        "CSV Source",
        options=["Use sample data", "Upload your own CSV"],
        label_visibility="collapsed",
    )

    # Resolved after user makes a selection; used by the Run Pipeline button below
    symbol = None
    df_csv = None

    # ── Branch A: sample data ─────────────────────────────────────────────────
    if csv_mode == "Use sample data":

        # Scan data/ once per session; adding a new CSV file auto-populates the radio.
        catalog = load_csv_catalog()
        sample_options = {entry["label"]: entry for entry in catalog}

        sample_label = st.radio(
            "Select sample dataset",
            options=list(sample_options.keys()),
            label_visibility="collapsed",
        )
        entry = sample_options[sample_label]
        symbol, df_csv = entry["symbol"], entry["df"]
        start_date, end_date = entry["start"], entry["end"]

        # Automatic preview – no button click required
        st.success(f"✅ Ready · {symbol} · {len(df_csv)} rows · {start_date} to {end_date}")

        # Persist in session_state so that Run Pipeline can read them
        # even after Streamlit reruns triggered by the button click
        st.session_state["symbol"]     = symbol
        st.session_state["start_date"] = start_date
        st.session_state["end_date"]   = end_date
        st.session_state["df_csv"]     = df_csv

    # ── Branch B: upload your own CSV ───────────────────────────────
    else:
        uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"], label_visibility="collapsed")
        st.caption(
            "Expected format: Date (YYYY-MM-DD) + OHLCV columns. "
            "Column names are normalized automatically (case-insensitive, whitespace stripped). "
            "Compatible sources: stooq.com / any provider with daily OHLCV data."
        )
        if uploaded_file is not None:
            guessed = symbol_from_filename(uploaded_file.name)
            symbol_input = st.text_input(
                "Ticker symbol",
                value=guessed,
                max_chars=10,
                help="Guessed from the filename. Override if incorrect (e.g. AAPL, SPY).",
            ).strip().upper()

            if symbol_input:
                try:
                    df_raw = load_uploaded_csv(uploaded_file)
                    df_csv = normalize_data(df_raw, symbol_input, "stooq")
                    start_date, end_date = get_date_range(df_csv)
                    symbol = symbol_input

                    st.success(
                        f"✅ Ready · {symbol} · {len(df_csv)} rows · {start_date} to {end_date}"
                    )

                    st.session_state["symbol"]     = symbol
                    st.session_state["start_date"] = start_date
                    st.session_state["end_date"]   = end_date
                    st.session_state["df_csv"]     = df_csv

                except KeyError as exc:
                    st.error(
                        f"❌ Missing column {exc}. "
                        "Expected: Date, Open, High, Low, Close, Volume (case-insensitive)."
                    )
                except Exception as exc:
                    st.error(f"❌ Could not parse CSV: {exc}")
            else:
                st.info("Enter the ticker symbol above to continue.")

    st.divider()

    # ── Run Pipeline button ───────────────────────────────────────────────────
    # Disabled when no CSV source is ready (e.g. "Upload your own CSV" selected
    # but no file provided yet)
    pipeline_ready = symbol is not None

    st.button(
        "🚀 Run Pipeline: CSV → API → Reconcile",
        key="run_pipeline",
        type="secondary",
        disabled=not pipeline_ready,
    )
    st.caption("Loads CSV · Fetches API data for matching date range · Reconciles both sources")

    # Run the pipeline only when the button was just clicked
    if st.session_state.get("run_pipeline"):
        _df_csv = st.session_state.get("df_csv")

        with st.status("Running pipeline...", expanded=True) as pipeline_status:
            # Step 1 – upsert CSV data into raw_stooq
            rows_csv = upsert_to_supabase(_df_csv, "raw_stooq", conn_str)
            st.write(f"✅ Step 1 · CSV loaded to database · {rows_csv} rows")

            # Step 2 – fetch Alpha Vantage data and upsert into raw_alphavantage
            # (compact = last ~100 trading days; sample CSV must cover a recent date range)
            try:
                df_api = fetch_api_data(
                    st.session_state["symbol"],
                    st.session_state["start_date"],
                    st.session_state["end_date"],
                    api_key,
                )
            except RuntimeError as exc:
                st.error(f"❌ API fetch failed: {exc}")
                pipeline_status.update(label="Pipeline failed", state="error")
                st.stop()

            rows_api = upsert_to_supabase(df_api, "raw_alphavantage", conn_str)
            st.write(f"✅ Step 2 · API data loaded to database · {rows_api} rows")

            # Row count mismatch between CSV and API is expected on holidays/weekends
            # that differ between sources, but flag it so the user is aware.
            if rows_api != rows_csv:
                st.warning(
                    f"⚠️ Row count mismatch · CSV: {rows_csv} rows · API: {rows_api} rows "
                    f"(CSV date range depends on the loaded file; API always returns the last ~100 trading days)"
                )

            # Step 3 – Reconciliation will be added in the next slice
            pipeline_status.update(label="Pipeline complete", state="complete", expanded=True)


# ── TAB 2: Data Explorer ────────────────────────────────────────────
# Three dependent filters: DB source → symbol (queried from source) → date range (min/max from source for a given symbol)
with tab2:
    st.subheader("Raw Data Explorer")

    col_src, col_sym, col_dates = st.columns([1.2, 1, 2])

    # Step 1 – source: static dropdown; user selects which raw table to query from DB
    with col_src:
        source_table = st.selectbox(
            "Source",
            options=["raw_stooq", "raw_alphavantage"],
            format_func=lambda x: "CSV · stooq" if x == "raw_stooq" else "API · Alpha Vantage",
        )

    # Step 2 – symbol: dynamic dropdown from DB; depends on source_table (Step 1)
    available_symbols = get_distinct_symbols(source_table, conn_str)

    if not available_symbols:
        st.info("Run the pipeline first")
    else:
        with col_sym:
            symbol_filter = st.selectbox("Symbol", options=available_symbols)

        # Step 3 – date range: bounds from DB for selected symbol; depends on Step 2
        min_date, max_date = get_date_bounds(source_table, symbol_filter, conn_str)

        with col_dates:
            # Default to full available range; min/max prevent selecting dates outside the DB
            date_range = st.date_input(
                "Date range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )

        start_filter = date_range[0] if len(date_range) >= 1 else None
        end_filter = date_range[1] if len(date_range) >= 2 else None

        try:
            df_view = query_ohlcv(
                source_table,
                conn_str,
                symbol=symbol_filter,
                start_date=start_filter,
                end_date=end_filter,
            )
        except Exception as exc:
            st.error(f"❌ Query failed: {exc}")
            df_view = pd.DataFrame()

        # Empty result is unlikely (date range comes from DB bounds) but handled defensively
        if df_view.empty:
            st.info("No rows found — adjust the date range.")
        else:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Rows", f"{len(df_view):,}")
            m2.metric("Symbol", symbol_filter)
            m3.metric("From", str(df_view["date"].min()))
            m4.metric("To", str(df_view["date"].max()))

            fig = px.line(df_view, x="date", y="close_price", title=f"{symbol_filter} – Close Price")
            st.plotly_chart(fig, use_container_width=True)

            display_cols = ["date", "symbol", "open_price", "high_price", "low_price",
                            "close_price", "volume", "loaded_at"]
            st.dataframe(
                df_view[display_cols],
                use_container_width=True,
                hide_index=True, 
                column_config={
                    "date": st.column_config.DateColumn("Date"),
                    "symbol": st.column_config.TextColumn("Symbol"),
                    "open_price": st.column_config.NumberColumn("Open", format="%.4f"),
                    "high_price": st.column_config.NumberColumn("High", format="%.4f"),
                    "low_price": st.column_config.NumberColumn("Low", format="%.4f"),
                    "close_price": st.column_config.NumberColumn("Close", format="%.4f"),
                    "volume": st.column_config.NumberColumn("Volume", format="%d"),
                    "loaded_at": st.column_config.DatetimeColumn("Loaded At"),
                },
            )


# ── TAB 3: About & SQL (placeholder) ───────────────────────────────
with tab3:
    st.markdown("""
# About This Project

A two-source OHLCV (Open, High, Low, Close prices + Volume) reconciliation pipeline that
verifies whether daily market data from a CSV file (stooq.com) and the Alpha Vantage API are
consistent for the same instrument and date range. Built with Python, SQL (PostgreSQL), and
Streamlit.

# Tech Stack

Python · Pandas · SQLAlchemy · Streamlit · PostgreSQL (Supabase) · Alpha Vantage API

# Data Sources

- **CSV**: stooq.com – free historical daily OHLCV data, no registration required
- **API**: Alpha Vantage – TIME_SERIES_DAILY endpoint (free tier)

API date range is automatically matched to the loaded CSV date range.
""")

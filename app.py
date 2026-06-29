"""Market Data Reconciliation – Streamlit application entry point."""

import streamlit as st

from src.ingest import fetch_api_data, upsert_to_supabase, scan_csv_catalog

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
            st.info("CSV upload processing will be available in the next update.")

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


# ── TAB 2: Data Explorer (placeholder) ─────────────────────────────
with tab2:
    st.info("Run the pipeline first to populate the database, then explore the raw data here.")


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

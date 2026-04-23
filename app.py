import streamlit as st
import polars as pl
import pandas as pd
from datetime import datetime, date

st.set_page_config(page_title="T24 Protocol Audit Tool", layout="centered")

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0f1117; }
    .block-container { padding-top: 2rem; }
    h1 { color: #00d4aa; font-family: 'Courier New', monospace; }
    .stTextInput > div > div > input {
        background-color: #1e2130;
        color: #e0e0e0;
        border: 1px solid #00d4aa44;
        border-radius: 6px;
    }
    .search-result-count {
        background: #00d4aa22;
        border-left: 3px solid #00d4aa;
        padding: 0.5rem 1rem;
        border-radius: 4px;
        margin: 0.5rem 0;
        color: #00d4aa;
        font-family: monospace;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #00d4aa33;
        border-radius: 8px;
        background: #1a1d2e;
    }
</style>
""", unsafe_allow_html=True)

st.title("📊 T24 Protocol Audit Tool")
st.write("Upload protocol Excel/CSV files, then search and filter the records.")

# ── Column definitions ────────────────────────────────────────────────────────
COLS = [
    "PROTOCOL_ID", "PROCESS_DATE", "DATE_VERSION", "TIME", "TIME_MSECS",
    "TERMINAL_ID", "PHANTOM_ID", "COMPANY_ID", "USER", "APPLICATION",
    "LEVEL_FUNCTION", "ID", "REMARK", "CLIENT_IP_ADDRESS", "LOCAL_DATE_TIME"
]

# ── File upload ───────────────────────────────────────────────────────────────
uploaded_files = st.file_uploader(
    "Upload Protocol Files (.xlsx, .csv)",
    type=["xlsx", "csv"],
    accept_multiple_files=True
)

# ── Processing function ───────────────────────────────────────────────────────
def process_protocol_file(file):
    file.seek(0)
    if file.name.lower().endswith(".csv"):
        df_raw = pl.read_csv(file, has_header=False, infer_schema_length=0, ignore_errors=True)
    else:
        df_raw = pl.read_excel(file, has_header=False)

    df_raw = df_raw.slice(1)
    first_col = df_raw.columns[0]

    df = df_raw.select(
        pl.col(first_col).cast(pl.Utf8).str.split("*").alias("split")
    ).with_columns([
        pl.col("split").list.get(i, null_on_oob=True).alias(COLS[i])
        for i in range(len(COLS))
    ]).drop("split")

    df = df.with_columns([
        pl.when(pl.col(c).str.strip_chars() == "")
          .then(None)
          .otherwise(pl.col(c).str.strip_chars())
          .alias(c)
        for c in COLS
    ])

    df = df.with_columns(
        pl.col("PROCESS_DATE")
          .str.strptime(pl.Date, "%Y%m%d", strict=False)
          .alias("PROCESS_DATE_DT")
    )

    df = df.with_columns(
        pl.col("PROTOCOL_ID")
          .str.slice(0, 8)
          .str.strptime(pl.Date, "%Y%m%d", strict=False)
          .alias("TRANS_DATE_DT")
    )

    df = df.with_columns(
        pl.col("TIME_MSECS")
          .cast(pl.Utf8)
          .str.slice(0, 2)
          .cast(pl.Int32, strict=False)
          .alias("HOUR")
    )

    return df

# ── Load and cache data ───────────────────────────────────────────────────────
@st.cache_data
def load_files(files_data):
    """Cache parsed dataframes. files_data is list of (name, bytes) tuples."""
    frames = []
    for name, data in files_data:
        import io
        file_like = io.BytesIO(data)
        file_like.name = name
        try:
            df = process_protocol_file(file_like)
            df = df.with_columns(pl.lit(name).alias("__source_file__"))
            frames.append(df)
        except Exception as e:
            st.warning(f"⚠️ Could not parse `{name}`: {e}")
    if frames:
        return pl.concat(frames, how="diagonal")
    return None

# ── Main logic ────────────────────────────────────────────────────────────────
if uploaded_files:
    files_data = [(f.name, f.read()) for f in uploaded_files]
    df = load_files(files_data)

    if df is not None:
        total_records = len(df)
        st.success(f"✅ Loaded **{total_records:,}** records from **{len(uploaded_files)}** file(s).")

        # ── SEARCH & FILTER UI ────────────────────────────────────────────────
        st.markdown("---")
        st.subheader("🔍 Search & Filter")

        with st.expander("**Search Options**", expanded=True):
            col1, col2 = st.columns(2)

            with col1:
                # Global keyword search
                keyword = st.text_input(
                    "🔎 Keyword search (searches all text columns)",
                    placeholder="e.g. ENQUIRY, admin, 192.168..."
                )

                # User filter
                users = sorted([u for u in df["USER"].drop_nulls().unique().to_list()])
                selected_users = st.multiselect("👤 Filter by USER", options=users)

                # Application filter
                apps = sorted([a for a in df["APPLICATION"].drop_nulls().unique().to_list()])
                selected_apps = st.multiselect("📱 Filter by APPLICATION", options=apps)

            with col2:
                # Terminal ID filter
                terminals = sorted([t for t in df["TERMINAL_ID"].drop_nulls().unique().to_list()])
                selected_terminals = st.multiselect("🖥️ Filter by TERMINAL_ID", options=terminals)

                # Company filter
                companies = sorted([c for c in df["COMPANY_ID"].drop_nulls().unique().to_list()])
                selected_companies = st.multiselect("🏢 Filter by COMPANY_ID", options=companies)

                # Date range filter
                min_date = df["PROCESS_DATE_DT"].drop_nulls().min()
                max_date = df["PROCESS_DATE_DT"].drop_nulls().max()

                if min_date and max_date:
                    date_range = st.date_input(
                        "📅 Filter by PROCESS_DATE range",
                        value=(min_date, max_date),
                        min_value=min_date,
                        max_value=max_date
                    )
                else:
                    date_range = None

            # Hour range filter (full width)
            min_hour, max_hour = st.slider(
                "⏰ Filter by Hour of Day (from TIME_MSECS)",
                min_value=0, max_value=23,
                value=(0, 23)
            )

            # Level/Function filter
            level_search = st.text_input(
                "⚙️ Filter by LEVEL_FUNCTION (contains)",
                placeholder="e.g. ENQUIRY, INPUT, AUTH"
            )

            # Remark search
            remark_search = st.text_input(
                "📝 Filter by REMARK (contains)",
                placeholder="e.g. success, error, timeout"
            )

        # ── Apply filters ─────────────────────────────────────────────────────
        filtered = df

        # Keyword search across all string columns
        if keyword.strip():
            kw = keyword.strip().lower()
            text_cols = [c for c in COLS if c not in ("PROCESS_DATE", "TIME_MSECS")]
            mask = pl.lit(False)
            for col in text_cols:
                mask = mask | pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(kw)
            filtered = filtered.filter(mask)

        if selected_users:
            filtered = filtered.filter(pl.col("USER").is_in(selected_users))

        if selected_apps:
            filtered = filtered.filter(pl.col("APPLICATION").is_in(selected_apps))

        if selected_terminals:
            filtered = filtered.filter(pl.col("TERMINAL_ID").is_in(selected_terminals))

        if selected_companies:
            filtered = filtered.filter(pl.col("COMPANY_ID").is_in(selected_companies))

        if date_range and len(date_range) == 2:
            start_date, end_date = date_range
            filtered = filtered.filter(
                (pl.col("PROCESS_DATE_DT") >= start_date) &
                (pl.col("PROCESS_DATE_DT") <= end_date)
            )

        # Hour filter
        filtered = filtered.filter(
            (pl.col("HOUR") >= min_hour) & (pl.col("HOUR") <= max_hour)
        )

        if level_search.strip():
            filtered = filtered.filter(
                pl.col("LEVEL_FUNCTION").cast(pl.Utf8).str.to_lowercase()
                  .str.contains(level_search.strip().lower())
            )

        if remark_search.strip():
            filtered = filtered.filter(
                pl.col("REMARK").cast(pl.Utf8).str.to_lowercase()
                  .str.contains(remark_search.strip().lower())
            )

        # ── Results ───────────────────────────────────────────────────────────
        st.markdown("---")
        result_count = len(filtered)
        pct = (result_count / total_records * 100) if total_records else 0

        st.markdown(
            f'<div class="search-result-count">🔍 Found <strong>{result_count:,}</strong> records '
            f'({pct:.1f}% of total {total_records:,})</div>',
            unsafe_allow_html=True
        )

        # Column selector
        display_cols = st.multiselect(
            "📋 Choose columns to display",
            options=COLS + ["PROCESS_DATE_DT", "TRANS_DATE_DT", "HOUR", "__source_file__"],
            default=["PROTOCOL_ID", "PROCESS_DATE_DT", "USER", "APPLICATION",
                     "LEVEL_FUNCTION", "REMARK", "CLIENT_IP_ADDRESS"]
        )

        if display_cols:
            available = [c for c in display_cols if c in filtered.columns]
            st.dataframe(
                filtered.select(available).to_pandas(),
                use_container_width=True,
                height=500
            )

            # ── Summary stats ─────────────────────────────────────────────────
            st.markdown("---")
            st.subheader("📈 Summary of Filtered Results")

            s1, s2, s3, s4 = st.columns(4)
            with s1:
                st.metric("Total Records", f"{result_count:,}")
            with s2:
                unique_users = filtered["USER"].drop_nulls().n_unique()
                st.metric("Unique Users", unique_users)
            with s3:
                unique_apps = filtered["APPLICATION"].drop_nulls().n_unique()
                st.metric("Unique Applications", unique_apps)
            with s4:
                unique_terminals = filtered["TERMINAL_ID"].drop_nulls().n_unique()
                st.metric("Unique Terminals", unique_terminals)

            # Top users
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown("**Top 10 Users by Activity**")
                top_users = (
                    filtered.group_by("USER").agg(pl.len().alias("Count"))
                    .sort("Count", descending=True).head(10)
                )
                st.dataframe(top_users.to_pandas(), use_container_width=True, hide_index=True)

            with col_b:
                st.markdown("**Top 10 Applications**")
                top_apps = (
                    filtered.group_by("APPLICATION").agg(pl.len().alias("Count"))
                    .sort("Count", descending=True).head(10)
                )
                st.dataframe(top_apps.to_pandas(), use_container_width=True, hide_index=True)

            # ── Export ────────────────────────────────────────────────────────
            st.markdown("---")
            csv_data = filtered.select(available).to_pandas().to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download Filtered Results as CSV",
                data=csv_data,
                file_name=f"protocol_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("Select at least one column to display results.")

else:
    st.info("👆 Upload one or more protocol files to get started.") 
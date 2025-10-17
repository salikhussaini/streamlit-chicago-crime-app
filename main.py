# dashboard.py
import streamlit as st
import altair as alt
import pandas as pd
import os
import geopandas as gpd
import pydeck as pdk
import numpy as np
import matplotlib.cm as cm
import matplotlib.colors as mcolors


# ----------------------------
# Load Data
# ----------------------------
@st.cache_data
def load_data(file_path: str) -> pd.DataFrame:
    """Load the Chicago crime summary data."""
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, parse_dates=["report_date", "start_date", "end_date"])
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
            print(df.columns)
        else:
            st.error("Unsupported file format. Use CSV or Parquet.")
            return pd.DataFrame()
    except FileNotFoundError:
        st.error(f"❌ Data file not found at: {file_path}")
        return pd.DataFrame()
    return df

# ----------------------------
# File Path
# ----------------------------
file_path = "data/agg/Crime_20250927_20251013.parquet"
df = load_data(file_path)

if df.empty:
    st.stop()

# ----------------------------
# Constants for Metrics
# ----------------------------
CASE_METRICS = [c for c in df.columns if c.startswith("total_") and not c.startswith("prior_")]
UNIQUE_METRICS = ['unique_beats', 'unique_wards', 'unique_districts']
PRIOR_METRICS = [f"prior_{m[6:]}" for m in CASE_METRICS if f"prior_{m[6:]}" in df.columns]
COMPARISON_PAIRS = list(zip(CASE_METRICS, PRIOR_METRICS))

# ----------------------------
# Dashboard Title
# ----------------------------
st.title("📊 Chicago Police Report Dashboard")

# ----------------------------
# Sidebar Filters
# ----------------------------
st.sidebar.header("Filters")

end_dates = df["end_date"].sort_values(ascending=False).unique()
selected_end_date = st.sidebar.selectbox("Select Report End Date", end_dates, index=0)

report_types = df["report_type"].unique().tolist()
selected_report_type = st.sidebar.selectbox(
    "Select Report Type",
    report_types,
    index=0
)

# Apply filters
filtered_df = df[
    (df["report_type"] == selected_report_type) &
    (df["end_date"] == selected_end_date)
]

trend_df = df[
    (df["report_type"] == selected_report_type) &
    (df["end_date"] <= selected_end_date)
]

# ----------------------------
# Reporting Period
# ----------------------------
if not filtered_df.empty:
    snapshot = filtered_df.iloc[0]

    st.subheader("🗓️ Reporting Period")
    st.write(f"**Start Date:** {snapshot['start_date']:%Y-%m-%d}")
    st.write(f"**End Date:** {snapshot['end_date']:%Y-%m-%d}")
    st.write(f"**Report Date Generated:** {snapshot['report_date']}")

# ----------------------------
# Organize Dashboard into Tabs
# ----------------------------
st.header("📌 Dashboard Views")

tab_overview, tab_crimes, tab_geo, tab_trends, tab_comparison = st.tabs(
    ["📊 Overview", "🚨 Crime Composition", "🏙️ Geographic Breakdown", "📈 Trends", "📉 Comparison"]
)

# --- Overview Tab ---
with tab_overview:
    if not filtered_df.empty:
        st.subheader("Summary Metrics")

        # Case counts
        st.subheader("📊 Case Counts")
        cols = st.columns(len(CASE_METRICS))
        for i in range(0, len(CASE_METRICS), 3):
            cols = st.columns(3)
            for c, m in zip(cols, CASE_METRICS[i:i+3]):
                c.metric(label=m.replace("_", " ").title(), value=f"{snapshot[m]:,.0f}")

        # Unique categories
        st.subheader("🔑 Unique Categories")
        cols = st.columns(len(UNIQUE_METRICS))
        for c, m in zip(cols, UNIQUE_METRICS):
            c.metric(label=m.replace("_", " ").title(), value=f"{snapshot[m]:,.0f}")


# --- Crime Composition Tab ---
with tab_crimes:
    st.subheader("🚨 Crime Composition")
    if not filtered_df.empty:
        primary_types = snapshot["primary_types"]
        case_counts = snapshot["primary_type_case_counts"]

        # Convert to lists safely
        if isinstance(primary_types, str):
            crime_types = [t.strip() for t in primary_types.split(",") if t.strip()]
        else:
            crime_types = list(primary_types)

        if isinstance(case_counts, str):
            counts = [int(x) for x in case_counts.split(",") if x.strip().isdigit()]
        else:
            counts = list(case_counts)

        # Match lengths
        min_len = min(len(crime_types), len(counts))
        crime_types, counts = crime_types[:min_len], counts[:min_len]

        if min_len > 0:
            crime_df = pd.DataFrame({"Crime Type": crime_types, "Count": counts})
            crime_df = crime_df.sort_values("Count", ascending=False)

            chart = alt.Chart(crime_df).mark_bar().encode(
                x=alt.X("Count:Q", sort="-y"),
                y=alt.Y("Crime Type:N", sort="-x"),
                tooltip=["Crime Type", "Count"]
            ).properties(width=600, height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No crime composition data available for this report.")


# --- Geographic Breakdown Tab ---
with tab_geo:
    st.subheader("🏙️ Geographic Map")

    if not filtered_df.empty:
        # Dropdown to select geography type
        geo_type = st.selectbox(
            "Select Geography Level",
            ["Ward", "District", "Community Area", "Beat"],
            index=0
        )

        # Load the corresponding GeoJSON file
        geojson_path = {
            "Ward": "data/geojson/chicago_wards.geojson",
            "District": "data/geojson/chicago_districts.geojson",
            "Community Area": "data/geojson/chicago_community_areas.geojson",
            "Beat": "data/geojson/chicago_beats.geojson"
        }.get(geo_type, None)

        try:
            geo_gdf = gpd.read_file(geojson_path)
        except Exception as e:
            st.error(f"Could not load {geo_type.lower()} polygons: {e}")
            geo_gdf = None

        if geo_gdf is not None:
            # Helper to safely convert lists
            def to_list(x):
                if isinstance(x, str):
                    return [i.strip() for i in x.split(",") if i.strip()]
                return list(x)
            def extract_geo_data(snapshot, geo_type):
                mapping = {
                    "Ward": ("Ward", "Ward_case_counts", "ward"),
                    "District": ("District", "District_case_count", "dist_num"),
                    "Community Area": ("community_area", "community_area_case_counts", "area_numbe"),
                    "Beat": ("Beat", "Beat_case_counts", "beat_num")
                }
                id_key, count_key, geo_field = mapping[geo_type]
                ids = snapshot.get(id_key, [])
                counts = snapshot.get(count_key, [])
                def to_list(x): return [i.strip() for i in x.split(",") if i.strip()] if isinstance(x, str) else list(x)
                ids = to_list(ids)
                counts = [int(v) for v in to_list(counts) if str(v).isdigit()]
                return ids, counts, geo_field
            
            # Match lengths
            ids, counts, geo_field = extract_geo_data(snapshot, geo_type)
            min_len = min(len(ids), len(counts))

            count_df = pd.DataFrame({
                geo_field: [str(i) for i in ids],
                "cases": counts
            }).dropna()

            # Clean IDs safely (handles "27.0", "27", 27.0, etc.)
            count_df[geo_field] = (
                pd.to_numeric(count_df[geo_field], errors="coerce")
                .astype("Int64")
                .astype(str)
            )

            # Merge with GeoJSON
            geo_gdf[geo_field] = pd.to_numeric(geo_gdf[geo_field], errors="coerce").astype("Int64").astype(str)
            merged = geo_gdf.merge(count_df, on=geo_field, how="left")
            merged["cases"] = merged["cases"].fillna(0).astype(float)

            # --- Linear Color Normalization ---
            vmin = merged["cases"].min()
            vmax = max(merged["cases"].max(), 1)
            norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
            cmap = cm.get_cmap("Reds")

            # Map each case value to RGBA color (0–255)
            rgba_colors = (cmap(norm(merged["cases"])) * 255).astype(int)
            merged["color_r"] = rgba_colors[:, 0]
            merged["color_g"] = rgba_colors[:, 1]
            merged["color_b"] = rgba_colors[:, 2]
            merged["color_a"] = 180  # keep constant opacity

            merged_json = merged.__geo_interface__

            # --- Define Pydeck Layer ---
            layer = pdk.Layer(
                "GeoJsonLayer",
                merged_json,
                stroked=True,
                filled=True,
                get_fill_color='[properties.color_r, properties.color_g, properties.color_b, properties.color_a]',
                get_line_color=[60, 60, 60],
                pickable=True,
                auto_highlight=True,
            )

            view_state = pdk.ViewState(
                latitude=41.8781,
                longitude=-87.6298,
                zoom=9,
                pitch=0
            )

            r = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip={"text": f"{geo_type}: {{{geo_field}}}\nCases: {{cases}}"}
            )

            st.pydeck_chart(r)

            # --- Add Legend / Caption ---
            st.caption(f"🟥 Darker red = higher case count per {geo_type.lower()}")
            st.markdown(
                f"<small>Linear scale range: {vmin:,.0f} – {vmax:,.0f}</small>",
                unsafe_allow_html=True
            )
        else:
            st.info(f"{geo_type} shapefile not loaded — cannot show map.")
    else:
        st.info("No data for selected filters.")

# --- Trends Tab ---
with tab_trends:
    st.subheader("📈 Trends Over Time")
    if not trend_df.empty:
        months_back = st.slider("Select Trend Window (Months)", 6, 36, 12)
        trend_window = trend_df[
            trend_df['end_date'] >= pd.to_datetime(selected_end_date) - pd.DateOffset(months=months_back)
        ]

        metric_choice = st.selectbox("Select Metric", ["total_cases", "total_arrests", "total_violent_cases"], index=0)
        trend_window["rolling_avg"] = trend_window[metric_choice].rolling(4).mean()
        chart = alt.Chart(trend_window).mark_line(point=True).encode(
            x="end_date:T",
            y=alt.Y(f"{metric_choice}:Q", title=metric_choice.replace("_", " ").title()),
            color=alt.value("#007BFF"),
            tooltip=["end_date", metric_choice]
        )
        avg_line = alt.Chart(trend_window).mark_line(strokeDash=[5,5], color="red").encode(
            x="end_date:T",
            y="rolling_avg:Q"
        )
        st.altair_chart(chart + avg_line, use_container_width=True)


# --- Comparison Tab ---
with tab_comparison:
    st.subheader("📉 Prior Period Comparison")
    if not filtered_df.empty:
        comparison = []
        for curr, prev in COMPARISON_PAIRS:
            current_val = snapshot[curr]
            prior_val = snapshot[prev]
            delta = current_val - prior_val
            pct_change = (delta / prior_val * 100) if pd.notna(prior_val) and prior_val != 0 else np.nan
            comparison.append({
                "Metric": curr.replace("_", " ").title(),
                "Current": current_val,
                "Prior": prior_val,
                "Δ": delta,
                "% Change": pct_change
            })

        comp_df = pd.DataFrame(comparison)
        st.dataframe(comp_df.style.format({
            "Current": "{:,.0f}",
            "Prior": "{:,.0f}",
            "Δ": "{:,.0f}",
            "% Change": "{:,.2f}%"
        }))

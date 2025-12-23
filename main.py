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
file_path = "data/gold_data/chicago_crimes_gold_reports.parquet"
with st.spinner("Loading data..."):
    df = load_data(file_path)
df["end_date"] = pd.to_datetime(df["end_date"])
df["start_date"] = pd.to_datetime(df["start_date"])
df["report_date"] = pd.to_datetime(df["report_date"])

if df.empty:
    st.stop()

# ----------------------------
# Constants for Metrics
# ----------------------------
# Define metric groups based on column prefixes
CASE_METRICS = [c for c in df.columns if c.startswith("total_")]
UNIQUE_METRICS = [c for c in df.columns if c.startswith("unique_")]
CRIME_TYPE_METRICS = [c for c in df.columns if c.startswith("crime_") or c.startswith("fbi_")]
GEO_METRICS = [c for c in df.columns if c.startswith("community_area_") or c.startswith("ward_") or c.startswith("district_") or c.startswith("beat_")]

# Comparison pairs using a prior prefix
prior_prefix = "prior_"
_candidate_metrics = CASE_METRICS + UNIQUE_METRICS + CRIME_TYPE_METRICS
COMPARISON_PAIRS = [(m, f"{prior_prefix}{m}") for m in _candidate_metrics if f"{prior_prefix}{m}" in df.columns]

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
selected_end_date = pd.Timestamp(selected_end_date)
report_types = ['R12', 'YTD']
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
        for i in range(0, len(CASE_METRICS), 3):
            cols = st.columns(3)
            for c, m in zip(cols, CASE_METRICS[i:i+3]):
                c.metric(label=m.replace("_", " ").title(), value=f"{snapshot[m]:,.0f}")

        # Unique categories
        st.subheader("🔑 Unique Categories")
        cols = st.columns(min(3, len(UNIQUE_METRICS)))
        for c, m in zip(cols, UNIQUE_METRICS):
            c.metric(label=m.replace("_", " ").title(), value=f"{snapshot[m]:,.0f}")

        # Crime type metrics (show first 6 as example)
        st.subheader("🚨 Crime Type Metrics")
        cols = st.columns(min(3, len(CRIME_TYPE_METRICS)))
        for i in range(0, min(6, len(CRIME_TYPE_METRICS)), 3):
            cols = st.columns(3)
            for c, m in zip(cols, CRIME_TYPE_METRICS[i:i+3]):
                c.metric(label=m.replace("_", " ").title(), value=f"{snapshot[m]:,.0f}")
# --- Trends Tab ---
with tab_trends:
    st.subheader("📈 Trends Over Time")
    if not trend_df.empty:
        months_back = st.slider("Select Trend Window (Months)", 6, 72, 12)
        trend_window = trend_df[
            trend_df['end_date'] >= pd.to_datetime(selected_end_date) - pd.DateOffset(months=months_back)
        ].sort_values("end_date").copy()

        metric_choice = st.selectbox("Select Metric", CASE_METRICS, index=0)
        trend_window = trend_window.assign(rolling_avg=trend_window[metric_choice].rolling(10).mean())

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

# --- Crime Composition Tab ---
with tab_crimes:
    st.subheader("🚨 Crime Composition")
    if not filtered_df.empty:
        # Dropdown to select metric type
        crime_metric_type = st.selectbox(
            "Select Crime Metric Type",
            ("Crime Type", "FBI Code"),
            key="crime_metric_type_select"
        )

        if crime_metric_type == "Crime Type":
            crime_cols = [col for col in CRIME_TYPE_METRICS if col.startswith("crime_")]
        else:
            crime_cols = [col for col in CRIME_TYPE_METRICS if col.startswith("fbi_")]

        crime_data = {col: snapshot[col] for col in crime_cols if col in snapshot}
        crime_df = pd.DataFrame(list(crime_data.items()), columns=["Crime Type", "Count"])
        crime_df = crime_df.sort_values("Count", ascending=False)
        st.dataframe(crime_df)
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
    st.subheader("🏙️ Geographic Breakdown")
    if not filtered_df.empty:
        geo_type = st.selectbox(
            "Select Geographic Type",
            ("District", "Ward", "Community Area", "Beat"),
            key="geo_type_select"
        )

        if geo_type == "Ward":
            geo_cols = [col for col in GEO_METRICS if col.startswith("ward_")]
            geojson_path = "data/geojson/chicago_wards.geojson"
            id_field = "ward_id"
        elif geo_type == "District":
            geo_cols = [col for col in GEO_METRICS if col.startswith("district_")]
            geojson_path = "data/geojson/chicago_districts.geojson"
            id_field = "dist_num"
        elif geo_type == "Community Area":
            geo_cols = [col for col in GEO_METRICS if col.startswith("community_area_")]
            geojson_path = "data/geojson/chicago_community_areas.geojson"
            id_field = "area_numbe"
        else:  # Beat
            geo_cols = [col for col in GEO_METRICS if col.startswith("beat_")]
            geojson_path = "data/geojson/chicago_beats.geojson"
            id_field = "beat_num"

        # Comparison option
        compare_option = st.selectbox(
            "Compare (value to visualize)",
            ("Current", "Prior", "Difference (Current - Prior)", "% Change (Current vs Prior)"),
            key="geo_compare_select"
        )

        # Build geo dataframe with Current and Prior columns
        geo_rows = []
        for col in geo_cols:
            try:
                geo_id = int(col.split("_")[-1])
            except Exception:
                continue
            current_val = snapshot.get(col, np.nan)
            prior_val = snapshot.get(f"{prior_prefix}{col}", np.nan)
            geo_rows.append({"Geography": geo_id, "Current": current_val, "Prior": prior_val})

        if len(geo_rows) == 0:
            st.info("No geographic data available for the selected type.")
        else:
            geo_df = pd.DataFrame(geo_rows)

            # Compute the Count column based on selection
            if compare_option == "Current":
                geo_df["Count"] = geo_df["Current"]
            elif compare_option == "Prior":
                geo_df["Count"] = geo_df["Prior"]
            elif compare_option == "Difference (Current - Prior)":
                geo_df["Count"] = geo_df["Current"] - geo_df["Prior"]
            else:  # % Change
                geo_df["Count"] = np.where(
                    (geo_df["Prior"].notna()) & (geo_df["Prior"] != 0),
                    (geo_df["Current"] - geo_df["Prior"]) / geo_df["Prior"] * 100,
                    np.nan
                )

            geo_df["Geography"] = geo_df["Geography"].astype(int).astype(str)
            geo_df = geo_df.sort_values("Count", ascending=False)

            # --- Map Visualization ---
            if os.path.exists(geojson_path):
                gdf = gpd.read_file(geojson_path)
                gdf[id_field] = gdf[id_field].astype(int).astype(str)
                merged = gdf.merge(geo_df, left_on=id_field, right_on="Geography", how="left", indicator=True)
                merged["Count"] = merged["Count"].fillna(0)

                # build RGBA colors per feature (0-255) using a matplotlib colormap
                min_count, max_count = merged["Count"].min(), merged["Count"].max()
                if min_count == max_count:
                    vmin, vmax = 0, max(1, float(max_count))
                else:
                    vmin, vmax = float(min_count), float(max_count)

                # use a diverging norm / colormap when values span negative to positive
                if (min_count < 0) and (max_count > 0):
                    norm = mcolors.TwoSlopeNorm(vmin=vmin, vcenter=0.0, vmax=vmax)
                    cmap = cm.get_cmap("RdYlGn")  # negatives -> red, positives -> green
                else:
                    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
                    cmap = cm.get_cmap("YlGn")

                def count_to_rgba(val):
                    " handle missing values with a neutral gray"
                    
                    if pd.isna(val):
                        alpha = 0.75
                        return [200, 200, 200, int(alpha * 255)]
                    r, g, b, a = cmap(norm(val))
                    alpha = 0.75
                    return [int(r * 255), int(g * 255), int(b * 255), int(alpha * 255)]

                merged["fill_color"] = merged["Count"].apply(count_to_rgba)

                geojson_dict = merged.__geo_interface__

                layer = pdk.Layer(
                    "GeoJsonLayer",
                    data=geojson_dict,
                    get_fill_color="properties.fill_color",
                    pickable=True,
                    auto_highlight=True,
                    get_line_color=[0, 0, 0, 80],
                    line_width_min_pixels=1,
                    filled=True,
                    stroked=True,
                    extruded=False,
                    opacity=0.8,
                )
                midpoint = (merged.geometry.centroid.y.mean(), merged.geometry.centroid.x.mean())
                view_state = pdk.ViewState(
                    latitude=midpoint[0],
                    longitude=midpoint[1],
                    zoom=9,
                    pitch=0,
                )
                st.pydeck_chart(
                    pdk.Deck(
                        layers=[layer],
                        initial_view_state=view_state,
                        tooltip={"text": f"{geo_type}: {{{id_field}}}\nValue: {{Count}}"}
                    )
                )

                chart = alt.Chart(geo_df).mark_bar().encode(
                    x=alt.X("Count:Q", sort="-y"),
                    y=alt.Y("Geography:N", sort="-x"),
                    tooltip=["Geography", "Count"]
                ).properties(width=600, height=400)
                st.altair_chart(chart, use_container_width=True)
            else:
                st.warning(f"GeoJSON file not found: {geojson_path}")
    else:
        st.info("No geographic breakdown data available for this report.")


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

DASHBOARD_VERSION = "v1.0.0"

# Sidebar enhancements
st.sidebar.markdown(f"**Dashboard Version:** `{DASHBOARD_VERSION}`")
st.sidebar.markdown(f"**Streamlit version:** `{st.__version__}`")

# Footer enhancement
st.markdown("---")
st.markdown(
    f"<div style='text-align: center; color: gray;'>"
    f"Chicago Crimes Dashboard {DASHBOARD_VERSION} | Author: Salik Hussaini | "
    "Powered by Streamlit"
    "</div>",
    unsafe_allow_html=True
)
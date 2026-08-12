# dashboard.py
import streamlit as st
import altair as alt
import pandas as pd
import os
import geopandas as gpd
from shapely.geometry import Point
import pydeck as pdk
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors


# ----------------------------
# Load Data
# ----------------------------
@st.cache_data(ttl=300)  # Cache for 5 minutes, then reload fresh data
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
# Load Forecast Data
# ----------------------------
@st.cache_data(ttl=300)  # Cache for 5 minutes, then reload fresh data
def load_forecast_data(file_path: str) -> pd.DataFrame:
    """Load the crime count forecast data."""
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, parse_dates=["date"])
        else:
            st.error("Unsupported file format. Use CSV.")
            return pd.DataFrame()
    except FileNotFoundError:
        st.error(f"❌ Forecast data file not found at: {file_path}")
        return pd.DataFrame()
    return df

# ----------------------------
# Load Choropleth Data
# ----------------------------
@st.cache_data(ttl=300)  # Cache for 5 minutes, then reload fresh data
def load_choropleth_data(file_path: str) -> pd.DataFrame:
    """Load the zip code choropleth data."""
    try:
        if file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
            if 'report_end_date' in df.columns:
                df['report_end_date'] = pd.to_datetime(df['report_end_date'])
        else:
            st.error("Unsupported file format. Use Parquet.")
            return pd.DataFrame()
    except FileNotFoundError:
        st.warning(f"Warning: Choropleth data file not found at: {file_path}")
        return pd.DataFrame()
    return df

# ----------------------------
# File Path
# ----------------------------
file_path = "data/gold_data/chicago_crimes_gold_reports_.parquet"
forecast_file_path = "data/gold_data/crime_count_forecasts.csv"
choropleth_file_path = "data/gold_data/chicago_crimes_zipcode_choropleth.parquet"
with st.spinner("Loading data..."):
    df = load_data(file_path)
    forecast_df = load_forecast_data(forecast_file_path)
    choropleth_df = load_choropleth_data(choropleth_file_path)
df["end_date"] = pd.to_datetime(df["report_end_date"])
df["start_date"] = pd.to_datetime(df["report_start_date"])
df["report_date"] = df["report_date"]

if df.empty:
    st.stop()

# ----------------------------
# Constants for Metrics
# ----------------------------
# Define metric groups based on column prefixes
CASE_METRICS = [c for c in df.columns if c.startswith("total_")]
UNIQUE_METRICS = [c for c in df.columns if c.startswith("unique_")]
CRIME_TYPE_METRICS = [c for c in df.columns if c.startswith("crime_") or c.startswith("fbi_")]
CRIME_TYPE_METRICS += [c for c in df.columns if c.startswith("iucr_")]
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
    
    # Extract dates
    try:
        start_date = pd.to_datetime(snapshot['report_start_date'])
        end_date = pd.to_datetime(snapshot['report_end_date'])
        prior_start_date = start_date - pd.Timedelta(days=365)
        prior_end_date = end_date - pd.Timedelta(days=365)
        
        st.write(f"**Current Period:** {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}")
        st.write(f"**Prior Year Same Period:** {prior_start_date:%Y-%m-%d} to {prior_end_date:%Y-%m-%d}")
        st.write(f"**Report Date Generated:** {snapshot['report_date']}")
    except (KeyError, TypeError):
        st.write("Date information not available")

# ----------------------------
# Organize Dashboard into Tabs
# ----------------------------
st.header("📌 Dashboard Views")

tab_overview, tab_crimes, tab_geo, tab_trends, tab_comparison, tab_forecasts = st.tabs(
    ["📊 Overview", "🚨 Crime Composition", "🏙️ Geographic Breakdown", "📈 Trends", "📉 Comparison", "📈 Forecasts"]
)

# --- Overview Tab ---
with tab_overview:
    if not filtered_df.empty:
        st.subheader("Summary Metrics")
        
        # Info about prior/YoY comparison
        st.info(
            "📅 **Year-over-Year (YoY) Comparison**: The percent change (%) shown next to each metric represents the change compared to the same period last year. "
            "For example, a 12-month rolling report compares June 2020-June 2021 crimes to June 2019-June 2020 crimes. "
            "Green ↑ indicates increase, Red ↓ indicates decrease."
        )

        # Case counts
        st.subheader("📊 Case Counts")
        for i in range(0, len(CASE_METRICS), 3):
            cols = st.columns(3)
            for c, m in zip(cols, CASE_METRICS[i:i+3]):
                current_val = snapshot[m]
                prior_val = snapshot.get(f"prior_{m}", None)
                pct_change = ((current_val - prior_val) / prior_val * 100) if pd.notna(prior_val) and prior_val != 0 else None
                c.metric(label=m.replace("_", " ").title(), value=f"{current_val:,.0f}", delta=f"{pct_change:.1f}%" if pct_change is not None else None)

        # Arrest Efficiency
        st.subheader("👮 Arrest Efficiency")
        arrest_cols = st.columns(3)
        with arrest_cols[0]:
            st.metric("Total Cases", f"{snapshot['total_cases']:,.0f}")
        with arrest_cols[1]:
            st.metric("Total Arrests", f"{snapshot['total_arrests']:,.0f}")
        with arrest_cols[2]:
            arrest_rate = (snapshot['total_arrests'] / snapshot['total_cases'] * 100) if snapshot['total_cases'] > 0 else 0
            prior_arrest_rate = (snapshot.get('prior_total_arrests', 0) / snapshot.get('prior_total_cases', 1) * 100) if snapshot.get('prior_total_cases', 0) > 0 else 0
            arrest_rate_change = arrest_rate - prior_arrest_rate if pd.notna(prior_arrest_rate) else None
            st.metric("Arrest Rate %", f"{arrest_rate:.1f}%", delta=f"{arrest_rate_change:.1f}%" if arrest_rate_change is not None else None)

        # Unique categories
        st.subheader("🔑 Unique Categories")
        cols = st.columns(min(3, len(UNIQUE_METRICS)))
        for c, m in zip(cols, UNIQUE_METRICS):
            current_val = snapshot[m]
            prior_val = snapshot.get(f"prior_{m}", None)
            pct_change = ((current_val - prior_val) / prior_val * 100) if pd.notna(prior_val) and prior_val != 0 else None
            c.metric(label=m.replace("_", " ").title(), value=f"{current_val:,.0f}", delta=f"{pct_change:.1f}%" if pct_change is not None else None)


        # slider default 5
        slider_top_n = st.slider("Select Number of Top Crime Types", 5, 50, 5)
        # Crime type metrics (show top crime types by count - exclude FBI Code and IUCR)
        st.subheader(f"🚨 Crime Types (Top {slider_top_n})")

        # Filter to only crime_ columns (exclude fbi_ and iucr_)
        crime_cols_only = [m for m in CRIME_TYPE_METRICS if m.startswith("crime_")]
        
        # Sort crime types by current value
        crime_type_sorted = sorted(
            [(m, snapshot[m]) for m in crime_cols_only],
            key=lambda x: x[1],
            reverse=True
        )
        
        # Display top 15 crime types
        top_crime_types = crime_type_sorted[:slider_top_n]
        
        for i in range(0, len(top_crime_types), 3):
            cols = st.columns(3)
            for c, (m, _) in zip(cols, top_crime_types[i:i+3]):
                current_val = snapshot[m]
                prior_val = snapshot.get(f"prior_{m}", None)
                pct_change = ((current_val - prior_val) / prior_val * 100) if pd.notna(prior_val) and prior_val != 0 else None
                c.metric(label=m.replace("_", " ").title(), value=f"{current_val:,.0f}", delta=f"{pct_change:.1f}%" if pct_change is not None else None)
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
        st.altair_chart(chart + avg_line, width='stretch')

# --- Crime Composition Tab ---
with tab_crimes:
    st.subheader("🚨 Crime Composition & Patterns")
    if not filtered_df.empty:
        # ===== Section 1: Crime Mix Donut Charts =====
        st.subheader("📊 Crime Distribution")
        
        # Dropdown to select chart type
        chart_type = st.selectbox(
            "Select Crime Breakdown Type",
            ("Crime Category", "Crime Type", "FBI Code", "IUCR"),
            key="crime_mix_type_select"
        )
        
        if chart_type == "Crime Category":
            # Original category mix
            crime_mix = pd.DataFrame({
                "Category": ["Violent Crimes", "Property Crimes", "Drug Crimes", "Other Crimes"],
                "Count": [
                    snapshot.get('total_violent_cases', 0),
                    snapshot.get('total_property_cases', 0),
                    snapshot.get('total_drug_cases', 0),
                    snapshot['total_cases'] - snapshot.get('total_violent_cases', 0) - snapshot.get('total_property_cases', 0) - snapshot.get('total_drug_cases', 0)
                ]
            })
            label_col = "Category"
        else:
            # Get crime type, FBI, or IUCR breakdown
            if chart_type == "Crime Type":
                crime_cols = [col for col in CRIME_TYPE_METRICS if col.startswith("crime_")]
            elif chart_type == "FBI Code":
                crime_cols = [col for col in CRIME_TYPE_METRICS if col.startswith("fbi_")]
            else:  # IUCR
                crime_cols = [col for col in CRIME_TYPE_METRICS if col.startswith("iucr_")]
            
            crime_data = {col.replace("crime_", "").replace("fbi_", "").replace("iucr_", "").replace("_", " ").title(): snapshot[col] 
                         for col in crime_cols if col in snapshot}
            crime_mix = pd.DataFrame(list(crime_data.items()), columns=[chart_type, "Count"])
            crime_mix = crime_mix.sort_values("Count", ascending=False).head(10)  # Top 10 for readability
            label_col = chart_type
        
        donut_chart = alt.Chart(crime_mix).mark_arc(innerRadius=50).encode(
            theta="Count:Q",
            color=alt.Color(f"{label_col}:N", scale=alt.Scale(scheme="tableau20")),
            tooltip=[label_col, "Count"]
        ).properties(width=600, height=400)
        st.altair_chart(donut_chart, use_container_width=True)
        
        # ===== Section 3: Weekend vs Weekday Comparison =====
        st.subheader("📅 Temporal Patterns")
        col1, col2, col3, col4 = st.columns(4)
        
        # Weekend cases with YoY change
        with col1:
            weekend_cases = snapshot.get('total_weekend_cases', 0)
            prior_weekend = snapshot.get('prior_total_weekend_cases', None)
            weekend_pct_change = ((weekend_cases - prior_weekend) / prior_weekend * 100) if pd.notna(prior_weekend) and prior_weekend > 0 else None
            st.metric("Weekend Cases", f"{weekend_cases:,.0f}", delta=f"{weekend_pct_change:+.1f}%" if weekend_pct_change is not None else None)
        
        # Weekday cases with YoY change
        with col2:
            weekday_cases = snapshot['total_cases'] - snapshot.get('total_weekend_cases', 0)
            prior_weekday = snapshot.get('prior_total_cases', 0) - snapshot.get('prior_total_weekend_cases', 0)
            weekday_pct_change = ((weekday_cases - prior_weekday) / prior_weekday * 100) if prior_weekday > 0 else None
            st.metric("Weekday Cases", f"{weekday_cases:,.0f}", delta=f"{weekday_pct_change:+.1f}%" if weekday_pct_change is not None else None)
        
        # Daytime cases with YoY change
        with col3:
            daytime_cases = snapshot.get('total_daytime_cases', 0)
            prior_daytime = snapshot.get('prior_total_daytime_cases', None)
            daytime_pct_change = ((daytime_cases - prior_daytime) / prior_daytime * 100) if pd.notna(prior_daytime) and prior_daytime > 0 else None
            st.metric("Daytime Cases", f"{daytime_cases:,.0f}", delta=f"{daytime_pct_change:+.1f}%" if daytime_pct_change is not None else None)
        
        # Nighttime cases with YoY change
        with col4:
            nighttime_cases = snapshot.get('total_nighttime_cases', 0)
            prior_nighttime = snapshot.get('prior_total_nighttime_cases', None)
            nighttime_pct_change = ((nighttime_cases - prior_nighttime) / prior_nighttime * 100) if pd.notna(prior_nighttime) and prior_nighttime > 0 else None
            st.metric("Nighttime Cases", f"{nighttime_cases:,.0f}", delta=f"{nighttime_pct_change:+.1f}%" if nighttime_pct_change is not None else None)
        
        # Temporal comparison chart
        temporal_data = pd.DataFrame({
            "Period": ["Weekday", "Weekend", "Daytime", "Nighttime"],
            "Cases": [
                snapshot['total_cases'] - snapshot.get('total_weekend_cases', 0),
                snapshot.get('total_weekend_cases', 0),
                snapshot.get('total_daytime_cases', 0),
                snapshot.get('total_nighttime_cases', 0)
            ]
        })
        
        temporal_chart = alt.Chart(temporal_data).mark_bar().encode(
            x=alt.X("Period:N", sort=["Weekday", "Weekend", "Daytime", "Nighttime"]),
            y="Cases:Q",
            color=alt.Color("Period:N", scale=alt.Scale(scheme="set2")),
            tooltip=["Period", "Cases"]
        ).properties(width=600, height=300)
        st.altair_chart(temporal_chart, use_container_width=True)
        
        # ===== Section 4: Domestic Violence Tracking =====
        st.subheader("🏠 Domestic Violence")
        col1, col2, col3 = st.columns(3)
        
        domestic_cases = snapshot.get('total_domestic_cases', 0)
        prior_domestic = snapshot.get('prior_total_domestic_cases', None)
        
        # Domestic cases metric
        with col1:
            domestic_change_pct = ((domestic_cases - prior_domestic) / prior_domestic * 100) if pd.notna(prior_domestic) and prior_domestic > 0 else None
            st.metric("Domestic Cases", f"{domestic_cases:,.0f}", delta=f"{domestic_change_pct:+.1f}%" if domestic_change_pct is not None else None)
        
        # Domestic as % of total
        with col2:
            domestic_pct = (domestic_cases / snapshot['total_cases'] * 100) if snapshot['total_cases'] > 0 else 0
            prior_total_cases = snapshot.get('prior_total_cases', None)
            prior_domestic_pct = (prior_domestic / prior_total_cases * 100) if pd.notna(prior_domestic) and pd.notna(prior_total_cases) and prior_total_cases > 0 else None
            domestic_pct_change = domestic_pct - prior_domestic_pct if prior_domestic_pct is not None else None
            st.metric("% of Total Cases", f"{domestic_pct:.1f}%", delta=f"{domestic_pct_change:+.1f}%" if domestic_pct_change is not None else None)
        
        # YoY absolute change
        with col3:
            absolute_change = domestic_cases - prior_domestic if pd.notna(prior_domestic) else None
            st.metric("Cases Change (YoY)", f"{absolute_change:+,.0f}" if absolute_change is not None else "N/A")

    else:
        st.info("No crime composition data available for this report.")

# --- Geographic Breakdown Tab ---
with tab_geo:
    st.subheader("🏙️ Geographic Breakdown")
    if not filtered_df.empty:
        geo_type = st.selectbox(
            "Select Geographic Type",
            ("District", "Ward", "Community Area", "Beat", "Zip Code"),
            key="geo_type_select"
        )

        # Handle Zip Code separately (uses choropleth data)
        if geo_type == "Zip Code":
            if not choropleth_df.empty:
                # Filter choropleth data to match selected filters
                choropleth_filtered = choropleth_df[
                    (choropleth_df["report_type"] == selected_report_type) &
                    (choropleth_df["report_end_date"] == selected_end_date)
                ]
                
                if not choropleth_filtered.empty:
                    # Load geojson
                    geojson_path = "data/geojson/chicago_zip_codes.geojson"
                    if os.path.exists(geojson_path):
                        gdf = gpd.read_file(geojson_path)
                        
                        # Prepare data for merge
                        choropleth_filtered["zip_code"] = choropleth_filtered["zip_code"].astype(str)
                        gdf["zip"] = gdf["zip"].astype(str)
                        
                        # Merge choropleth data with geojson
                        merged = gdf.merge(
                            choropleth_filtered[["zip_code", "zip_code_crime_count", "total_cases"]],
                            left_on="zip",
                            right_on="zip_code",
                            how="left"
                        )
                        merged["zip_code_crime_count"] = merged["zip_code_crime_count"].fillna(0)
                        
                        # Create color mapping
                        min_count = merged["zip_code_crime_count"].min()
                        max_count = merged["zip_code_crime_count"].max()
                        
                        if min_count == max_count:
                            vmin, vmax = 0, max(1, float(max_count))
                        else:
                            vmin, vmax = float(min_count), float(max_count)
                        
                        norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
                        cmap = plt.get_cmap("YlOrRd")  # Yellow -> Orange -> Red for crime intensity
                        
                        def count_to_rgba(val):
                            """Convert crime count to RGBA color."""
                            if pd.isna(val) or val == 0:
                                alpha = 0.5
                                return [220, 220, 220, int(alpha * 255)]
                            r, g, b, a = cmap(norm(val))
                            alpha = 0.85
                            return [int(r * 255), int(g * 255), int(b * 255), int(alpha * 255)]
                        
                        merged["fill_color"] = merged["zip_code_crime_count"].apply(count_to_rgba)
                        
                        # Create pydeck layer
                        geojson_dict = merged.__geo_interface__
                        layer = pdk.Layer(
                            "GeoJsonLayer",
                            data=geojson_dict,
                            get_fill_color="properties.fill_color",
                            pickable=True,
                            auto_highlight=True,
                            get_line_color=[0, 0, 0, 100],
                            line_width_min_pixels=1,
                            filled=True,
                            stroked=True,
                            opacity=0.85,
                        )
                        
                        # Calculate map center
                        merged_projected = merged.to_crs(epsg=3857)
                        centroid_projected = merged_projected.geometry.centroid
                        centroid_mean_x = centroid_projected.x.mean()
                        centroid_mean_y = centroid_projected.y.mean()
                        centroid_point = gpd.GeoSeries([Point(centroid_mean_x, centroid_mean_y)], crs='EPSG:3857').to_crs('EPSG:4326')
                        midpoint = (centroid_point.y.values[0], centroid_point.x.values[0])
                        
                        view_state = pdk.ViewState(
                            latitude=midpoint[0],
                            longitude=midpoint[1],
                            zoom=10,
                            pitch=0,
                        )
                        
                        st.pydeck_chart(
                            pdk.Deck(
                                layers=[layer],
                                initial_view_state=view_state,
                                tooltip={"text": "Zip Code: {zip}\nCrimes: {zip_code_crime_count}"}
                            )
                        )
                        
                        # Show top zip codes by crime count
                        st.subheader("📊 Top Zip Codes by Crime Count")
                        zip_summary = choropleth_filtered[["zip_code", "zip_code_crime_count"]].sort_values(
                            "zip_code_crime_count", ascending=False
                        ).head(20)
                        
                        chart = alt.Chart(zip_summary).mark_bar().encode(
                            x=alt.X("zip_code_crime_count:Q", title="Crime Count"),
                            y=alt.Y("zip_code:N", sort="-x", title="Zip Code"),
                            tooltip=["zip_code", "zip_code_crime_count"],
                            color=alt.Color("zip_code_crime_count:Q", scale=alt.Scale(scheme="reds"))
                        ).properties(width=600, height=600)
                        st.altair_chart(chart, use_container_width=True)
                        
                    else:
                        st.warning(f"GeoJSON file not found: {geojson_path}")
                else:
                    st.info("No zip code data available for the selected filters.")
            else:
                st.info("Choropleth data not available. Please ensure zip code enrichment is complete.")
        
        # Handle other geographic types
        else:
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
                        cmap = plt.get_cmap("RdYlGn")  # negatives -> red, positives -> green
                    else:
                        norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
                        cmap = plt.get_cmap("YlGn")

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
                    # Reproject to projected CRS for accurate centroid calculation
                    merged_projected = merged.to_crs(epsg=3857)
                    centroid_projected = merged_projected.geometry.centroid
                    centroid_mean_x = centroid_projected.x.mean()
                    centroid_mean_y = centroid_projected.y.mean()
                    # Convert centroid back to lat/lon
                    centroid_point = gpd.GeoSeries([Point(centroid_mean_x, centroid_mean_y)], crs='EPSG:3857').to_crs('EPSG:4326')
                    midpoint = (centroid_point.y.values[0], centroid_point.x.values[0])
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
                    st.altair_chart(chart, width='stretch')
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
        # Format numeric columns for display
        comp_df["Current"] = comp_df["Current"].apply(lambda x: f"{x:,.0f}")
        comp_df["Prior"] = comp_df["Prior"].apply(lambda x: f"{x:,.0f}")
        comp_df["Δ"] = comp_df["Δ"].apply(lambda x: f"{x:,.0f}")
        comp_df["% Change"] = comp_df["% Change"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
        st.dataframe(comp_df)

# --- Forecasts Tab ---
with tab_forecasts:
    st.subheader("📈 Monthly Crime Trends with Forecasts")

    if not forecast_df.empty:
        st.subheader("Crime Count Forecasts")

        # Select the metric to visualize
        metric_options = ["actual_crime_count"] + [
            col for col in forecast_df.columns if col.startswith("predicted_crime_count_")
        ]

        # Replace negative values with NaN
        forecast_df[metric_options[1:]] = forecast_df[metric_options[1:]].map(lambda x: x if x >= 0 else np.nan)

        # Filter valid columns (exclude columns with all NaN values)
        valid_columns = ["date", "actual_crime_count"] + [
            col for col in metric_options[1:] if not forecast_df[col].isna().all()
        ]
        metric_data = forecast_df[valid_columns].copy()

        # Ensure the date column is a datetime type
        metric_data["date"] = pd.to_datetime(metric_data["date"])

        # Add Slider for years
        cutoff_years = st.slider('Select number of years to display', min_value=1, max_value=10, value=2)
        # Filter to the selected number of years
        cutoff_date = metric_data["date"].max() - pd.DateOffset(years=cutoff_years)
        metric_data = metric_data[metric_data["date"] >= cutoff_date]

        if not metric_data.empty:
            # Melt the data for easier plotting with Altair
            metric_data = metric_data.melt(id_vars=["date"], var_name="Model", value_name="Crime Count")

            # Calculate the y-axis maximum value (10x the max of actual_crime_count)
            y_max = metric_data[metric_data["Model"] == "actual_crime_count"]["Crime Count"].max() * 1.3
            if pd.isna(y_max) or y_max == 0:
                y_max = 1  # Set a default value if y_max is invalid


            # Create Altair chart
            chart = alt.Chart(metric_data).mark_line(point=True).encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y(
                    "Crime Count:Q",
                    title="Crime Count"
                    , scale=alt.Scale(domain=[-50, y_max], clamp=True)  # Set y-axis range
                ),
                color=alt.Color("Model:N", title="Model"),  # Dynamically assign colors to each model
                tooltip=["date:T", "Model:N", "Crime Count:Q"]
            ).properties(title="Actual and Forecasted Crime Counts")

            st.altair_chart(chart, width='stretch')
        else:
            st.warning("No data available after filtering. Check your data for values > 0.")
    else:
        st.warning("No forecast data available.")
# ----------------------------
DASHBOARD_VERSION = "v1.1.0"
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
# dashboard.py
import streamlit as st
import altair as alt
import pandas as pd
import os

# ----------------------------
# Load Data
# ----------------------------
@st.cache_data
def load_data(file_path: str) -> pd.DataFrame:
    """Load the Chicago crime summary data."""
    try:
        df = pd.read_csv(
            file_path,
            parse_dates=["Start Date", "End Date", "Report Date", "Report Date_prior"]
        )
    except FileNotFoundError:
        st.error(f"❌ Data file not found at: {file_path}")
        return pd.DataFrame()
    return df


# ----------------------------
# Constants for Metrics
# ----------------------------
CASE_METRICS = ["Total Cases", "Total Arrests", "Total Domestic Cases",
                "Total Violent Cases", "Total Property Cases"]

UNIQUE_METRICS = ["Unique Crime Types", "Unique Beats", "Unique Wards", "Unique Districts"]

RATE_METRICS = ["Arrest Rate", "Domestic Rate", "Violent Rate", "Property Rate"]

CHANGE_METRICS = ["Case Growth %", "Arrest Rate Change", "Domestic Rate Change",
                  "Violent Rate Change", "Property Rate Change"]

COMPARISON_METRICS = ["Total Cases", "Total Arrests", "Total Violent Cases",
                      "Total Property Cases", "Arrest Rate", "Domestic Rate",
                      "Violent Rate", "Property Rate"]


# ----------------------------
# File Path
# ----------------------------
current_file_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_file_dir)
file_path = os.path.join(parent_dir, "data", "agg", "chicago_crime_summary_stats.csv")

df = load_data(file_path)

if df.empty:
    st.stop()


# ----------------------------
# Dashboard Title
# ----------------------------
st.title("📊 Chicago Police Report Dashboard")


# ----------------------------
# Sidebar Filters
# ----------------------------
st.sidebar.header("Filters")

end_dates = df["End Date"].sort_values(ascending=False).unique()
selected_end_date = st.sidebar.selectbox("Select Report End Date", end_dates, index=0)

report_types = df["Report Type"].unique().tolist()
selected_report_type = st.sidebar.multiselect(
    "Select Report Type",
    report_types,
    default=report_types[0]
)

# Apply filters
filtered_df = df[
    (df["Report Type"].isin(selected_report_type)) &
    (df["End Date"] == selected_end_date)
]

trend_df = df[
    (df["Report Type"].isin(selected_report_type)) &
    (df["End Date"] <= selected_end_date)
]


# ----------------------------
# Reporting Period
# ----------------------------
if not filtered_df.empty:
    snapshot = filtered_df.iloc[0]

    st.subheader("🗓️ Reporting Period")
    st.write(f"**Start Date:** {snapshot['Start Date'].strftime('%Y-%m-%d')}")
    st.write(f"**End Date:** {snapshot['End Date'].strftime('%Y-%m-%d')}")
    st.write(f"**Report Date Generated:** {snapshot['Report Date']}")


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
        for c, m in zip(cols, CASE_METRICS):
            c.metric(label=m, value=f"{snapshot[m]:,.0f}")

        # Unique categories
        st.subheader("🔑 Unique Categories")
        cols = st.columns(len(UNIQUE_METRICS))
        for c, m in zip(cols, UNIQUE_METRICS):
            c.metric(label=m, value=f"{snapshot[m]:,.0f}")

        # Current rates
        st.subheader("📈 Current Rates (%)")
        st.bar_chart(snapshot[RATE_METRICS].to_frame("Rate (%)"))

        # Growth & rate changes
        st.subheader("📉 Growth & Rate Changes (%)")
        st.bar_chart(snapshot[CHANGE_METRICS].to_frame("Change (%)"))

        # Highlight top crime
        st.subheader("🔥 Key Crime Highlight")
        st.metric(
            label=f"Most Reported Crime ({snapshot['Top 1 Crime Type']})",
            value=f"{snapshot['Top 1 Crime Count']:,}"
        )


# --- Crime Composition Tab ---
with tab_crimes:
    st.subheader("🚨 Top Crimes")
    if not filtered_df.empty:
        top_crimes = []
        for i in range(1, 12):  # Top 1 ... Top 11
            crime_col = f"Top {i} Crime Type"
            count_col = f"Top {i} Crime Count"
            if crime_col in snapshot and count_col in snapshot:
                top_crimes.append((snapshot[crime_col], snapshot[count_col]))

        top_crimes_df = pd.DataFrame(top_crimes, columns=["Crime Type", "Count"])
        top_crimes_df = top_crimes_df.sort_values("Count", ascending=False)

        crime_chart = alt.Chart(top_crimes_df).mark_bar().encode(
            x=alt.X("Count:Q", sort="-y"),
            y=alt.Y("Crime Type:N", sort="-x"),
            tooltip=["Crime Type", "Count"]
        ).properties(width=600, height=400)

        st.altair_chart(crime_chart, use_container_width=True)

# --- Geographic Breakdown Tab ---
with tab_geo:
    st.subheader("🏙️ Geographic Breakdown")
    if not filtered_df.empty:
        # Districts
        district_cols = [c for c in snapshot.index if c.startswith("District_") and not c.endswith("_prior")]
        district_data = snapshot[district_cols].to_frame("Cases").reset_index()
        district_data["District"] = district_data["index"].str.replace("District_", "")
        district_data = district_data.sort_values("Cases", ascending=False)

        district_chart = alt.Chart(district_data).mark_bar().encode(
            x=alt.X("Cases:Q"),
            y=alt.Y("District:N", sort="-x"),
            tooltip=["District", "Cases"]
        ).properties(width=600)

        st.subheader("Cases by District")
        st.altair_chart(district_chart, use_container_width=True)

        # Wards
        ward_cols = [c for c in snapshot.index if c.startswith("Ward_") and not c.endswith("_prior")]
        ward_data = snapshot[ward_cols].to_frame("Cases").reset_index()
        ward_data["Ward"] = ward_data["index"].str.replace("Ward_", "")
        ward_data = ward_data.sort_values("Cases", ascending=False)

        ward_chart = alt.Chart(ward_data).mark_bar().encode(
            x=alt.X("Cases:Q"),
            y=alt.Y("Ward:N", sort="-x"),
            tooltip=["Ward", "Cases"]
        ).properties(width=600)

        st.subheader("Cases by Ward")
        st.altair_chart(ward_chart, use_container_width=True)

# --- Trends Tab ---
with tab_trends:
    st.subheader("📈 Trends Over Time")
    if not trend_df.empty:
        months_back = st.slider("Select Trend Window (Months)", 6, 36, 20)
        trend_window = trend_df[trend_df['End Date'] >= selected_end_date - pd.DateOffset(months=months_back)].set_index("End Date")

        subtab1, subtab2 = st.tabs(["Rates", "Case Growth %"])
        with subtab1:
            st.line_chart(trend_window[RATE_METRICS])
        with subtab2:
            st.line_chart(trend_window["Case Growth %"])

# --- Comparison Tab ---
with tab_comparison:
    st.subheader("📉 Prior Period Comparison")
    if not filtered_df.empty:
        current = snapshot[COMPARISON_METRICS]
        prior = snapshot[[c + "_prior" for c in COMPARISON_METRICS]].rename(
            lambda x: x.replace("_prior", ""), axis=0
        )
        comparison = pd.DataFrame({"Current": current, "Prior": prior})
        comparison["Δ"] = comparison["Current"] - comparison["Prior"]

        st.dataframe(comparison.style.format("{:,.2f}"))

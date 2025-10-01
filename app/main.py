# dashboard.py
import streamlit as st
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
    default=report_types
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
# KPIs (Snapshot)
# ----------------------------
st.header("Summary Metrics")

if not filtered_df.empty:
    # Case Counts
    st.subheader("📊 Case Counts")
    cols = st.columns(len(CASE_METRICS))
    for c, m in zip(cols, CASE_METRICS):
        c.metric(label=m, value=f"{snapshot[m]:,.0f}")

    # Unique Categories
    st.subheader("🔑 Unique Categories")
    cols = st.columns(len(UNIQUE_METRICS))
    for c, m in zip(cols, UNIQUE_METRICS):
        c.metric(label=m, value=f"{snapshot[m]:,.0f}")

    # Current Rates
    st.subheader("📈 Current Rates (%)")
    rate_df = snapshot[RATE_METRICS].to_frame("Rate (%)")
    st.bar_chart(rate_df)

    # Growth & Rate Changes
    st.subheader("📉 Growth & Rate Changes (%)")
    change_df = snapshot[CHANGE_METRICS].to_frame("Change (%)")
    st.bar_chart(change_df)


# ----------------------------
# Trends Over Time
# ----------------------------
st.header("Trends Over Time")

if not trend_df.empty:
    trend_indexed = trend_df.set_index("End Date")

    # User-selectable trend window
    months_back = st.slider("Select Trend Window (Months)", 6, 36, 20)
    trend_window = trend_indexed.loc[selected_end_date - pd.DateOffset(months=months_back):selected_end_date]

    tabs = st.tabs(["Rates", "Case Growth %"])
    with tabs[0]:
        st.line_chart(trend_window[RATE_METRICS])
    with tabs[1]:
        st.line_chart(trend_window["Case Growth %"])


# ----------------------------
# Prior Period Comparison
# ----------------------------
st.header("Prior Period Comparison")

if not filtered_df.empty:
    st.write("Current vs Prior Metrics")

    current = snapshot[COMPARISON_METRICS]
    prior = snapshot[[c + "_prior" for c in COMPARISON_METRICS]].rename(
        lambda x: x.replace("_prior", ""), axis=0
    )

    comparison = pd.DataFrame({"Current": current, "Prior": prior})
    comparison["Δ"] = comparison["Current"] - comparison["Prior"]

    st.dataframe(comparison.style.format("{:,.2f}"))

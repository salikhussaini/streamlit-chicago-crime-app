# dashboard.py
import streamlit as st
import pandas as pd
import os
# Load your data
@st.cache_data
def load_data(file_path):
    df = pd.read_csv(file_path)
    return df

# Get the directory of the current file
current_file_dir = os.path.dirname(os.path.abspath(__file__))

# Go back one folder
parent_dir = os.path.dirname(current_file_dir)
# Replace with your CSV file path
df = load_data(f"{parent_dir}/data/agg/chicago_crime_summary_stats.csv")

st.title("Police Report Dashboard")

# --- Filters ---
st.sidebar.header("Filters")

# Filter by Report End Date (discrete)
end_dates = df['End Date'].sort_values(ascending=False).unique()
selected_end_date = st.sidebar.selectbox("Select Report End Date", end_dates, index=0)  # default: most recent

# Filter by Report Type
report_types = df['Report Type'].unique().tolist()
selected_report_type = st.sidebar.multiselect(
    "Select Report Type", 
    report_types, 
    default=report_types
)

# Apply filters
filtered_df = df[
    (df['Report Type'].isin(selected_report_type)) &
    (df['End Date'] == selected_end_date)
]
# sub filer for end date and 10 subsequent months
filter_df_new = df[
    (df['Report Type'].isin(selected_report_type)) &
    (df['End Date'] <= selected_end_date) &
    (df['End Date'] >= (pd.to_datetime(selected_end_date) - pd.DateOffset(months=20)).strftime('%Y-%m-%d')) 
]

# --- KPIs ---
st.header("Summary Metrics")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Cases", filtered_df['Total Cases'].sum())
col2.metric("Total Arrests", filtered_df['Total Arrests'].sum())
col3.metric("Total Violent Cases", filtered_df['Total Violent Cases'].sum())
col4.metric("Total Property Cases", filtered_df['Total Property Cases'].sum())

# --- Display filtered data ---
st.header("Filtered Data")
st.dataframe(filtered_df)

# --- Line Charts ---
st.header("Trends Over Time")
for col in ['Arrest Rate', 'Domestic Rate', 'Violent Rate', 'Property Rate']:
    st.subheader(f"{col} Over Time")
    st.line_chart(
        filter_df_new[
            [col, 'End Date']
        ].set_index('End Date')
        , x_label='End Date', y_label=col
    )
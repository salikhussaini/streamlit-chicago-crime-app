def summary_stats(df, st):
    total_records = len(df)
    unique_crime_types = df["primary type"].nunique()
    years_covered = f"{df['year'].min()} - {df['year'].max()}"

    st.subheader("Summary Statistics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", total_records)
    col2.metric("Unique Crime Types", unique_crime_types)
    col3.metric("Years Covered", years_covered)
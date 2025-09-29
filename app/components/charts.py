import plotly.express as px

def crime_by_type(df):
    counts = df["primary type"].value_counts().reset_index()
    counts.columns = ["Crime Type", "Count"]
    return px.bar(counts.head(10), x="Crime Type", y="Count", title="Top 10 Crime Types")

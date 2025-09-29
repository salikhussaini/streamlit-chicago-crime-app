def clean_data(df):
    df.columns = df.columns.str.lower()
    df = df.dropna(subset=["latitude", "longitude"])
    df.columns = df.columns.str.lower()
    return df

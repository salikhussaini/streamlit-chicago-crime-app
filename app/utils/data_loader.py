import pandas as pd
import streamlit as st

@st.cache_data
def load_data(path="data/chicago_crime_sample.csv", nrows=None):
    return pd.read_csv(path, parse_dates=["Date"], nrows=nrows)

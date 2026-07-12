# 📊 Chicago Police Report Dashboard

A comprehensive Streamlit dashboard for visualizing and analyzing Chicago Police Department crime data with interactive geographic maps, trend analysis, and forecasting capabilities.

## Overview

This dashboard provides real-time insights into Chicago crime statistics with:
- **Summary metrics** and case counts
- **Crime composition** breakdown by type, FBI code, and IUCR classification
- **Geographic visualization** by district, ward, community area, and beat
- **Zip code choropleth mapping** with crime intensity visualization
- **Trend analysis** with customizable time windows
- **Prior period comparisons** with delta and percentage changes
- **Crime forecasts** with actual vs. predicted values

## Features

### 📊 Overview Tab
- Case count metrics
- Unique crime categories
- Crime type metrics
- Customizable metric display in 3-column layout

### 🚨 Crime Composition Tab
- Filter crimes by:
  - Crime Type
  - FBI Code
  - IUCR (Illinois Uniform Crime Reporting)
- Bar charts ranked by frequency

### 🏙️ Geographic Breakdown Tab
- Interactive maps with PyDeck visualization
- Filter by geographic level:
  - **District**
  - **Ward**
  - **Community Area**
  - **Beat**
- Comparison options:
  - Current values
  - Prior period values
  - Difference (Current - Prior)
  - Percentage change
- Dynamic color mapping using diverging/sequential colormaps

### 🗺️ Zip Code Choropleth Tab
- Interactive zip code-level crime visualization with PyDeck
- Color-coded intensity mapping (Yellow → Orange → Red scale)
- Filters match selected report type and end date
- Top 20 zip codes by crime count bar chart
- Hover-enabled tooltips showing zip code and crime count
- Centroid-based map centering for optimal view

### 📈 Trends Tab
- Time-series visualization of crime metrics
- Adjustable trend window (6-72 months)
- 10-month rolling average overlay
- Metric selection dropdown

### 📉 Comparison Tab
- Side-by-side prior period comparison
- Calculates delta and percentage change
- Formatted metric display

### 📈 Forecasts Tab
- Actual crime counts vs. predicted forecasts
- Multiple forecast models support
- Handles negative values gracefully
- Dynamic y-axis scaling

## Setup & Installation

### Prerequisites
- Python 3.8+
- Virtual environment (venv/conda recommended)

### 1. Clone the Repository
```bash
git clone <repository-url>
cd streamlit-chicago-crime-app
```

### 2. Create Virtual Environment
```bash
# Using venv
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # macOS/Linux
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### Required Packages
```
streamlit
altair
pandas
geopandas
shapely
pydeck
numpy
matplotlib
```

## Data Structure

The dashboard expects the following data files:

```
data/
├── geojson/
│   ├── chicago_beats.geojson
│   ├── chicago_community_areas.geojson
│   ├── chicago_districts.geojson
│   ├── chicago_wards.geojson
│   └── chicago_zip_codes.geojson
├── gold_data/
│   ├── crime_count_forecasts.csv
│   ├── gold_parquet_reports/
│   │   └── chicago_crimes_gold_reports_.parquet
│   └── [optional] chicago_crimes_zipcode_choropleth.parquet
└── raw_data/
    └── gold_data_dash/
        └── [optional] chicago_crimes_zipcode_choropleth.parquet
```

### Data Format Requirements

**Main Data File** (`chicago_crimes_gold_reports_.parquet` or `.csv`):
- `report_date` - Date report was generated
- `start_date` - Reporting period start
- `end_date` - Reporting period end
- `report_type` - Report type (e.g., 'R12', 'YTD')
- Metrics with prefixes:
  - `total_*` - Case counts
  - `unique_*` - Unique categories
  - `crime_*` - Crime type counts
  - `fbi_*` - FBI code counts
  - `iucr_*` - IUCR code counts
  - `district_*`, `ward_*`, `community_area_*`, `beat_*` - Geographic metrics
  - `prior_*` - Prior period comparison values

**Forecast File** (`crime_count_forecasts.csv`):
- `date` - Forecast date
- `actual_crime_count` - Actual crime count
- `predicted_crime_count_*` - Multiple forecast model predictions

**Choropleth File** (`chicago_crimes_zipcode_choropleth.parquet`) - **Optional**:
- `report_type` - Report type (e.g., 'R12', 'YTD')
- `report_end_date` - Reporting period end date
- `zip_code` - Chicago zip code identifier
- `zip_code_crime_count` - Crime count for zip code
- `total_cases` - Total case count for zip code

## Live Demo

🚀 **Access the dashboard online:** https://app-chicago-crime.streamlit.app/

## Running the Dashboard Locally

```bash
streamlit run main.py
```

The dashboard will open in your default browser at `http://localhost:8501`

### Configuration Options

**Sidebar Filters:**
- **Report End Date** - Select the reporting period
- **Report Type** - Choose between 'R12' or 'YTD' reports

**Cache Settings:**
- Data is cached for 5 minutes (300 seconds)
- Adjust `ttl` parameter in `@st.cache_data()` decorator to change cache duration

## Project Structure

```
streamlit-chicago-crime-app/
├── main.py                    # Main dashboard application
├── requirements.txt           # Python dependencies
├── README.md                  # This file
├── LICENSE                    # Project license
└── data/
    ├── geojson/              # GeoJSON files for mapping
    ├── gold_data/            # Processed/aggregated data
```

## Key Functions

### Data Loading
- `load_data(file_path)` - Loads CSV or Parquet crime data with caching
- `load_forecast_data(file_path)` - Loads forecast CSV with caching
- `load_choropleth_data(file_path)` - Loads zip code choropleth Parquet data with caching

### Color Mapping
- Adaptive color normalization for geographic visualizations
- Diverging colormap (RdYlGn) for negative/positive values in geographic tabs
- Sequential colormap (YlOrRd) for positive-only values in choropleth visualization
- Dynamic normalization based on min/max values in dataset

### Data Aggregation
- Automatic metric grouping by prefix
- Dynamic comparison pair generation
- Geographic ID extraction and validation

## Technical Stack

- **Framework:** Streamlit
- **Data Processing:** Pandas, NumPy
- **Geospatial:** GeoPandas, Shapely
- **Visualization:** Altair, PyDeck, Matplotlib
- **Data Format:** Parquet, CSV, GeoJSON

## Performance Notes

- Data is cached for 5 minutes to balance freshness and performance
- GeoJSON files are loaded on-demand for selected geographic types
- Geographic centroid calculation uses projected coordinates (EPSG:3857) for accuracy

## Customization

### Adding New Metrics
1. Add columns with appropriate prefix to source data
2. Metrics are automatically detected and organized:
   - `total_*` → Case Metrics
   - `unique_*` → Unique Metrics
   - `crime_*`, `fbi_*`, `iucr_*` → Crime Type Metrics
   - Geographic prefixes → Geographic Metrics

### Adjusting Color Schemes
Modify the `count_to_rgba()` function in the Geographic Breakdown tab to change:
- Colormap (line: `cmap = plt.get_cmap("YlGn")`)
- Alpha transparency values
- Normalization approach

### Extending Tabs
To add new visualization tabs:
1. Create new tab in `st.tabs()` call
2. Add data logic and Altair/PyDeck visualization
3. Link to appropriate data columns

## Troubleshooting

**"Data file not found"**
- Verify file paths in code match your data directory structure
- Use relative paths from project root

**Map not loading**
- Ensure GeoJSON files exist at specified paths
- Check GeoJSON file format validity
- Verify geographic ID fields match data values

**Zip Code Choropleth not appearing**
- Choropleth tab requires optional `chicago_crimes_zipcode_choropleth.parquet` file
- If not available, the tab will display an informational message
- Ensure choropleth file has required columns: `report_type`, `report_end_date`, `zip_code`, `zip_code_crime_count`

**Forecast visualization issues**
- Check for NaN/negative values in forecast columns
- Ensure `actual_crime_count` has valid values
- Verify date column is properly formatted
- Negative forecast values are automatically replaced with NaN

## Author

Salik Hussaini

## License

See [LICENSE](LICENSE) file for details.

---

**Dashboard Version:** v1.0.0  
**Last Updated:** 2026-01-31  
Powered by Streamlit

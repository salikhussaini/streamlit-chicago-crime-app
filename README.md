# Chicago Crimes Dashboard & ETL

## Project Purpose
The **Chicago Crimes Dashboard & ETL** project aims to provide an interactive and insightful platform for analyzing crime data in Chicago. By leveraging historical and recent crime data, this project enables users to explore trends, visualize geographic distributions, and compare crime metrics over time. The dashboard is designed for policymakers, law enforcement agencies, researchers, and the general public to gain actionable insights into crime patterns and make data-driven decisions.

---

## Features
### Key Features of the Dashboard:
- **📊 Overview Metrics**:
  - Displays high-level crime statistics, including total cases, unique categories, and crime type breakdowns.
- **🚨 Crime Composition**:
  - Provides detailed insights into crime types and FBI codes, with visualizations for better understanding.
- **🏙️ Geographic Visualizations**:
  - Interactive maps to explore crime data by districts, wards, community areas, and police beats.
- **📈 Trends Over Time**:
  - Analyze crime trends over customizable time windows with rolling averages and comparisons.
- **📉 Prior Period Comparison**:
  - Compare current crime metrics with prior periods to identify changes and trends.
- **Filters**:
  - Dynamic filters for report type (e.g., R12, YTD) and reporting periods to customize the analysis.

---

## Data Sources
The raw data used in this project is sourced from the **Chicago Police Department**'s publicly available crime data. The data includes detailed records of reported crimes, including dates, locations, and crime types. GeoJSON files for geographic visualizations are also included to map crime data to specific districts, wards, community areas, and beats.

### Data Directory:
- **`data/raw_data/`**: Contains raw CSV files with crime data.
- **`data/geojson/`**: GeoJSON files for mapping geographic areas.
- **`data/transformed_data/`**: Intermediate data files generated during ETL processes.
- **`data/gold_data/`**: Final processed data used for the dashboard.

---

## Quick Start
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/chicago-crimes-dashboard.git
   cd chicago-crimes-dashboard
   ```
2. Install dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```
3. Run the dashboard:
   ```bash
   streamlit run main.py
   ```
4. Place raw data files in the `data/raw_data/` directory.

---

## ETL Pipeline

### Running the Complete Pipeline

The ETL pipeline consists of 7 stages that process Chicago crime data from raw API data through to dashboard aggregations:

**Run entire pipeline (incremental mode - skips existing files):**
```bash
python src/orchestrate_pipeline.py
```

**Run entire pipeline with full reprocessing:**
```bash
python src/orchestrate_pipeline.py --rerun-silver
```

**Run specific stages only:**
```bash
# Run only gold aggregation stages (use cached silver data)
python src/orchestrate_pipeline.py --stages 5 6

# Run bronze through gold stages, skip dashboard stage
python src/orchestrate_pipeline.py --stages 0 1 2 3 4 5

# Run with reprocessing of silver data
python src/orchestrate_pipeline.py --stages 3 4 5 6 --rerun-silver
```

### Individual Stage Execution

**Silver Data Enhancement (individual script):**
```bash
# Incremental mode (skip existing files)
python src/1_silver_data_enhance.py

# Reprocess all files
python src/1_silver_data_enhance.py --rerun
```

**Silver Report Data Creation (individual script):**
```bash
# Incremental mode (skip existing report periods)
python src/2_silver_report_data_create.py

# Reprocess all report periods
python src/2_silver_report_data_create.py --rerun
```

### Pipeline Stages

| Index | Stage | Module | Description |
|-------|-------|--------|-------------|
| 0 | Metadata Tracker (Initial) | `00_metadata_tracker` | Check baseline metadata and status tracking |
| 1 | Bronze API Data Pull | `0_bronze_api_data_pull` | Fetch raw crime data from Chicago Police API |
| 2 | Metadata Tracker (Verify) | `00_metadata_tracker` | Verify and update metadata after bronze pull |
| 3 | Silver Data Enhancement | `1_silver_data_enhance` | Clean and enrich raw data with 50+ features |
| 4 | Silver Report Data Creation | `2_silver_report_data_create` | Create R12 and YTD report period aggregations |
| 5 | Gold Aggregation | `3_gold_agg` | Aggregate to gold layer using ProcessPoolExecutor |
| 6 | Gold Dashboard Aggregation | `4_gold_dash_agg` | Final dashboard-layer aggregations |

### Performance Notes

- **Incremental Mode (Default)**: Recommended for routine runs. Skips already-processed files, significantly reducing execution time.
  - Typical execution: ~30 seconds per 1,000 files
  - Full pipeline on first run: ~4-5 minutes for 9,307 files
  
- **Rerun Mode** (`--rerun` or `--rerun-silver`): Use when you need to reprocess all data or apply code changes to existing files.

---

## Directory Structure
```
.
├── main.py                # Streamlit dashboard entry point
├── src/                   # ETL and data processing scripts
│   └── src.py             # Core ETL functions
├── data/                  # Data directory
│   ├── geojson/           # GeoJSON files for map visualization
│   ├── gold_data/         # Processed data (not committed)
│   ├── raw_data/          # Raw data files (not committed)
│   └── transformed_data/  # Intermediate transformed data (not committed)
├── .gitignore             # Ignore unnecessary files
├── requirements.txt       # Python dependencies
└── readme.md              # Project documentation
```

---

## Screenshots
### Dashboard Overview:
![Dashboard Overview](https://via.placeholder.com/800x400?text=Dashboard+Overview+Screenshot)

### Geographic Visualization:
![Geographic Visualization](https://via.placeholder.com/800x400?text=Geographic+Visualization+Screenshot)

### Crime Trends:
![Crime Trends](https://via.placeholder.com/800x400?text=Crime+Trends+Screenshot)

---
## Contributing to Chicago Crimes Dashboard
Thank you for considering contributing! Here's how you can help:
- Report bugs or suggest features via GitHub Issues.
- Submit pull requests with clear descriptions and test coverage.

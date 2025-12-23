# get start time
import time
start_time = time.time()
def ensure_requirements(requirements=None):
    """Ensure required packages are installed before imports."""
    import subprocess
    import sys
    if requirements is None:
        requirements = ['pygeohash', 'holidays', 'duckdb', 'pyarrow', 'polars']
    for package in requirements:
        try:
            __import__(package)
            print(f'{package} is already installed.')
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Call the function at the top of your script
#ensure_requirements()

# =========================
# IMPORTS
# =========================
import os
import polars as pl
import math
import holidays
import pygeohash as pgh
from datetime import datetime
from datetime import timedelta

# =========================
# CONFIGURATION
# =========================
root_data_dir = r'C:\Users\salik\Documents\PROJECTS\202512_streamlit-chicag\data'
DATA_DIR = os.path.join(root_data_dir, 'raw_data')
transformed_data_dir = os.path.join(root_data_dir, 'transformed_data')
REPORT_DATA_DIR = os.path.join(transformed_data_dir, 'report_data_new')
gold_data_dir = os.path.join(root_data_dir, 'gold_data')
# last 5 years crimes data
#RAW_CSV = os.path.join(DATA_DIR, 'Chicago_Crimes_Last_5_Years.csv')
# full crimes data as of Sept 27, 2025
RAW_CSV = os.path.join(DATA_DIR, 'Chicago_Crimes_20251223.csv')
SILVER_PARQUET = os.path.join(transformed_data_dir, 'chicago_crimes_silver.parquet')


os.makedirs(REPORT_DATA_DIR, exist_ok=True)
join_keys = ["report_type", "report_date", "start_date", "end_date"]

# =========================
# FEATURE ENGINEERING
# =========================
def add_features(df: pl.DataFrame) -> pl.DataFrame:
    # Defensive: Check required columns
    required_orginal = ['Latitude', 'Longitude', 'Beat', 'District', 'Ward',
                        'Community Area', 'Primary Type', 'FBI Code', 'ID', 'Date']
    missing = [col for col in required_orginal if col not in df.columns]
    if missing:
        #raise ValueError(f"Missing columns in input DataFrame: {missing}")
        pass
    # Rename columns for consistency
    rename_map = {
        'Latitude': 'latitude', 'Longitude': 'longitude',
        'Beat': 'beat', 'District': 'district', 'Ward': 'ward',
        'Community Area': 'community_area', 'Primary Type': 'primary_type',
        'FBI Code': 'fbi_code', 'ID': 'id'
    }
    df = df.rename(rename_map)

    # Parse datetime
    df = df.with_columns([
        pl.col('Date').str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p", strict=False)
    ])

    # Temporal features
    df = df.with_columns([
        pl.col('Date').dt.year().alias('Year'),
        pl.col('Date').dt.quarter().alias('Quarter'),
        pl.col('Date').dt.month().alias('Month'),
        pl.col('Date').dt.day().alias('Day'),
        pl.col('Date').dt.hour().alias('Hour'),
        pl.col('Date').dt.weekday().alias('day_of_week_num'),
        pl.col('Date').dt.week().alias('week_of_year'),
        (pl.col('Date').dt.weekday() >= 5).alias('Is_Weekend'),
        (pl.col('Date').dt.weekday() < 5).alias('Is_Weekday')
    ])
    df = df.with_columns([
        pl.col('Hour').is_between(6, 18).alias('Is_Daytime'),
        (~pl.col('Hour').is_between(6, 18)).alias('Is_Nighttime'),
        pl.col('Hour').is_between(0, 11).alias('Is_AM'),
        (~pl.col('Hour').is_between(0, 11)).alias('Is_PM'),
        pl.col('Hour').is_between(9, 17).alias('Is_Business_Hours'),
        (~pl.col('Hour').is_between(9, 17)).alias('Is_Off_Business_Hours'),
        (pl.col('Hour').is_between(8, 15) & pl.col('Is_Weekday')).alias('Is_School_Hours'),
        pl.col('Hour').is_between(0, 5).alias('Is_Late_Night'),
        pl.when(pl.col('Hour') < 6).then(pl.lit('Night'))
          .when(pl.col('Hour') < 12).then(pl.lit('Morning'))
          .when(pl.col('Hour') < 18).then(pl.lit('Afternoon'))
          .otherwise(pl.lit('Evening')).alias('part_of_day')
    ])

    # Seasonal & holiday
    df = df.with_columns([
        pl.when(pl.col('Month').is_in([12, 1, 2])).then(pl.lit('Winter'))
        .when(pl.col('Month').is_in([3, 4, 5])).then(pl.lit('Spring'))
        .when(pl.col('Month').is_in([6, 7, 8])).then(pl.lit('Summer'))
        .when(pl.col('Month').is_in([9, 10, 11])).then(pl.lit('Fall'))
        .otherwise(pl.lit('Unknown'))
        .alias('Season')
    ])

    years = [int(y) for y in df['Year'].unique().to_list() if y is not None]
    us_holidays = holidays.US(years=years)
    holiday_dates = set([d for d in us_holidays])
    df = df.with_columns([
        pl.col('Date').dt.date().is_in(holiday_dates).alias('is_holiday')
    ])
    
    # Spatial
    df = df.with_columns([
        pl.col('latitude').round(2).alias('lat_bin'),
        pl.col('longitude').round(2).alias('lon_bin'),
        (pl.col('latitude').round(2).cast(pl.Utf8) + '_' + pl.col('longitude').round(2).cast(pl.Utf8)).alias('geo_grid'),
        (((111 * (pl.col('latitude') - 41.8781)) ** 2 + (85 * (pl.col('longitude') + 87.6298)) ** 2) ** 0.5).alias('distance_from_downtown_km')
    ])

    # Crime density
    df = df.with_columns([
        pl.col('id').count().over(['lat_bin', 'lon_bin']).alias('crime_density_bin')
    ])


    print("Finished data frame 1")
    # Crime type flags
    violent_types = ['BATTERY', 'ASSAULT', 'HOMICIDE', 'ROBBERY', 'CRIM SEXUAL ASSAULT']
    property_types = ['BURGLARY', 'THEFT', 'MOTOR VEHICLE THEFT', 'ARSON']
    drug_types = ['NARCOTICS', 'OTHER NARCOTIC VIOLATION']
    public_order_types = ['PUBLIC PEACE VIOLATION', 'INTERFERENCE WITH PUBLIC OFFICER']
    weapon_types = ['WEAPONS VIOLATION', 'UNLAWFUL USE OF WEAPON', 'CONCEALED CARRY LICENSE VIOLATION']

    df = df.with_columns([
        pl.col('primary_type').is_in(violent_types).alias('is_violent'),
        pl.col('primary_type').is_in(property_types).alias('is_property'),
        pl.col('primary_type').is_in(drug_types).alias('is_drug_related'),
        pl.col('primary_type').is_in(public_order_types).alias('is_public_order'),
        pl.col('primary_type').is_in(weapon_types).alias('is_weapon_related')
    ])

    # FBI code mapping
    fbi_map = {
        '01A': 'Homicide', '01B': 'Manslaughter', '02': 'Sexual Assault', '03': 'Robbery',
        '04A': 'Aggravated Assault/Battery', '04B': 'Simple Assault/Battery', '05': 'Burglary',
        '06': 'Theft', '07': 'Motor Vehicle Theft', '08A': 'Arson', '08B': 'Criminal Damage',
        '09': 'Fraud', '10': 'Forgery/Counterfeiting', '11': 'Embezzlement', '12': 'Stolen Property',
        '13': 'Vandalism', '14': 'Weapons Violation', '15': 'Prostitution', '16': 'Sex Offense (Other)',
        '17': 'Drug Violation', '18': 'Gambling', '19': 'Offense Against Family/Children',
        '20': 'Driving Offenses', '21': 'Liquor Law Violation', '22': 'Public Order Crime',
        '24': 'Disorderly Conduct', '26': 'Miscellaneous Offense'
    }
    df = df.with_columns([
        pl.col('fbi_code').map_dict(fbi_map, default='Unknown').alias('fbi_category')
    ])
    severity_map = {
        'Homicide': 5, 'Manslaughter': 4, 'Sexual Assault': 4, 'Robbery': 4,
        'Aggravated Assault/Battery': 4, 'Arson': 4, 'Burglary': 3, 'Motor Vehicle Theft': 3,
        'Weapons Violation': 3, 'Drug Violation': 2, 'Fraud': 2, 'Forgery/Counterfeiting': 2,
        'Disorderly Conduct': 1, 'Vandalism': 1, 'Public Order Crime': 1, 'Miscellaneous Offense': 1
    }
    df = df.with_columns([
        pl.col('fbi_category').map_dict(severity_map, default=0).cast(pl.Int32).alias('crime_severity_level')
    ])
    severity_labels = {5: 'Critical', 4: 'High', 3: 'Moderate', 2: 'Low', 1: 'Minor', 0: 'Unknown'}
    df = df.with_columns([
        pl.col('crime_severity_level').map_dict(severity_labels, default='Unknown').alias('crime_severity_label')
    ])

    # Derived flags
    df = df.with_columns([
        pl.col('fbi_code').is_in(['01A', '01B', '02', '03', '04A', '04B']).alias('is_violent_fbi'),
        pl.col('fbi_code').is_in(['05', '06', '07', '08A', '08B', '09', '10', '11', '12', '13']).alias('is_property_fbi')
    ])
    # Combined flags
    df = df.with_columns([
        (pl.col('is_violent') | pl.col('is_violent_fbi')).alias('is_violent_combined'),
        (pl.col('is_property') | pl.col('is_property_fbi')).alias('is_property_combined')
    ])
    # Risk score
    df = df.with_columns([
        (pl.col('is_violent_combined').cast(pl.Int32) * 3 +
         pl.col('is_property_combined').cast(pl.Int32) * 2 +
         pl.col('is_drug_related').cast(pl.Int32)).alias('crime_risk_score'),
        ((pl.col('is_violent_combined') & pl.col('Is_Nighttime')) |
         (pl.col('is_weapon_related') & pl.col('Is_Weekend'))).alias('high_risk_situation')
    ])

    # Crime category
    df = df.with_columns([
        pl.when(pl.col('is_violent_combined')).then(pl.lit('Violent Crime'))
          .when(pl.col('is_property_combined')).then(pl.lit('Property Crime'))
          .when(pl.col('is_drug_related')).then(pl.lit('Drug Crime'))
          .when(pl.col('is_weapon_related')).then(pl.lit('Weapons Crime'))
          .when(pl.col('is_public_order')).then(pl.lit('Public Order Crime'))
          .otherwise(pl.lit('Other')).alias('crime_category')
    ])

    # Additional flags for analysis
    df = df.with_columns([
        pl.col('lat_bin').cast(pl.Utf8).alias('text_lat_bin'),
        pl.col('lon_bin').cast(pl.Utf8).alias('text_lon_bin')
    ])

    return df
# =========================
# SILVER PARQUET CREATION
def create_silver_parquet(RAW_CSV: str, out_path: str):
    """Save DataFrame to Parquet after feature engineering."""
    if RAW_CSV.endswith('.parquet'):
        df = pl.read_parquet(RAW_CSV)
    else:
        df = pl.read_csv(RAW_CSV)
    df = add_features(df)
    df.write_parquet(out_path)
    return df

# =========================
# REPORT PERIODS GENERATION (Polars version)
# =========================
def generate_report_periods(min_date, max_date):
    """Generate report periods using Python datetime."""
    months = []
    current = min_date
    while current <= max_date:
        months.append(current)
        year = current.year
        month = current.month
        if month == 12:
            current = datetime(year + 1, 1, 1)
        else:
            current = datetime(year, month + 1, 1)
    records = []
    for report_date in months:
        year = report_date.year
        if year < 2:  # skip invalid prior years
            continue
        ytd_start = datetime(year, 1, 1)
        prior_ytd_start = datetime(year - 1, 1, 1)
        if report_date.month != 12:
            prior_ytd_end = datetime(year - 1, report_date.month + 1, 1) - timedelta(days=1)
        else:
            prior_ytd_end = datetime(year, 1, 1) - timedelta(days=1)
        r12_end = report_date
        r12_start = r12_end - timedelta(days=365)
        prior_r12_end = r12_start - timedelta(days=1)
        prior_r12_start = prior_r12_end - timedelta(days=365)
        records.extend([
            {"report_type": "R12", "report_date": report_date, "report_date_yyyymm": int(f"{report_date.year}{str(report_date.month).zfill(2)}"),
             "start_date": r12_start, "end_date": r12_end},
            {"report_type": "YTD", "report_date": report_date, "report_date_yyyymm": int(f"{report_date.year}{str(report_date.month).zfill(2)}"),
             "start_date": ytd_start, "end_date": report_date},
            {"report_type": "Prior R12", "report_date": report_date, "report_date_yyyymm": int(f"{report_date.year}{str(report_date.month).zfill(2)}"),
             "start_date": prior_r12_start, "end_date": prior_r12_end},
            {"report_type": "Prior YTD", "report_date": report_date, "report_date_yyyymm": int(f"{report_date.year}{str(report_date.month).zfill(2)}"),
             "start_date": prior_ytd_start, "end_date": prior_ytd_end},
        ])
    return pl.DataFrame(records)

def create_silver_df():
    """Main ETL process to create silver parquet and generate reports (Polars only)."""
    silver_df = create_silver_parquet(RAW_CSV, SILVER_PARQUET)
    min_date = silver_df.select(pl.col("Date").min()).to_dicts()[0]["Date"]
    max_date = silver_df.select(pl.col("Date").max()).to_dicts()[0]["Date"]

    df_report_dates = generate_report_periods(min_date, max_date)
    df_report_dates = df_report_dates.sort('report_date_yyyymm', descending=True)
    for row in df_report_dates.iter_rows(named=True):
        report_type = row['report_type']
        report_date = row['report_date'].strftime('%Y-%m-%d')
        report_start_date = row['start_date'].date()
        report_end_date = row['end_date'].date()
        filtered = (
            silver_df
            .filter(
                    (pl.col("Date") >= pl.lit(report_start_date)) &
                    (pl.col("Date") <= pl.lit(report_end_date))
            )
            .with_columns([
                pl.lit(report_type).alias("report_type"),
                pl.lit(report_date).alias("report_date"),
                pl.lit(report_start_date).cast(pl.Date).alias("start_date"),
                pl.lit(report_end_date).cast(pl.Date).alias("end_date"),
            ])
        )
        if filtered.height > 0:
            report_date = row['report_date'].strftime('%Y%m')
            file_name = f"{report_type}_{report_date}__Chicago_Crimes_Report_Data.parquet"
            filtered.write_parquet(os.path.join(REPORT_DATA_DIR, file_name))
        else:
            print(f"⚠️ No data for {report_type} {report_date}")

def isnan(val):
    """Check if a value is NaN."""
    try:
        return math.isnan(float(val))
    except:
        return False

def cast_columns_to_utf8(df, columns):
    """Cast specified columns to Utf8 (string) type, only if they exist."""
    for col in columns:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).cast(pl.Utf8, strict=False).alias(col)
            ])
    return df
def make_pivot(
        df : pl.DataFrame
        , group_col: str
        , prefix: str
        , join_keys: list
        , int_suffix: bool = False
        , lower_and_underscore: bool = False
    ) -> pl.DataFrame:
    """
    Create a pivot table with prefixed columns.
    Logic: 
    1. Get unique values from group_col
    2. Create rename_dict and select_cols based on parameters
    3. Group by join_keys + group_col, count cases
    4. Pivot the table
    5. Rename columns and select final columns
    """
    # group_col: the column to pivot (e.g. "ward")
    # prefix: prefix for new columns (e.g. "ward_")
    # int_suffix: if True, suffix is int(float(col)), else just col
    # lower_and_underscore: if True, col.lower().replace(' ', '_')
    unique_vals = df[group_col].unique().to_list()
    unique_vals = [col for col in unique_vals if col is not None and not isnan(col)]
    if lower_and_underscore:
        rename_dict = {str(col): f"{prefix}{str(col).lower().replace(' ', '_')}" for col in unique_vals}
        select_cols = join_keys + [f"{prefix}{str(col).lower().replace(' ', '_')}" for col in unique_vals]
    elif int_suffix:
        rename_dict = {str(col): f"{prefix}{int(float(col))}" for col in unique_vals}
        select_cols = join_keys + [f"{prefix}{int(float(col))}" for col in unique_vals]
    else:
        rename_dict = {str(col): f"{prefix}{str(col)}" for col in unique_vals}
        select_cols = join_keys + [f"{prefix}{str(col)}" for col in unique_vals]
    return (
        df.groupby(join_keys + [group_col])
          .agg(pl.count("Case Number").alias("case_count"))
          .pivot(
              values="case_count",
              index=join_keys,
              columns=group_col,
              aggregate_function="sum"
          )
          .rename(rename_dict)
          .select(select_cols)
    )

def make_summary_agg(
        df: pl.DataFrame
        , join_keys: list             
        ) -> pl.DataFrame:
    """Aggregate summary statistics."""
    return (
        df.groupby(join_keys)
          .agg([
              pl.col("id").n_unique().alias("total_cases"),
              pl.col("primary_type").n_unique().alias("unique_crime_types"),
              pl.col("Arrest").sum().alias("total_arrests"),
              pl.col("Domestic").sum().alias("total_domestic_cases"),
              pl.col("Is_Weekend").sum().alias("total_weekend_cases"),
              pl.col("Is_Nighttime").sum().alias("total_nighttime_cases"),
              pl.col("Is_Daytime").sum().alias("total_daytime_cases"),
              pl.col("is_violent").sum().alias("total_violent_cases"),
              pl.col("is_property").sum().alias("total_property_cases"),
              pl.col("is_drug_related").sum().alias("total_drug_cases"),
              pl.col("is_public_order").sum().alias("total_public_order_cases"),
              pl.col("is_weapon_related").sum().alias("total_weapon_cases"),
              pl.col("high_risk_situation").sum().alias("total_high_risk_cases"),
              pl.col("crime_risk_score").mean().alias("avg_crime_risk_score"),
              pl.col("crime_risk_score").max().alias("max_crime_risk_score"),
              pl.col("crime_severity_level").mean().alias("avg_severity_level"),
              pl.col("distance_from_downtown_km").mean().alias("avg_distance_from_downtown_km"),
              pl.col("beat").n_unique().alias("unique_beats"),
              pl.col("ward").n_unique().alias("unique_wards"),
              pl.col("district").n_unique().alias("unique_districts"),
              pl.col("community_area").n_unique().alias("unique_community_areas"),
          ])
    )

def create_aggregated_dataframe(df : pl.DataFrame
                                , join_keys: list
                                ) -> pl.DataFrame:
    """Create the final aggregated DataFrame with all pivots and summary."""
    # Create pivots
    crime_type_pivot = make_pivot(df, "primary_type", "crime_", join_keys, lower_and_underscore=True)
    district_pivot = make_pivot(df, "district", "district_", join_keys)
    ward_pivot = make_pivot(df, "ward", "ward_", join_keys, int_suffix=True)
    community_area_pivot = make_pivot(df, "community_area", "community_area_", join_keys, int_suffix=True)
    beat_pivot = make_pivot(df, "beat", "beat_", join_keys, int_suffix=True)
    fbi_code_pivot = make_pivot(df, "fbi_code", "fbi_", join_keys)

    agg_df = make_summary_agg(df, join_keys)

    final_df = (
        agg_df
        .join(crime_type_pivot, on=join_keys, how="left")
        .join(district_pivot, on=join_keys, how="left")
        .join(ward_pivot, on=join_keys, how="left")
        .join(community_area_pivot, on=join_keys, how="left")
        .join(fbi_code_pivot, on=join_keys, how="left")
        .join(beat_pivot, on=join_keys, how="left")
        .fill_null(0)
    )
    return final_df

def get_all_report_dates(df: pl.DataFrame) -> list:
    """Fetch all unique report dates from the silver table."""
    # extract unique report dates from duckdb ordered descending
    dates = df['report_date'].unique().to_list()
    dates = sorted(dates, reverse=True)
    
    return dates

def aggregate_for_report_date(report_date: str, df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate for a single report date."""
    # Load data for the specific report date
    df = df.filter(pl.col("report_date") == pl.lit(report_date))
    if df.height == 0:
        print(f"⚠️ No data for report date {report_date}")
        return None
    df = cast_columns_to_utf8(df, ["district", "ward", "community_area", "fbi_code"])
    return create_aggregated_dataframe(df, join_keys)

def format_report_date(report_date):
    try:
        dt = datetime.strptime(str(report_date), "%Y_%m")
        return f"{dt.year}{str(dt.month).zfill(2)}"
    except Exception:
        return str(report_date).replace(":", "-").replace(" ", "_")

def _harmonize_and_concat(dfs: list) -> pl.DataFrame | None:
    """Make all DataFrames share the same columns and dtypes, then concat."""
    if not dfs:
        return None
    # superset of columns in insertion order
    all_cols = []
    for d in dfs:
        for c in d.columns:
            if c not in all_cols:
                all_cols.append(c)
    # pick target dtype for each column from first DF that contains it
    targets = {}
    for c in all_cols:
        for d in dfs:
            if c in d.columns:
                targets[c] = d.schema[c]
                break
    normalized = []
    for d in dfs:
        # add missing cols as nulls with target dtype
        missing = [c for c in all_cols if c not in d.columns]
        for c in missing:
            d = d.with_columns(pl.lit(None).cast(targets[c]).alias(c))
        # cast existing cols to target dtype when needed
        casts = []
        for c in all_cols:
            if d.schema.get(c) != targets[c]:
                casts.append(pl.col(c).cast(targets[c]).alias(c))
        if casts:
            d = d.with_columns(casts)
        # enforce column order
        d = d.select(all_cols)
        normalized.append(d)
    return pl.concat(normalized, how='vertical')


def merge_prior_and_current(df: pl.DataFrame) -> pl.DataFrame:
    """Merge prior period data into current period data."""
    # separate prior and current 
    df_prior = df.filter(pl.col("report_type").str.starts_with("Prior "))
    df_current = df.filter(~pl.col("report_type").str.starts_with("Prior "))

    # remove "Prior " prefix from report_type in df_prior
    df_prior = df_prior.with_columns(
        pl.col("report_type").str.replace("Prior ", "").alias("report_type")
    )   
    # prefix all columns in df_prior except join keys
    join_keys = ["report_date", "report_type"]
    df_prior = df_prior.select(
        [pl.col(col).alias(f"prior_{col}") if col not in join_keys else pl.col(col) for col in df_prior.columns]
    )

    # merge on join keys 
    merged_df = df_current.join(
        df_prior,
        on=join_keys,
        how="left"
    )

    return merged_df

def aggregate_all_report_dates():
    """Aggregate for all report dates and concatenate results using individual report parquet files."""
    gold_data_sub_dir = os.path.join(gold_data_dir, 'gold_parquet_reports')
    # Ensure silver DataFrame is created
    #create_silver_df()
    
    # create gold data directory if not exists
    os.makedirs(gold_data_sub_dir, exist_ok=True)
    # Find all parquet files in REPORT_DATA_DIR
    parquet_files = [os.path.join(REPORT_DATA_DIR, f) for f in os.listdir(REPORT_DATA_DIR) if f.endswith('.parquet')]
    # Sort files for consistent processing
    parquet_files.sort()

    # Check if any parquet files found
    if not parquet_files:
        print("❌ No parquet files found in report data directory.")
        return None

    # Process each parquet file individually
    for file_path in parquet_files:
        # Load the parquet file
        df = pl.read_parquet(file_path)
        # Extract report_type and report_date from file name
        file_path_split = os.path.basename(file_path).split('__')[0]
        report_type, report_date_str = file_path_split.split('_', 1)
        if df.height == 0:
            print(f"⚠️ No data in {file_path}")
            continue
        # Aggregate and save to gold parquet
        agg = create_aggregated_dataframe(df, join_keys)
        out_path = os.path.join(gold_data_sub_dir, f"Crimes_gold_{report_type}_{report_date_str}.parquet")
        agg.write_parquet(out_path)
    
    # Combine all aggregated data into a single DataFrame
    all_agg_dfs = []
    for file_name in os.listdir(gold_data_sub_dir):
        if file_name.endswith('.parquet'):
            df = pl.read_parquet(os.path.join(gold_data_sub_dir, file_name))
            all_agg_dfs.append(df)
    if all_agg_dfs:
        #combined_df = pl.concat(all_agg_dfs, how='vertical')
        combined_df = _harmonize_and_concat(all_agg_dfs)
        if combined_df is None:
            print("❌ No aggregated DataFrames to combine.")
            return None
        combined_out_path = os.path.join(gold_data_dir, 'chicago_crimes_gold_reports.parquet')
        combined_df.write_parquet(combined_out_path)
        print(f"✅ Combined aggregated data saved to {combined_out_path}, total records: {combined_df.height}")
    
    else:
        print("❌ No aggregated DataFrames found.")
        return None
    if combined_df is None:
        return None
    else:
        combined_df = merge_prior_and_current(combined_df)
        # Return combined DataFrame
        return combined_df
def main():
    """Main function to run the aggregation and save to Parquet."""
    # Run the aggregation and save to Parquet
    aggregate_all_report_dates()

if __name__ == "__main__":
    main()
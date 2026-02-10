# Module: silver_data_enhance
# Purpose: transform raw daily crime extracts into an enriched "silver" parquet with
# temporal, spatial, categorical and risk features for downstream analysis/visualization.

import os
from datetime import datetime, timedelta
import holidays
import polars as pl
import zipfile
import glob
import shutil
from pyproj import Transformer


# =========================
# DATA CLEANING AND NORMALIZATION
# =========================
def clean_raw_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize and validate raw geographic/id fields coming from the API/raw extracts.

    Rules:
    - beat, district, community_area: canonicalized to Utf8 (text). Remove punctuation/extra characters.
      * district and community_area will keep digits only but remain text to preserve upstream formatting.
    - ward: extract digits and cast to Int32. If extraction/cast fails, ward will become null.
    - For each field processed a corresponding "<field>_invalid" boolean flag is added:
      True => field is missing, unparsable or outside expected range.
    - Function is defensive: operates only on columns present in the DataFrame.
    - Preserve original raw columns by adding "<field>_raw" when possible so nothing is lost.
    """
    exprs = []

    # Beat: preserve raw then add cleaned column
    if 'beat' in df.columns:
        if 'beat_raw' not in df.columns:
            exprs.append(pl.col('beat').alias('beat_raw'))
        beat_clean = (
            pl.col('beat')
              .cast(pl.Utf8, strict=False)  # ensure text for string ops
              .str.replace_all(r'[^0-9A-Za-z]', '')   # drop punctuation and spaces
              .str.to_lowercase()                      # canonical lowercase
              .cast(pl.Int64, strict=False) # allow nulls
              .cast(pl.Utf8, strict=False)  # ensure text for string ops
              .str.zfill(4)                              # pad to 4 characters


        )
        exprs.append(beat_clean.alias('beat_clean'))
    
    # District: preserve raw then cleaned (digits only as text)
    if 'district' in df.columns:
        if 'district_raw' not in df.columns:
            exprs.append(pl.col('district').alias('district_raw'))
        district_clean = (
            pl.col('district')
              .cast(pl.Utf8, strict=False)
              .str.replace_all(r'[^0-9]', '')   # retain digits only
              .cast(pl.Int64, strict=False)
              .cast(pl.Utf8, strict=False)
              .str.zfill(3)
        )
        exprs.append(district_clean.alias('district'))
    # Community Area: preserve raw then cleaned (digits only as text)
    if 'community_area' in df.columns:
        if 'community_area_raw' not in df.columns:
            exprs.append(pl.col('community_area').alias('community_area_raw'))
        ca_clean = (
            pl.col('community_area')
              .cast(pl.Utf8, strict=False)
              .str.replace_all(r'[^0-9]', '')   # digits only, keep as text for downstream joins/lookups
              .cast(pl.Int64, strict=False)
              .cast(pl.Utf8, strict=False)
              .str.zfill(3)
        )
        exprs.append(ca_clean.alias('community_area'))
    # Ward: preserve raw then numeric extraction; flag invalids
    if 'ward' in df.columns:
        if 'ward_raw' not in df.columns:
            exprs.append(pl.col('ward').alias('ward_raw'))
        # extract digits as text first, then try cast to Int32 (nulls preserved)
        ca_clean = (
            pl.col('ward')
            .cast(pl.Utf8, strict=False)
            .str.extract(r'(\d+)', 1)
            .cast(pl.Int32, strict=False)
            .cast(pl.Utf8, strict=False)
            .str.zfill(3)
            )
        
        exprs.append(ca_clean.alias('ward'))  # replace/ensure numeric ward where possible
    # Primary Type: preserve raw then cleaned (canonical lowercase with underscores)
    if 'primary_type' in df.columns:
        if 'primary_type_raw' not in df.columns:
            exprs.append(pl.col('primary_type').alias('primary_type_raw'))
        type_clean = (
            pl.col('primary_type')
            .cast(pl.Utf8, strict=False) # ensure text for string ops
            .str.strip_chars() # remove leading/trailing whitespace
            .str.replace_all(r"[^0-9A-Za-z\s]", "")  # remove punctuation and special characters
            .str.replace_all(r"\s+", "_")  # replace spaces with underscores
            .str.to_lowercase() # canonical lowercase
        )
        exprs.append(type_clean.alias('primary_type'))
    
    # IUCR: preserve raw then cleaned (remove special characters, ensure 4 characters)
    if 'iucr' in df.columns:
        if 'iucr_raw' not in df.columns:
            exprs.append(pl.col('iucr').alias('iucr_raw'))
        iucr_clean = (
            pl.col('iucr')
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(r"[^0-9A-Za-z]", "")  # Remove special characters
            .str.zfill(4)  # Ensure 4 characters
        )
        exprs.append(iucr_clean.alias('iucr'))

    # Description: preserve raw then cleaned (remove special characters, lowercase)
    if 'description' in df.columns:
        if 'description_raw' not in df.columns:
            exprs.append(pl.col('description').alias('description_raw'))
        description_clean = (
            pl.col('description')
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(r"[^0-9A-Za-z\s]", "")  # Remove special characters
            .str.to_lowercase()  # Or .str.to_titlecase() for title case
        )
        exprs.append(description_clean.alias('description'))
    # FBI Code: preserve raw then cleaned (remove special characters, uppercase)
    if 'fbi_code' in df.columns:
        if 'fbi_code_raw' not in df.columns:
            exprs.append(pl.col('fbi_code').alias('fbi_code_raw'))
        fbi_code_clean = (
            pl.col('fbi_code')
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(r"[^0-9A-Za-z]", "")  # Remove special characters
            .str.to_uppercase()
        )
        exprs.append(fbi_code_clean.alias('fbi_code'))
    
    # Case Number: preserve raw then cleaned (remove special characters)
    if 'case_number' in df.columns:
        if 'case_number_raw' not in df.columns:
            exprs.append(pl.col('case_number').alias('case_number_raw'))
        case_number_clean = (
            pl.col('case_number')
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(r"[^0-9A-Za-z]", "")  # Remove special characters
        )
        exprs.append(case_number_clean.alias('case_number'))

    # Block: preserve raw then cleaned (remove special characters, uppercase)
    if 'block' in df.columns:
        if 'block_raw' not in df.columns:
            exprs.append(pl.col('block').alias('block_raw'))
        block_clean = (
            pl.col('block')
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(r"XX", "00")  # Replace "XX" with "00" for better geocoding
            .str.replace_all(r"\bST\b", "STREET")  # Standardize street abbreviations
            .str.replace_all(r"\bAVE\b", "AVENUE")
            .str.replace_all(r"\bBLVD\b", "BOULEVARD")
            .str.replace_all(r"\bDR\b", "DRIVE")
            .str.replace_all(r"\bLN\b", "LANE")
            .str.replace_all(r"\bCT\b", "COURT")
            .str.replace_all(r"\bPL\b", "PLACE")
            .str.replace_all(r"\sN\s\b", "NORTH")
            .str.replace_all(r"\sS\s\b", "SOUTH")
            .str.replace_all(r"\sE\s\b", "EAST")
            .str.replace_all(r"\sW\s\b", "WEST")
            .str.to_uppercase()
        )
        exprs.append(block_clean.alias('block'))

    # Location Description: preserve raw then cleaned (remove special characters, lowercase)
    if 'location_description' in df.columns:
        if 'location_description_raw' not in df.columns:
            exprs.append(pl.col('location_description').alias('location_description_raw'))
        location_clean = (
            pl.col('location_description')
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(r"\bAPT\b", "APARTMENT")
            .str.replace_all(r"\\RESIDENCE\\b", "HOME")  # Fixed regex
            .str.to_lowercase()  # Or .str.to_titlecase() for title case
        )
        exprs.append(location_clean.alias('location_description'))

    # Apply all transformations/flags in a single with_columns call for efficiency.
    if exprs:
        df = df.with_columns(exprs)

    return df

# =========================
# FEATURE ENGINEERING
# =========================
def add_features(df: pl.DataFrame) -> pl.DataFrame:
    """
        Add temporal, spatial, crime type, and risk features to the DataFrame.
    """
    # normalize column names to lowercase to avoid case issues from different sources
    if any(c != c.lower() for c in df.columns):
        df = df.rename({c: c.lower() for c in df.columns})

    # api columns
        # 'id', 'case_number', 'date', 'block', 'iucr', 'primary_type'
        # , 'description', 'location_description', 'arrest', 'domestic', 'beat', 'district', 'ward'
        # , 'community_area', 'fbi_code', 'year', 'updated_on', 'x_coordinate', 'y_coordinate'
        # , 'latitude', 'longitude', 'location', '__index_level_0__']
    
    # Defensive: Check required columns
    required_orginal = ['latitude', 'longitude', 'beat', 'district', 'ward',
                        'community_area', 'primary_type', 'fbi_code', 'id', 'date']
    lower_required = [col.lower() for col in required_orginal]
    missing = [col for col in lower_required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in input DataFrame: {missing}")

    # Clean and standardize raw geographic/id fields:
        # - Ensure beat, district, community_area come in as text (Utf8) 
        #   with punctuation/extra whitespace removed.
        # - Ensure ward is numeric (Int) where possible (extract digits); 
        #   invalid or unparsable wards become null.
        # - Add boolean validation flags (e.g., ward_invalid) 
        #   to help detect problematic rows downstream.
    df = clean_raw_fields(df)
    # Parse 'date' column in format  2001-01-01T00:00:00.000 to datetime
    df = df.with_columns([
        pl.col('date').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False)
    ])

    # Defensive casts: ensure numeric/text types for arithmetic and mappings
    cast_ops = []
    if 'latitude' in df.columns:
        cast_ops.append(pl.col('latitude').cast(pl.Float64))
    if 'longitude' in df.columns:
        cast_ops.append(pl.col('longitude').cast(pl.Float64))
    # numeric ids/counts
    if 'id' in df.columns:
        cast_ops.append(pl.col('id').cast(pl.Int64))
    # ensure categorical/text columns are Utf8 for map/lookups
    if 'fbi_code' in df.columns:
        cast_ops.append(pl.col('fbi_code').cast(pl.Utf8))
    if 'primary_type' in df.columns:
        cast_ops.append(pl.col('primary_type').cast(pl.Utf8))
    if 'arrest' in df.columns:
        cast_ops.append(pl.col('arrest').cast(pl.Utf8))
    if 'domestic' in df.columns:
        cast_ops.append(pl.col('domestic').cast(pl.Utf8))

    if cast_ops:
        df = df.with_columns(cast_ops)
    
    # validate numeric ranges and flag problems
    df = df.with_columns([
        (~pl.col('ward').cast(pl.Int64).is_between(1, 50)).alias('ward_out_of_range') if 'ward' in df.columns else pl.lit(False).alias('ward_out_of_range'),
        (~pl.col('community_area').cast(pl.Int64).is_between(1, 100)).alias('community_area_out_of_range') if 'community_area' in df.columns else pl.lit(False).alias('community_area_out_of_range')
    ])

    # normalize boolean-like columns
    if 'arrest' in df.columns:
        df = df.with_columns([
            pl.when(pl.col('arrest').str.to_lowercase().is_in(['true','t','1','yes','y'])).then(True).otherwise(False).alias('arrest')
        ])
    if 'domestic' in df.columns:
        df = df.with_columns([
            pl.when(pl.col('domestic').str.to_lowercase().is_in(['true','t','1','yes','y'])).then(True).otherwise(False).alias('domestic')
        ])

    # normalize categorical columns to canonical form
    if 'primary_type' in df.columns:
        df = df.with_columns([
            pl.col('primary_type')
              .str.to_uppercase()
              .str.replace_all(r'^\s+|\s+$', '')
              .alias('primary_type')
        ])
    if 'fbi_code' in df.columns:
        df = df.with_columns([
            pl.col('fbi_code')
              .str.to_uppercase()
              .str.replace_all(r'^\s+|\s+$', '')
              .alias('fbi_code')
        ])

    # Temporal features
    df = df.with_columns([
        pl.col('date').dt.year().alias('year'),
        pl.col('date').dt.quarter().alias('quarter'),
        pl.col('date').dt.month().alias('month'),
        pl.col('date').dt.day().alias('day'),
        pl.col('date').dt.hour().alias('hour'),
        pl.col('date').dt.weekday().alias('day_of_week_num'),
        pl.col('date').dt.week().alias('week_of_year'),
        (pl.col('date').dt.weekday() >= 5).alias('is_weekend'),
        (pl.col('date').dt.weekday() < 5).alias('is_weekday')
    ])
    df = df.with_columns([
        pl.col('hour').is_between(6, 18).alias('is_daytime'),
        (~pl.col('hour').is_between(6, 18)).alias('is_nighttime'),
        pl.col('hour').is_between(0, 11).alias('is_am'),
        (~pl.col('hour').is_between(0, 11)).alias('is_pm'),
        pl.col('hour').is_between(9, 17).alias('is_business_hours'),
        (~pl.col('hour').is_between(9, 17)).alias('is_off_business_hours'),
        (pl.col('hour').is_between(8, 15) & pl.col('is_weekday')).alias('is_school_hours'),
        pl.col('hour').is_between(0, 5).alias('is_late_night'),
        pl.when(pl.col('hour') < 6).then(pl.lit('Night'))
          .when(pl.col('hour') < 12).then(pl.lit('Morning'))
          .when(pl.col('hour') < 18).then(pl.lit('Afternoon'))
          .otherwise(pl.lit('Evening')).alias('part_of_day')
    ])

    # Seasonal & holiday
    df = df.with_columns([
        pl.when(pl.col('month').is_in([12, 1, 2])).then(pl.lit('Winter'))
        .when(pl.col('month').is_in([3, 4, 5])).then(pl.lit('Spring'))
        .when(pl.col('month').is_in([6, 7, 8])).then(pl.lit('Summer'))
        .when(pl.col('month').is_in([9, 10, 11])).then(pl.lit('Fall'))
        .otherwise(pl.lit('Unknown'))
        .alias('season')
    ])

    years = [int(y) for y in df['year'].unique().to_list() if y is not None]
    us_holidays = holidays.US(years=years)
    holiday_dates = set([d for d in us_holidays])
    df = df.with_columns([
        pl.col('date').dt.date().is_in(holiday_dates).alias('is_holiday')
    ])
    
    # Spatial
    df = df.with_columns([
        pl.col('latitude').is_null().alias('is_missing_lat'),
        pl.col('longitude').is_null().alias('is_missing_lon'),
        (~pl.col('latitude').is_between(-90.0, 90.0)).alias('lat_out_of_range'),
        (~pl.col('longitude').is_between(-180.0, 180.0)).alias('lon_out_of_range')
    ])
    # mark any bad coord rows
    df = df.with_columns([
        (pl.col('is_missing_lat') | pl.col('is_missing_lon') | pl.col('lat_out_of_range') | pl.col('lon_out_of_range')).alias('is_bad_location')
    ])
    # Zero coordinates often indicate a sentinel/invalid location (not real lat/lon).
    # Replace exact (0,0) with null so downstream imputation/grouping won't treat them as a valid cluster.
    df = df.with_columns([
        pl.when((pl.col('latitude') == 0) & (pl.col('longitude') == 0)).then(None).otherwise(pl.col('latitude')).alias('latitude'),
        pl.when((pl.col('latitude') == 0) & (pl.col('longitude') == 0)).then(None).otherwise(pl.col('longitude')).alias('longitude')
    ])

    df = df.with_columns([
        pl.col('latitude').round(4).alias('lat_bin'),
        pl.col('longitude').round(4).alias('lon_bin'),
        (pl.col('latitude').round(4).cast(pl.Utf8) + '_' + pl.col('longitude').round(4).cast(pl.Utf8)).alias('geo_grid'),
        (((111 * (pl.col('latitude') - 41.8781)) ** 2 + (85 * (pl.col('longitude') + 87.6298)) ** 2) ** 0.5).alias('distance_from_downtown_km')
    ])

    # Crime density
    df = df.with_columns([
        pl.col('id').count().over(['lat_bin', 'lon_bin']).alias('crime_density_bin')
    ])

    # Crime type flags
    violent_types = ['BATTERY', 'ASSAULT', 'HOMICIDE', 'ROBBERY', 'CRIM SEXUAL ASSAULT']
    property_types = ['BURGLARY', 'THEFT', 'MOTOR VEHICLE THEFT', 'ARSON']
    drug_types = ['NARCOTICS', 'OTHER NARCOTIC VIOLATION']
    public_order_types = ['PUBLIC PEACE VIOLATION', 'INTERFERENCE WITH PUBLIC OFFICER']
    weapon_types = ['WEAPONS VIOLATION', 'UNLAWFUL USE OF WEAPON', 'CONCEALED CARRY LICENSE VIOLATION']

    # Add boolean flags for crime types
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
    # map via join (works regardless of Polars version)
    fbi_map_df = pl.DataFrame({'fbi_code': list(fbi_map.keys()), 'fbi_category': list(fbi_map.values())})
    df = df.join(fbi_map_df, on='fbi_code', how='left').with_columns([
        pl.col('fbi_category').fill_null('Unknown')
    ])

    severity_map = {
        'Homicide': 5, 'Manslaughter': 4, 'Sexual Assault': 4, 'Robbery': 4,
        'Aggravated Assault/Battery': 4, 'Arson': 4, 'Burglary': 3, 'Motor Vehicle Theft': 3,
        'Weapons Violation': 3, 'Drug Violation': 2, 'Fraud': 2, 'Forgery/Counterfeiting': 2,
        'Disorderly Conduct': 1, 'Vandalism': 1, 'Public Order Crime': 1, 'Miscellaneous Offense': 1
    }
    severity_df = pl.DataFrame({'fbi_category': list(severity_map.keys()), 'crime_severity_level': list(severity_map.values())})
    df = df.join(severity_df, on='fbi_category', how='left').with_columns([
        pl.col('crime_severity_level').fill_null(0).cast(pl.Int32)
    ])

    severity_labels = {5: 'Critical', 4: 'High', 3: 'Moderate', 2: 'Low', 1: 'Minor', 0: 'Unknown'}
    labels_df = pl.DataFrame({'crime_severity_level': list(severity_labels.keys()), 'crime_severity_label': list(severity_labels.values())})
    df = df.join(labels_df, on='crime_severity_level', how='left').with_columns([
        pl.col('crime_severity_label').fill_null('Unknown')
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
        ((pl.col('is_violent_combined') & pl.col('is_nighttime')) |
         (pl.col('is_weapon_related') & pl.col('is_weekend'))).alias('high_risk_situation')
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

    # export pl.DataFrame for debugging
    #df.to_pandas().to_csv("debug_silver_enhanced.csv", index=False)
    return df

def lat_lon_to_xy(latitude, longitude, target_epsg='3857'):
    """
    Converts latitude and longitude (WGS84, EPSG:4326) to X and Y 
    coordinates in a specified target coordinate system.

    Args:
        latitude (float): The latitude in degrees.
        longitude (float): The longitude in degrees.
        target_epsg (str or int): The EPSG code of the target projection (e.g., '3857' for Web Mercator).

    Returns:
        tuple: The X and Y coordinates in meters.
    """
    # Define the source (WGS84 lat/lon) and the target projection
    transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{target_epsg}", always_xy=True)
    
    # Perform the transformation (note: always_xy=True ensures lon, lat order for input)
    x, y = transformer.transform(longitude, latitude)
    
    return x, y

# ========================
# Column Check and Validation
# ========================
def ensure_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Ensure specified columns exist; add missing ones in one with_columns call.
    UPDATED: compute missing set once and add all columns in a single operation (faster).
    """
    required_orginal = ['id', 'case_number', 'date', 'block', 'iucr', 'primary_type', 'description'
                        , 'location_description', 'arrest', 'domestic', 'beat', 'district', 'ward'
                        , 'community_area', 'fbi_code', 'year', 'updated_on', 'x_coordinate'
                        , 'y_coordinate', 'latitude', 'longitude', '__index_level_0__'
                        , 'ward_out_of_range', 'community_area_out_of_range', 'quarter'
                        , 'month', 'day', 'hour', 'day_of_week_num', 'week_of_year', 'is_weekend'
                        , 'is_weekday', 'is_daytime', 'is_nighttime', 'is_am', 'is_pm', 'is_business_hours'
                        , 'is_off_business_hours', 'is_school_hours', 'is_late_night', 'part_of_day', 'season'
                        , 'is_holiday', 'is_missing_lat', 'is_missing_lon', 'lat_out_of_range'
                        , 'lon_out_of_range', 'is_bad_location', 'lat_bin', 'lon_bin', 'geo_grid'
                        , 'distance_from_downtown_km', 'crime_density_bin', 'is_violent', 'is_property'
                        , 'is_drug_related', 'is_public_order', 'is_weapon_related', 'fbi_category'
                        , 'crime_severity_level', 'crime_severity_label', 'is_violent_fbi', 'is_property_fbi'
                        , 'is_violent_combined', 'is_property_combined', 'crime_risk_score', 'high_risk_situation'
                        , 'crime_category', 'text_lat_bin', 'text_lon_bin']

    missing = [c for c in required_orginal if c not in df.columns]
    if not missing:
        return df

    exprs = []
    for col in missing:
        print(f"Adding missing column: {col}")
        if col == 'year':
            # use existing date column if present
            if 'date' in df.columns:
                exprs.append(pl.col('date').dt.year().alias('year'))
            else:
                exprs.append(pl.lit(None).alias('year'))
        elif col in ('x_coordinate', 'y_coordinate'):
            # derive x/y if lat/lon present, otherwise null
            if 'latitude' in df.columns and 'longitude' in df.columns:
                idx = 0 if col == 'x_coordinate' else 1
                # still uses python function per row (keeps existing behavior)
                exprs.append(
                    pl.when(pl.col('latitude').is_not_null() & pl.col('longitude').is_not_null())
                      .then(
                          pl.struct(['latitude', 'longitude']).apply(
                              lambda coords, _idx=idx: lat_lon_to_xy(coords['latitude'], coords['longitude'])[_idx]
                          )
                      )
                      .otherwise(None)
                      .alias(col)
                )
            else:
                exprs.append(pl.lit(None).alias(col))
        else:
            exprs.append(pl.lit(None).alias(col))

    # UPDATED: add all missing cols in one call (avoids repeated copies)
    df = df.with_columns(exprs)
    return df

def ensure_column_order(df: pl.DataFrame) -> pl.DataFrame:
    CANONICAL_COLUMNS = [
    'id','case_number','date','block','iucr','primary_type','description','location_description',
    'arrest','domestic','beat','district','ward','community_area','fbi_code','year','updated_on',
    'x_coordinate','y_coordinate','latitude','longitude','ward_out_of_range',
    'community_area_out_of_range','quarter','month','day','hour','day_of_week_num','week_of_year',
    'is_weekend','is_weekday','is_daytime','is_nighttime','is_am','is_pm','is_business_hours',
    'is_off_business_hours','is_school_hours','is_late_night','part_of_day','season','is_holiday',
    'is_missing_lat','is_missing_lon','lat_out_of_range','lon_out_of_range','is_bad_location','lat_bin',
    'lon_bin','geo_grid','distance_from_downtown_km','crime_density_bin','is_violent','is_property',
    'is_drug_related','is_public_order','is_weapon_related','fbi_category','crime_severity_level',
    'crime_severity_label','is_violent_fbi','is_property_fbi','is_violent_combined','is_property_combined',
    'crime_risk_score','high_risk_situation','crime_category','text_lat_bin','text_lon_bin'
    ]
    # update df columns order with canonical columns
    df = df.select(CANONICAL_COLUMNS)
    return df

# =========================
# ZIP FILE PROCESSING
# =========================
def process_single_zip(zip_path: str, silver_dir: str, temp_dir: str = None, rezip: bool = True):
    """Process one zip file; each zip contains a single parquet/csv file."""
    os.makedirs(silver_dir, exist_ok=True)
    temp_dir = temp_dir or os.path.join(silver_dir, "_tmp_extract")
    os.makedirs(temp_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zin:
        members = [m for m in zin.namelist() if m.lower().endswith((".parquet", ".csv"))]
        if not members:
            return

        member = members[0]  # assume one file per zip
        target_name = os.path.basename(member)
        target_path = os.path.join(temp_dir, target_name)
        try:
            zin.extract(member, path=temp_dir)
            extracted_candidate = os.path.join(temp_dir, member)
            if os.path.exists(extracted_candidate) and extracted_candidate != target_path:
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.move(extracted_candidate, target_path)

            # read single file and process
            if target_path.lower().endswith(".parquet"):
                df = pl.read_parquet(target_path)
            else:
                df = pl.read_csv(target_path)

            if "Date" in df.columns and "date" not in df.columns:
                df = df.rename({"Date": "date"})

            silver_df = add_features(df)
            # ensure all required columns exist
            silver_df = ensure_columns(silver_df)
            # ensure columns are in canonical order
            silver_df = ensure_column_order(silver_df)
            # derive date_str from silver_df 'date' column if available, else fall back to zip basename
            try:
                min_date = silver_df.select(pl.col("date").min()).to_series()[0]
                if min_date is None:
                    raise ValueError("no date available")
                date_str = min_date.strftime("%Y-%m-%d")
            except Exception:
                date_str = os.path.splitext(os.path.basename(zip_path))[0]
            
            out_parquet = os.path.join(temp_dir, f"silver_{date_str}.parquet")
            silver_df.write_parquet(out_parquet)
            zip_name = f"silver_{date_str}.zip"
            if rezip:
                out_zip = os.path.join(silver_dir, zip_name)
                with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zout:
                    zout.write(out_parquet, os.path.basename(out_parquet))
                os.remove(out_parquet)
            else:
                final_path = os.path.join(silver_dir, f"silver_{date_str}.parquet")
                shutil.move(out_parquet, final_path)
        except Exception as e:
            print(f'line error occurred here: {e.__traceback__.tb_lineno}')
            print(f"Error processing {zip_path}: {e}")
        finally:
            # remove extracted member file if it exists
            try:
                if os.path.exists(target_path):
                    os.remove(target_path)
            except Exception:
                pass

def create_silver_from_daily_zips(input_dir: str, silver_dir: str, temp_dir: str = None, rezip: bool = True):
    """Process zip files in input_dir one zip (and one inner file) at a time by delegating to process_single_zip."""
    zip_paths = sorted(glob.glob(os.path.join(input_dir, "*.zip")))
    start_time = datetime.now()
    for idx, zip_path in enumerate(zip_paths):
        #if idx == 1:
        #    break  # for testing, process only first zip
        if idx % 1000 == 0:
            elapsed = datetime.now() - start_time
            print(f"Processed {idx} of {len(zip_paths)} zips in {elapsed}")
        process_single_zip(zip_path, silver_dir, temp_dir=temp_dir, rezip=rezip)
    # cleanup temp_dir if used
    if temp_dir is None:
        tmp = os.path.join(silver_dir, "_tmp_extract")
    else:
        tmp = temp_dir
    try:
        if os.path.exists(tmp):
            shutil.rmtree(tmp)
    except Exception:
        pass

# =========================
# MAIN ETL PROCESS
# =========================
def main():
    # Define input and output directories
    bronze_data_folder = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\api_crime_data'
    silver_data_folder = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\silver_crime_data'
    # Create silver data from bronze daily zip files
    create_silver_from_daily_zips(bronze_data_folder, silver_data_folder, rezip=True)

if __name__ == "__main__":
    main()
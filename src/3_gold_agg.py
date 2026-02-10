# Import necessary libraries
import zipfile
import os
import polars as pl
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

def melt_and_pivot(df, value_var):
    # lowercase the value_var column
    df = df.with_columns(pl.col(value_var).str.to_lowercase())
    # snake_case the value_var column
    df = df.with_columns(pl.col(value_var).str.replace_all(r"\s+", "_"))
    # if value var beat, district, ward, community_area cast to int then back to str
    if value_var in ["beat", "district", "ward", "community_area"]:
        df = df.with_columns(pl.col(value_var).cast(pl.Int64).cast(pl.Utf8))
    # if value_var is beat lpad 4 characters
    if value_var == "beat":
        df = df.with_columns(pl.col(value_var).str.zfill(4))
    # if value_var is district, ward, community_area lpad 3 characters
    elif value_var in ["district", "ward", "community_area"]:
        df = df.with_columns(pl.col(value_var).str.zfill(3))

    # Group by report_type and crime_type, then count occurrences
    grouped_df = (
        df.group_by(["report_type", value_var])
        .agg(pl.col("id").count().alias("crime_count"))
    )
    # filter out null valu
    grouped_df = grouped_df.filter(pl.col(value_var).is_not_null())

    grouped_df = grouped_df.sort(value_var)

    # Pivot the data to create a wide format with each crime_type as a column
    pivoted_df = grouped_df.pivot(
        values="crime_count",
        index="report_type",
        on=value_var
    )
    if value_var == "primary_type":
        
        # Add prefix to the column names (except for the index column)
        pivoted_df = pivoted_df.rename({col: f"crime_{col}" for col in pivoted_df.columns if col != "report_type"})
    else:
        pivoted_df = pivoted_df.rename({col: f"{value_var}_{col}" for col in pivoted_df.columns if col != "report_type"})

    # Fill missing values with 0 (optional)
    pivoted_df = pivoted_df.fill_null(0)

    # Return the pivoted DataFrame
    return pivoted_df

def base_agg(df):
    """
    Perform base aggregation on the DataFrame.
    """
    # report type columns
    #' id', 'case_number', 'date', 'block', 'iucr', 'primary_type', 'description'
    #, 'location_description', 'arrest', 'domestic', 'beat', 'district', 'ward'
    #, 'community_area', 'fbi_code', 'year', 'updated_on', 'x_coordinate', 'y_coordinate'
    # , 'latitude', 'longitude', 
    # used already:
    # id', 'case_number', primary_type', arrest', domestic', 'beat', 'district', 'ward', 'community_area'
    # , 'fbi_code', 'iucr'
    # 'report_type', 'report_start_date', 'report_end_date']

    # Perform aggregation
    aggregated_df = (
        df.group_by("report_type")
        .agg([
            pl.col("date").min().cast(pl.Date).alias("report_start_date"),
            pl.col("date").max().cast(pl.Date).alias("report_end_date"),
            pl.col("id").count().alias("total_cases"),
            pl.col("case_number").n_unique().alias("unique_case_numbers"),
            pl.col("primary_type").n_unique().alias("unique_crime_types"),
            pl.col("fbi_code").n_unique().alias("unique_fbi_codes"),
            pl.col("iucr").n_unique().alias("unique_iucr_codes"),
            pl.col("arrest").sum().alias("total_arrests"),
            pl.col("domestic").sum().alias("total_domestic_cases"),
            pl.col("is_weekend").sum().alias("total_weekend_cases"),
            pl.col("is_nighttime").sum().alias("total_nighttime_cases"),
            pl.col("is_daytime").sum().alias("total_daytime_cases"),
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
        # convert report_end_date to yyyymm format
        .with_columns(pl.col("report_end_date").dt.strftime("%Y%m").cast(pl.Int64).alias("report_date"))

    )
    return aggregated_df

def process_zip_file(zip_file_path, temp_dir):
    expected_parquet_file_path = None 
    # check if file is a valid zip file
    if not zipfile.is_zipfile(zip_file_path):
        return None
    try:
        
        # Unzip the file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Get the expected Parquet file name based on the zip file name
        expected_parquet_file_name = os.path.basename(zip_file_path).replace('.zip', '.parquet')
        expected_parquet_file_path = os.path.join(temp_dir, expected_parquet_file_name)
        # Check if the expected Parquet file exists
        if not os.path.exists(expected_parquet_file_path):
            raise FileNotFoundError(f"Expected Parquet file not found: {expected_parquet_file_path}")

        # Read the Parquet file using Polars
        df = pl.read_parquet(expected_parquet_file_path)
        if df.is_empty():
            print(f"⚠️ The Parquet file {expected_parquet_file_path} is empty.")
            return None
        # Perform base aggregation
        df_agg = base_agg(df)

        # Melt and pivot the raw DataFrame for each specified value variable
        for value_var in ["primary_type", "fbi_code", "beat", "ward", "district", "community_area", "iucr"]:
            if value_var not in df.columns:
                continue
            # Melt and pivot the raw DataFrame for the current value variable
            melted_df = melt_and_pivot(df, value_var)
            # Join the melted DataFrame with the aggregated DataFrame
            df_agg = df_agg.join(melted_df, on="report_type", how="left")

        # Delete the temporary file
        os.remove(expected_parquet_file_path)
        
        return df_agg
    except Exception as e:
        print(f"Error processing zip file {zip_file_path}: {e}")
        return None
    finally:
        # Clean up the temporary directory
        if expected_parquet_file_path and os.path.exists(expected_parquet_file_path):
            os.remove(expected_parquet_file_path)

def save_aggregated_df(df, output_dir, file_name):
    """
    Save the aggregated DataFrame to a Parquet file and create a zip archive.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    # Ensure file_name is just the base name
    file_name = os.path.basename(file_name).replace('.zip', '').replace('.parquet', '')
    # split file_name by underscore - # silver_r12_2001_03_2002_03.zip.parquet
    file_name_parts = file_name.split("_")
    report_type = file_name_parts[1]
    report_start = "".join(file_name_parts[2:4])
    report_end = "".join(file_name_parts[4:6])
    file_name = f"{report_type}_{report_start}_{report_end}.parquet"
    # Ensure file_name ends with .parquet
    if not file_name.lower().endswith('.parquet'):
        file_name += '.parquet'

    output_file_path = os.path.join(output_dir, file_name)

    # Write parquet file
    df.write_parquet(output_file_path)

    # Check if file exists before zipping
    if not os.path.exists(output_file_path):
        raise FileNotFoundError(f"Parquet file not found: {output_file_path}")

    zip_name = f'gold_{file_name.replace(".parquet", ".zip")}'
    zip_path = os.path.join(output_dir, zip_name)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(output_file_path, arcname=os.path.basename(output_file_path))
    
    # delete the temporary Parquet file
    os.remove(output_file_path)


# Move the process_zip function to the global scope
def process_zip(zip_file, folder_path, temp_dir, output_dir, file_count):
    zip_file_path = os.path.join(folder_path, zip_file)
    df = process_zip_file(zip_file_path, temp_dir)
    if df is not None and df.shape[0] > 0:
        save_aggregated_df(df, output_dir, file_name=zip_file)
        return f'Processed {zip_file}'
    return f"Skipped {zip_file}"

def main():
    # Report period zip folder
    folder_path = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\silver_report_period_crime_data'
    # Temp directory for extracting zip files
    temp_dir = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\temp'
    # Output folder
    output_dir = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\gold_data'

    # Create temp directory if it doesn't exist
    os.makedirs(temp_dir, exist_ok=True)

    # Initialize file count and start time
    file_count = 0
    global_start = time.time()
    time_start = time.time()
    print(f'Start Time: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_start))}')

    # Use ProcessPoolExecutor for concurrent processing
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_zip, zip_file, folder_path, temp_dir, output_dir, file_count)
            for zip_file in sorted(os.listdir(folder_path)) if zip_file.endswith('.zip')
        ]
        # collect results
        results = []
        for future in as_completed(futures):
            file_count += 1
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                pass

    # Print a summary of the results
    processed_files = [res for res in results if res and "Processed" in res]
    skipped_files = [res for res in results if res and "Skipped" in res]

    print(f"\nSummary:")
    print(f"Total files processed: {len(processed_files)}")
    print(f"Total files skipped: {len(skipped_files)}")
    print(f"Skipped files: {skipped_files}")

    # Calculate total processing time
    time_end = time.time()
    time_elapsed_hours = (time_end - global_start) / 3600
    print(f'End Time: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_end))}')
    print(f"Total processing time: {time_elapsed_hours:.2f} hours.")

    # Remove the temporary directory if empty
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()



# report type columns
#'id', 'case_number', 'date', 'block', 'iucr', 'primary_type', 'description'
#, 'location_description', 'arrest', 'domestic', 'beat', 'district', 'ward'
#, 'community_area', 'fbi_code', 'year', 'updated_on', 'x_coordinate', 'y_coordinate'
# , 'latitude', 'longitude', 'ward_out_of_range', 'community_area_out_of_range'
# , 'quarter', 'month', 'day', 'hour', 'day_of_week_num', 'week_of_year'
# , 'is_weekend', 'is_weekday', 'is_daytime', 'is_nighttime', 'is_am', 'is_pm'
# , 'is_business_hours', 'is_off_business_hours', 'is_school_hours', 'is_late_night'
# , 'part_of_day', 'season', 'is_holiday', 'is_missing_lat', 'is_missing_lon'
# , 'lat_out_of_range', 'lon_out_of_range', 'is_bad_location', 'lat_bin', 'lon_bin'
# , 'geo_grid', 'distance_from_downtown_km', 'crime_density_bin', 'is_violent'
# , 'is_property', 'is_drug_related', 'is_public_order', 'is_weapon_related'
# , 'fbi_category', 'crime_severity_level', 'crime_severity_label', 'is_violent_fbi'
# , 'is_property_fbi', 'is_violent_combined', 'is_property_combined'
# , 'crime_risk_score', 'high_risk_situation', 'crime_category', 'text_lat_bin'
# , 'text_lon_bin', 'report_type', 'report_start_date', 'report_end_date']

# aggregate target columns 
# 'report_type', 'report_date', 'start_date', 'end_date', 'total_cases'
# , 'unique_crime_types', 'total_arrests', 'total_domestic_cases', 'total_weekend_cases'
# , 'total_nighttime_cases', 'total_daytime_cases', 'total_violent_cases', 'total_property_cases'
# , 'total_drug_cases', 'total_public_order_cases', 'total_weapon_cases', 'total_high_risk_cases'
# , 'avg_crime_risk_score', 'max_crime_risk_score', 'avg_severity_level', 'avg_distance_from_downtown_km', 'unique_beats', 'unique_wards', 'unique_districts', 'unique_community_areas', 'crime_criminal_sexual_assault', 'crime_assault', 'crime_other_narcotic_violation', 'crime_kidnapping', 'crime_weapons_violation', 'crime_arson', 'crime_sex_offense', 'crime_interference_with_public_officer', 'crime_motor_vehicle_theft', 'crime_criminal_trespass', 'crime_intimidation', 'crime_obscenity', 'crime_deceptive_practice', 'crime_liquor_law_violation', 'crime_robbery', 'crime_stalking', 'crime_gambling', 'crime_crim_sexual_assault', 'crime_burglary', 'crime_battery', 'crime_ritualism', 'crime_public_peace_violation', 'crime_homicide', 'crime_domestic_violence', 'crime_offense_involving_children', 'crime_prostitution', 'crime_criminal_damage', 'crime_narcotics', 'crime_theft', 'crime_other_offense', 'district_1', 'district_2', 'district_3', 'district_4', 'district_5', 'district_6', 'district_7', 'district_8', 'district_9', 'district_10', 'district_11', 'district_12', 'district_14', 'district_15', 'district_16', 'district_17', 'district_18', 'district_19', 'district_20', 'district_22', 'district_24', 'district_25', 'ward_1', 'ward_2', 'ward_3', 'ward_4', 'ward_5', 'ward_6', 'ward_7', 'ward_8', 'ward_9', 'ward_10', 'ward_11', 'ward_12', 'ward_13', 'ward_14', 'ward_15', 'ward_16', 'ward_17', 'ward_18', 'ward_19', 'ward_20', 'ward_21', 'ward_22', 'ward_23', 'ward_24', 'ward_25', 'ward_26', 'ward_27', 'ward_28', 'ward_29', 'ward_30', 'ward_31', 'ward_32', 'ward_33', 'ward_34', 'ward_35', 'ward_36', 'ward_37', 'ward_38', 'ward_39', 'ward_40', 'ward_41', 'ward_42', 'ward_43', 'ward_44', 'ward_45', 'ward_46', 'ward_47', 'ward_48', 'ward_49', 'ward_50', 'community_area_1', 'community_area_2', 'community_area_3', 'community_area_4', 'community_area_5', 'community_area_6', 'community_area_7', 'community_area_8', 'community_area_10', 'community_area_11', 'community_area_12', 'community_area_13', 'community_area_14', 'community_area_15', 'community_area_16', 'community_area_17', 'community_area_19', 'community_area_20', 'community_area_21', 'community_area_22', 'community_area_23', 'community_area_24', 'community_area_25', 'community_area_26', 'community_area_27', 'community_area_28', 'community_area_29', 'community_area_30', 'community_area_31'
# , 'community_area_32', 'community_area_33', 'community_area_34', 'community_area_35', 'community_area_38', 'community_area_39', 'community_area_40', 'community_area_41', 'community_area_42', 'community_area_43', 'community_area_44', 'community_area_45', 'community_area_46', 'community_area_47', 'community_area_48', 'community_area_49', 'community_area_50', 'community_area_52', 'community_area_53', 'community_area_54', 'community_area_56', 'community_area_58', 'community_area_59', 'community_area_60', 'community_area_61', 'community_area_62', 'community_area_63', 'community_area_64', 'community_area_65', 'community_area_66', 'community_area_67', 'community_area_68', 'community_area_69', 'community_area_70', 'community_area_71', 'community_area_72', 'community_area_73', 'community_area_74', 'community_area_75', 'community_area_76', 'community_area_77', 'fbi_26', 'fbi_09', 'fbi_22', 'fbi_06', 'fbi_18', 'fbi_03', 'fbi_24', 'fbi_07', 'fbi_01A', 'fbi_08B', 'fbi_04A', 'fbi_13', 'fbi_12', 'fbi_19', 'fbi_17', 'fbi_02', 'fbi_08A', 'fbi_15', 'fbi_10', 'fbi_16', 'fbi_05', 'fbi_04B', 'fbi_11', 'fbi_14', 'fbi_20', 'beat_111', 'beat_112', 'beat_113', 'beat_122', 'beat_123', 'beat_124', 'beat_131', 'beat_132', 'beat_133', 'beat_134', 'beat_211', 'beat_212', 'beat_213', 'beat_214', 'beat_221', 'beat_222', 'beat_223', 'beat_224', 'beat_231', 'beat_232', 'beat_233', 'beat_234', 'beat_310', 'beat_311', 'beat_312', 'beat_313', 'beat_314', 'beat_321', 'beat_322', 'beat_323', 'beat_324', 'beat_331', 'beat_332', 'beat_333', 'beat_334', 'beat_411', 'beat_412', 'beat_413', 'beat_414', 'beat_421', 'beat_422', 'beat_423', 'beat_424', 'beat_431', 'beat_432', 'beat_433', 'beat_434', 'beat_511', 'beat_512', 'beat_513', 'beat_522', 'beat_523', 'beat_524', 'beat_531', 'beat_532', 'beat_533', 'beat_611', 'beat_612', 'beat_613', 'beat_614', 'beat_621', 'beat_622', 'beat_623', 'beat_624', 'beat_631', 'beat_632', 'beat_633', 'beat_634', 'beat_711', 'beat_712', 'beat_713', 'beat_714', 'beat_715', 'beat_722', 'beat_723', 'beat_724', 'beat_725', 'beat_726', 'beat_731', 'beat_732', 'beat_733', 'beat_734', 'beat_735', 'beat_811', 'beat_812', 'beat_813', 'beat_814', 'beat_815', 'beat_821', 'beat_822', 'beat_823', 'beat_824', 'beat_825', 'beat_831', 'beat_832', 'beat_833', 'beat_834', 'beat_835', 'beat_911', 'beat_912', 'beat_913', 'beat_914', 'beat_915', 'beat_921', 'beat_922', 'beat_923', 'beat_924', 'beat_925', 'beat_931', 'beat_932', 'beat_933', 'beat_934', 'beat_935', 'beat_1011', 'beat_1012', 'beat_1013', 'beat_1014', 'beat_1021', 'beat_1022', 'beat_1023', 'beat_1024', 'beat_1031', 'beat_1032', 'beat_1033', 'beat_1034', 'beat_1111', 'beat_1112', 'beat_1113', 'beat_1114', 'beat_1115', 'beat_1121', 'beat_1122', 'beat_1123', 'beat_1124', 'beat_1125', 'beat_1131', 'beat_1132', 'beat_1133', 'beat_1134', 'beat_1135', 'beat_1211', 'beat_1212', 'beat_1213', 'beat_1222', 'beat_1223', 'beat_1224', 'beat_1231', 'beat_1232', 'beat_1233', 'beat_1234', 'beat_1311', 'beat_1312', 'beat_1313', 'beat_1322', 'beat_1323', 'beat_1324', 'beat_1331', 'beat_1332', 'beat_1333', 'beat_1411', 'beat_1412', 'beat_1413', 'beat_1414', 'beat_1421', 'beat_1422', 'beat_1423', 'beat_1424', 'beat_1431', 'beat_1432', 'beat_1433', 'beat_1434', 'beat_1511', 'beat_1512', 'beat_1513', 'beat_1522', 'beat_1523', 'beat_1524', 'beat_1531', 'beat_1532', 'beat_1533', 'beat_1611', 'beat_1612', 'beat_1613', 'beat_1614', 'beat_1621', 'beat_1622', 'beat_1623', 'beat_1624', 'beat_1631', 'beat_1632', 'beat_1633', 'beat_1634', 'beat_1651', 'beat_1711', 'beat_1712', 'beat_1713', 'beat_1722', 'beat_1723', 'beat_1724', 'beat_1731', 'beat_1732', 'beat_1733', 'beat_1811', 'beat_1812', 'beat_1813', 'beat_1814', 'beat_1821', 'beat_1822', 'beat_1823', 'beat_1824', 'beat_1831', 'beat_1832', 'beat_1833', 'beat_1834', 'beat_1911', 'beat_1912', 'beat_1913', 'beat_1922', 'beat_1923', 'beat_1924', 'beat_1931', 'beat_1932', 'beat_1933', 'beat_2011', 'beat_2012', 'beat_2013', 'beat_2022', 'beat_2023', 'beat_2024', 'beat_2031', 'beat_2032', 'beat_2033', 'beat_2111', 'beat_2112', 'beat_2113', 'beat_2122', 'beat_2123', 'beat_2124', 'beat_2131', 'beat_2132', 'beat_2133', 'beat_2211', 'beat_2212', 'beat_2213', 'beat_2221', 'beat_2222', 'beat_2223', 'beat_2232', 'beat_2233', 'beat_2234', 'beat_2311', 'beat_2312', 'beat_2313', 'beat_2322', 'beat_2323', 'beat_2324', 'beat_2331', 'beat_2332', 'beat_2333', 'beat_2411', 'beat_2412', 'beat_2413', 'beat_2422', 'beat_2423', 'beat_2424', 'beat_2431', 'beat_2432', 'beat_2433', 'beat_2511', 'beat_2512', 'beat_2513', 'beat_2514', 'beat_2515', 'beat_2521', 'beat_2522', 'beat_2523', 'beat_2524', 'beat_2525', 'beat_2531', 'beat_2532', 'beat_2533', 'beat_2534', 'beat_2535', 'crime_public_indecency', 'community_area_18', 'community_area_36', 'community_area_37', 'community_area_51', 'beat_114', 'community_area_9', 'community_area_57', 'beat_215', 'beat_1214', 'beat_1221', 'beat_1654', 'crime_concealed_carry_license_violation', 'community_area_55', 'beat_1215', 'beat_1235', 'beat_1925', 'beat_225', 'district_31', 'beat_1225', 'beat_1915', 'community_area_0', 'beat_1934', 'beat_1914', 'beat_1921', 'fbi_01B', 'beat_1935', 'beat_121', 'district_21', 'beat_235', 'beat_430', 'beat_1653', 'crime_human_trafficking', 'beat_1652', 'beat_1655', 'crime_non-criminal', 'beat_1650', 'prior_start_date', 'prior_end_date', 'prior_total_cases', 'prior_unique_crime_types', 'prior_total_arrests', 'prior_total_domestic_cases', 'prior_total_weekend_cases', 'prior_total_nighttime_cases', 'prior_total_daytime_cases', 'prior_total_violent_cases', 'prior_total_property_cases', 'prior_total_drug_cases', 'prior_total_public_order_cases', 'prior_total_weapon_cases', 'prior_total_high_risk_cases', 'prior_avg_crime_risk_score', 'prior_max_crime_risk_score', 'prior_avg_severity_level', 'prior_avg_distance_from_downtown_km', 'prior_unique_beats', 'prior_unique_wards', 'prior_unique_districts', 'prior_unique_community_areas', 'prior_crime_criminal_sexual_assault', 'prior_crime_assault', 'prior_crime_other_narcotic_violation', 'prior_crime_kidnapping', 'prior_crime_weapons_violation', 'prior_crime_arson', 'prior_crime_sex_offense', 'prior_crime_interference_with_public_officer', 'prior_crime_motor_vehicle_theft', 'prior_crime_criminal_trespass', 'prior_crime_intimidation', 'prior_crime_obscenity', 'prior_crime_deceptive_practice', 'prior_crime_liquor_law_violation', 'prior_crime_robbery', 'prior_crime_stalking', 'prior_crime_gambling', 'prior_crime_crim_sexual_assault', 'prior_crime_burglary', 'prior_crime_battery', 'prior_crime_ritualism', 'prior_crime_public_peace_violation', 'prior_crime_homicide', 'prior_crime_domestic_violence', 'prior_crime_offense_involving_children', 'prior_crime_prostitution', 'prior_crime_criminal_damage', 'prior_crime_narcotics', 'prior_crime_theft', 'prior_crime_other_offense', 'prior_district_1', 'prior_district_2', 'prior_district_3', 'prior_district_4', 'prior_district_5', 'prior_district_6', 'prior_district_7', 'prior_district_8', 'prior_district_9', 'prior_district_10', 'prior_district_11', 'prior_district_12', 'prior_district_14', 'prior_district_15', 'prior_district_16', 'prior_district_17', 'prior_district_18', 'prior_district_19', 'prior_district_20', 'prior_district_22', 'prior_district_24', 'prior_district_25', 'prior_ward_1', 'prior_ward_2', 'prior_ward_3', 'prior_ward_4', 'prior_ward_5', 'prior_ward_6', 'prior_ward_7', 'prior_ward_8', 'prior_ward_9', 'prior_ward_10', 'prior_ward_11', 'prior_ward_12', 'prior_ward_13', 'prior_ward_14', 'prior_ward_15', 'prior_ward_16', 'prior_ward_17'
# , 'prior_ward_18', 'prior_ward_19', 'prior_ward_20', 'prior_ward_21', 'prior_ward_22', 'prior_ward_23', 'prior_ward_24', 'prior_ward_25', 'prior_ward_26', 'prior_ward_27', 'prior_ward_28', 'prior_ward_29', 'prior_ward_30', 'prior_ward_31', 'prior_ward_32', 'prior_ward_33', 'prior_ward_34', 'prior_ward_35', 'prior_ward_36', 'prior_ward_37', 'prior_ward_38', 'prior_ward_39', 'prior_ward_40', 'prior_ward_41', 'prior_ward_42', 'prior_ward_43', 'prior_ward_44', 'prior_ward_45', 'prior_ward_46', 'prior_ward_47', 'prior_ward_48', 'prior_ward_49', 'prior_ward_50', 'prior_community_area_1', 'prior_community_area_2', 'prior_community_area_3', 'prior_community_area_4', 'prior_community_area_5', 'prior_community_area_6', 'prior_community_area_7', 'prior_community_area_8', 'prior_community_area_10', 'prior_community_area_11', 'prior_community_area_12', 'prior_community_area_13', 'prior_community_area_14', 'prior_community_area_15', 'prior_community_area_16', 'prior_community_area_17', 'prior_community_area_19', 'prior_community_area_20', 'prior_community_area_21', 'prior_community_area_22', 'prior_community_area_23', 'prior_community_area_24', 'prior_community_area_25', 'prior_community_area_26', 'prior_community_area_27', 'prior_community_area_28', 'prior_community_area_29', 'prior_community_area_30', 'prior_community_area_31', 'prior_community_area_32', 'prior_community_area_33', 'prior_community_area_34', 'prior_community_area_35', 'prior_community_area_38', 'prior_community_area_39', 'prior_community_area_40', 'prior_community_area_41', 'prior_community_area_42', 'prior_community_area_43', 'prior_community_area_44', 'prior_community_area_45', 'prior_community_area_46', 'prior_community_area_47', 'prior_community_area_48', 'prior_community_area_49', 'prior_community_area_50', 'prior_community_area_52', 'prior_community_area_53', 'prior_community_area_54', 'prior_community_area_56', 'prior_community_area_58', 'prior_community_area_59', 'prior_community_area_60', 'prior_community_area_61', 'prior_community_area_62', 'prior_community_area_63', 'prior_community_area_64', 'prior_community_area_65', 'prior_community_area_66', 'prior_community_area_67', 'prior_community_area_68', 'prior_community_area_69', 'prior_community_area_70', 'prior_community_area_71', 'prior_community_area_72', 'prior_community_area_73', 'prior_community_area_74', 'prior_community_area_75', 'prior_community_area_76', 'prior_community_area_77', 'prior_fbi_26', 'prior_fbi_09', 'prior_fbi_22', 'prior_fbi_06', 'prior_fbi_18', 'prior_fbi_03', 'prior_fbi_24', 'prior_fbi_07', 'prior_fbi_01A', 'prior_fbi_08B', 'prior_fbi_04A', 'prior_fbi_13', 'prior_fbi_12', 'prior_fbi_19', 'prior_fbi_17', 'prior_fbi_02', 'prior_fbi_08A', 'prior_fbi_15', 'prior_fbi_10', 'prior_fbi_16', 'prior_fbi_05', 'prior_fbi_04B', 'prior_fbi_11', 'prior_fbi_14', 'prior_fbi_20', 'prior_beat_111', 'prior_beat_112', 'prior_beat_113', 'prior_beat_122', 'prior_beat_123', 'prior_beat_124', 'prior_beat_131', 'prior_beat_132', 'prior_beat_133', 'prior_beat_134', 'prior_beat_211', 'prior_beat_212', 'prior_beat_213', 'prior_beat_214', 'prior_beat_221', 'prior_beat_222', 'prior_beat_223', 'prior_beat_224', 'prior_beat_231', 'prior_beat_232', 'prior_beat_233', 'prior_beat_234', 'prior_beat_310', 'prior_beat_311', 'prior_beat_312', 'prior_beat_313', 'prior_beat_314', 'prior_beat_321', 'prior_beat_322', 'prior_beat_323', 'prior_beat_324', 'prior_beat_331', 'prior_beat_332', 'prior_beat_333', 'prior_beat_334', 'prior_beat_411', 'prior_beat_412', 'prior_beat_413', 'prior_beat_414', 'prior_beat_421', 'prior_beat_422', 'prior_beat_423', 'prior_beat_424', 'prior_beat_431', 'prior_beat_432', 'prior_beat_433', 'prior_beat_434', 'prior_beat_511', 'prior_beat_512', 'prior_beat_513', 'prior_beat_522', 'prior_beat_523', 'prior_beat_524', 'prior_beat_531', 'prior_beat_532', 'prior_beat_533', 'prior_beat_611', 'prior_beat_612', 'prior_beat_613', 'prior_beat_614', 'prior_beat_621', 'prior_beat_622', 'prior_beat_623', 'prior_beat_624', 'prior_beat_631', 'prior_beat_632', 'prior_beat_633', 'prior_beat_634', 'prior_beat_711', 'prior_beat_712', 'prior_beat_713', 'prior_beat_714', 'prior_beat_715', 'prior_beat_722', 'prior_beat_723', 'prior_beat_724', 'prior_beat_725', 'prior_beat_726', 'prior_beat_731', 'prior_beat_732', 'prior_beat_733', 'prior_beat_734', 'prior_beat_735', 'prior_beat_811', 'prior_beat_812', 'prior_beat_813', 'prior_beat_814', 'prior_beat_815', 'prior_beat_821', 'prior_beat_822', 'prior_beat_823', 'prior_beat_824', 'prior_beat_825', 'prior_beat_831', 'prior_beat_832', 'prior_beat_833', 'prior_beat_834', 'prior_beat_835', 'prior_beat_911', 'prior_beat_912', 'prior_beat_913', 'prior_beat_914', 'prior_beat_915', 'prior_beat_921', 'prior_beat_922', 'prior_beat_923', 'prior_beat_924', 'prior_beat_925', 'prior_beat_931', 'prior_beat_932', 'prior_beat_933', 'prior_beat_934', 'prior_beat_935', 'prior_beat_1011', 'prior_beat_1012', 'prior_beat_1013', 'prior_beat_1014', 'prior_beat_1021', 'prior_beat_1022', 'prior_beat_1023', 'prior_beat_1024', 'prior_beat_1031', 'prior_beat_1032', 'prior_beat_1033', 'prior_beat_1034', 'prior_beat_1111', 'prior_beat_1112', 'prior_beat_1113', 'prior_beat_1114', 'prior_beat_1115', 'prior_beat_1121', 'prior_beat_1122', 'prior_beat_1123', 'prior_beat_1124', 'prior_beat_1125', 'prior_beat_1131', 'prior_beat_1132', 'prior_beat_1133', 'prior_beat_1134', 'prior_beat_1135', 'prior_beat_1211', 'prior_beat_1212', 'prior_beat_1213', 'prior_beat_1222', 'prior_beat_1223', 'prior_beat_1224', 'prior_beat_1231', 'prior_beat_1232', 'prior_beat_1233', 'prior_beat_1234', 'prior_beat_1311', 'prior_beat_1312', 'prior_beat_1313', 'prior_beat_1322', 'prior_beat_1323', 'prior_beat_1324', 'prior_beat_1331', 'prior_beat_1332', 'prior_beat_1333', 'prior_beat_1411', 'prior_beat_1412', 'prior_beat_1413', 'prior_beat_1414', 'prior_beat_1421', 'prior_beat_1422', 'prior_beat_1423', 'prior_beat_1424', 'prior_beat_1431', 'prior_beat_1432', 'prior_beat_1433', 'prior_beat_1434', 'prior_beat_1511', 'prior_beat_1512', 'prior_beat_1513', 'prior_beat_1522', 'prior_beat_1523', 'prior_beat_1524', 'prior_beat_1531', 'prior_beat_1532', 'prior_beat_1533', 'prior_beat_1611', 'prior_beat_1612', 'prior_beat_1613', 'prior_beat_1614', 'prior_beat_1621', 'prior_beat_1622', 'prior_beat_1623', 'prior_beat_1624', 'prior_beat_1631', 'prior_beat_1632', 'prior_beat_1633', 'prior_beat_1634', 'prior_beat_1651', 'prior_beat_1711', 'prior_beat_1712', 'prior_beat_1713', 'prior_beat_1722', 'prior_beat_1723', 'prior_beat_1724', 'prior_beat_1731', 'prior_beat_1732', 'prior_beat_1733', 'prior_beat_1811', 'prior_beat_1812', 'prior_beat_1813', 'prior_beat_1814', 'prior_beat_1821', 'prior_beat_1822', 'prior_beat_1823', 'prior_beat_1824', 'prior_beat_1831', 'prior_beat_1832', 'prior_beat_1833', 'prior_beat_1834', 'prior_beat_1911', 'prior_beat_1912', 'prior_beat_1913', 'prior_beat_1922', 'prior_beat_1923', 'prior_beat_1924', 'prior_beat_1931', 'prior_beat_1932', 'prior_beat_1933', 'prior_beat_2011', 'prior_beat_2012', 'prior_beat_2013', 'prior_beat_2022', 'prior_beat_2023', 'prior_beat_2024', 'prior_beat_2031', 'prior_beat_2032', 'prior_beat_2033', 'prior_beat_2111', 'prior_beat_2112', 'prior_beat_2113', 'prior_beat_2122', 'prior_beat_2123', 'prior_beat_2124', 'prior_beat_2131', 'prior_beat_2132', 'prior_beat_2133', 'prior_beat_2211', 'prior_beat_2212', 'prior_beat_2213', 'prior_beat_2221', 'prior_beat_2222', 'prior_beat_2223', 'prior_beat_2232', 'prior_beat_2233', 'prior_beat_2234', 'prior_beat_2311', 'prior_beat_2312', 'prior_beat_2313', 'prior_beat_2322', 'prior_beat_2323', 'prior_beat_2324', 'prior_beat_2331', 'prior_beat_2332', 'prior_beat_2333', 'prior_beat_2411', 'prior_beat_2412', 'prior_beat_2413', 'prior_beat_2422', 'prior_beat_2423', 'prior_beat_2424', 'prior_beat_2431', 'prior_beat_2432', 'prior_beat_2433', 'prior_beat_2511', 'prior_beat_2512', 'prior_beat_2513', 'prior_beat_2514', 'prior_beat_2515', 'prior_beat_2521', 'prior_beat_2522', 'prior_beat_2523', 'prior_beat_2524', 'prior_beat_2525', 'prior_beat_2531', 'prior_beat_2532', 'prior_beat_2533', 'prior_beat_2534', 'prior_beat_2535', 'prior_crime_public_indecency', 'prior_community_area_18', 'prior_community_area_36', 'prior_community_area_37', 'prior_community_area_51', 'prior_beat_114', 'prior_community_area_9', 'prior_community_area_57', 'prior_beat_215', 'prior_beat_1214', 'prior_beat_1221', 'prior_beat_1654', 'prior_crime_concealed_carry_license_violation', 'prior_community_area_55', 'prior_beat_1215', 'prior_beat_1235', 'prior_beat_1925', 'prior_beat_225', 'prior_district_31', 'prior_beat_1225', 'prior_beat_1915', 'prior_community_area_0', 'prior_beat_1934', 'prior_beat_1914', 'prior_beat_1921', 'prior_fbi_01B', 'prior_beat_1935', 'prior_beat_121', 'prior_district_21', 'prior_beat_235', 'prior_beat_430', 'prior_beat_1653', 'prior_crime_human_trafficking', 'prior_beat_1652', 'prior_beat_1655', 'prior_crime_non-criminal', 'prior_beat_1650'
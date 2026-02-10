import os
import zipfile
import pandas as pd
import shutil
import tempfile

# Input folder containing ZIP files
input_dir = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\gold_data'
# output folder for extracted files and combined Parquet file
output_dir = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\gold_data_dash'
dashboard_output_dir = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\gold_data'

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)
os.makedirs(dashboard_output_dir, exist_ok=True)

# Function to extract all ZIP files in the input directory to a temporary directory
def extract_zip_files_to_temp(input_dir):
    temp_dir = tempfile.mkdtemp()
    zip_files = [file_name for file_name in os.listdir(input_dir) if file_name.endswith('.zip')]
    for i, file_name in enumerate(zip_files):
        zip_path = os.path.join(input_dir, file_name)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        if (i + 1) % 250 == 0:
            print(f"Processed {i + 1} out of {len(zip_files)} zip files...")
    print(f"All {len(zip_files)} zip files have been processed.")
    return temp_dir

# Function to clean up temporary directory
def clean_up_temp_dir(temp_dir):
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"Temporary directory {temp_dir} has been removed.")

# Function to combine all Parquet files in the output directory incrementally and save as a single Parquet file
def combine_parquet_files_incremental(output_dir, combined_file_name='combined_data.parquet'):
    parquet_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.parquet')]
    if not parquet_files:
        print("No Parquet files found to combine.")
        return

    combined_file_path = os.path.join(output_dir, combined_file_name)

    # Remove existing combined file if it exists to avoid appending to an old file
    if os.path.exists(combined_file_path):
        os.remove(combined_file_path)

    # Process files incrementally to avoid memory issues
    combined_df = pd.DataFrame()
    for i, file in enumerate(parquet_files):
        df = pd.read_parquet(file)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Print progress for every 250 files
        if (i + 1) % 250 == 0:
            print(f"Processed {i + 1} out of {len(parquet_files)} Parquet files...")
    # for all int columns, fill NaN with 0
    int_columns = combined_df.select_dtypes(include=['int64', 'Int64']).columns
    float_columns = combined_df.select_dtypes(include=['float64', 'Float64']).columns
    combined_df[float_columns] = combined_df[float_columns].fillna(0.0)
    combined_df[int_columns] = combined_df[int_columns].fillna(0)

    # Save the combined DataFrame as a single Parquet file
    combined_df.to_parquet(combined_file_path, index=False)
    print(f"Combined Parquet file saved to: {combined_file_path}")

def create_prior_df(output_dir, combined_file_name='combined_data.parquet'):
    combined_file_path = os.path.join(output_dir, combined_file_name)
    if not os.path.exists(combined_file_path):
        print(f"Combined file {combined_file_path} does not exist.")
        return None

    df = pd.read_parquet(combined_file_path)

    # Check if required columns exist
    required_columns = ['report_end_date', 'report_type']
    for col in required_columns:
        if col not in df.columns:
            raise KeyError(f"Required column '{col}' is missing from the DataFrame.")
    # Convert report_end_date to datetime
    df['report_end_date'] = pd.to_datetime(df['report_end_date'])
    # Create a copy of the DataFrame for prior data
    df_prior = df.copy()
    # Convert report_end_date to datetime and subtract by 12 months
    df['prior_end_date'] = df['report_end_date'] - pd.DateOffset(months=12)
    
    # prefix columns in df_prior with 'prior_' except for report_end_date and report_type
    df_prior = df_prior.rename(columns={col: f'prior_{col}' for col in df_prior.columns})

    df = df.merge(
        df_prior,
        left_on=['prior_end_date', 'report_type'],
        right_on=['prior_report_end_date', 'prior_report_type'],
        how='left'
    )

    # remove 

    # export df to csv
    return df

# Updated main script execution
if __name__ == "__main__":
    temp_dir = None
    try:
        # Extract ZIP files to a temporary directory
        temp_dir = extract_zip_files_to_temp(input_dir)

        # Combine all Parquet files from the temporary directory incrementally into a single Parquet file
        combine_parquet_files_incremental(temp_dir, os.path.join(output_dir, 'combined_data.parquet'))

        df = create_prior_df(output_dir, os.path.join(output_dir, 'combined_data.parquet'))

        # export 
        file_name = 'chicago_crimes_gold_reports_.parquet'
        output_file_path = os.path.join(output_dir, file_name)
        dashboard_file_path = os.path.join(dashboard_output_dir, file_name)
        df.to_parquet(output_file_path, index=False)
        shutil.copy(output_file_path, dashboard_file_path)
    finally:
        # Ensure temporary files are cleaned up even if the script is interrupted
        if temp_dir:
            clean_up_temp_dir(temp_dir)
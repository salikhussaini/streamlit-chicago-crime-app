# =========================
# IMPORTS
# ========================
from datetime import date, datetime, timedelta
import polars as pl
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    df = pl.DataFrame(records)
    # filter start date 2001-01-01
    df = df.filter(pl.col("start_date") >= datetime(2001, 1, 1))
    # filter out report type prior r12 and prior ytd reports
    df = df.filter(~df["report_type"].str.to_lowercase().is_in(["prior r12", "prior ytd"]))
    return df

def get_min_max_dates(folder):
    """Get min and max dates from a folder's data that is partitioned by date."""

    min_date = None
    max_date = None
    # crawl files in folder that end with .zip
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".zip"):
                # extract date parts from the folder structure
                # file path example: silver_2001-01-01.zip
                # remove .zip extension
                file_name = os.path.splitext(file)[0] 
                # get date part
                date_str = file_name.split("_")[-1] 
  
                try:
                    # parse date
                    date = datetime.strptime(date_str, "%Y-%m-%d") 
                    # update min_date
                    if min_date is None or date < min_date:
                        min_date = date
                    if max_date is None or date > max_date:
                        max_date = date
                except ValueError:
                    continue
    return min_date, max_date

def check_if_file_missing(folder, min_date, max_date):
    """Check if any monthly files are missing in the folder."""
    current = min_date
    missing_files = []
    while current <= max_date:
        expected_file = f"silver_{current.strftime('%Y-%m-%d')}.zip"
        if not os.path.exists(os.path.join(folder, expected_file)):
            missing_files.append(expected_file)
        year = current.year
        month = current.month
        if month == 12:
            current = datetime(year + 1, 1, 1)
        else:
            current = datetime(year, month + 1, 1)
    return missing_files

def create_report_periods_table(data_folder):
    """Create report periods table and save as Parquet."""
    # Get min and max dates from data folder
    min_date, max_date = get_min_max_dates(data_folder)

    # add 12 months to min_date to ensure full R12 coverage
    if min_date is not None:
        min_date = min_date + timedelta(days=365)
    
    if min_date is None or max_date is None:
        raise ValueError("Could not determine min and max dates from data folder.")

    # Generate report periods DataFrame
    report_periods_df = generate_report_periods(min_date, max_date)

    return report_periods_df

def unzip_read_parquet(file_path):
    """Unzip and read a Parquet file into a Polars DataFrame and delete the unzipped file."""
    # unzip the file
    import zipfile
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(file_path))
    # get the extracted parquet file path
    parquet_file_path = file_path.replace('.zip', '.parquet')
    # read the parquet file into a Polars DataFrame
    df = pl.read_parquet(parquet_file_path)
    # delete the extracted parquet file
    os.remove(parquet_file_path)
    # return the DataFrame
    return df

def get_report_periods_data(data_folder, report_start_date, report_end_date, report_type):
    """Retrieve report periods data files and combine files, for given date range and type."""
    # list of files to read
    files_to_read = []
    current = report_start_date
    # iterate through days from report_start_date to report_end_date
    while current <= report_end_date:
        # example file name: silver_2026-01-14.zip
        file_name = f"silver_{current.strftime('%Y-%m-%d')}.zip"
        file_path = os.path.join(data_folder, file_name)
        if os.path.exists(file_path):
            files_to_read.append(file_path)
        current = current + timedelta(days=1)
    # Combine files into a single DataFrame
    if not files_to_read:
        raise ValueError("No files found for the specified date range and report type.")
    for i, file in enumerate(files_to_read):
        df = unzip_read_parquet(file)
        if i == 0:
            report_df = df
        else:
            try:
                report_df = pl.concat([report_df, df])
            except Exception as e:
                print(f"Error concatenating {os.path.basename(file)}: \n{e}")
    # create report_type, report_start_date, report_end_date columns
    report_start_date = report_start_date.strftime('%Y_%m')
    report_end_date = report_end_date.strftime('%Y_%m')
    report_df = report_df\
        .with_columns(
            pl.lit(report_type).alias("report_type")
            , pl.lit(report_start_date).alias("report_start_date")
            , pl.lit(report_end_date).alias("report_end_date")            
        )
    return report_df
def zip_parquet_file(df, output_path):
    """Save Polars DataFrame as Parquet and zip the file."""
    parquet_file_path = output_path.replace('.zip', '.parquet')
    df.write_parquet(parquet_file_path)
    import zipfile
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(parquet_file_path, os.path.basename(parquet_file_path))
    os.remove(parquet_file_path)

def calculate_elapsed_delta_time(idx, start_time, previous_time=None):
    """Calculate and print elapsed and delta time for processing."""
    # get current time
    current_time = datetime.now()

    # calculate elapsed time
    elapsed_time = current_time - start_time 
    elapsed_time = str(elapsed_time).split(".")[0]
    
    # calculate delta time if previous_time is provided
    if previous_time:  
        # calculate delta time
        delta_time = current_time - previous_time
        delta_time = str(delta_time).split(".")[0]
        print(f"Processing row {idx}, elapsed time: {elapsed_time}, delta time: {delta_time}")
    else:
        print(f"Processing row {idx}, elapsed time: {elapsed_time}, delta time: N/A")

    # update previous_time
    previous_time = datetime.now() 
    return previous_time
def create_report_periods_df(report_periods_df, data_folder, output_folder=None):
    """Enumerate each report period and report type and generate report periods DataFrame."""
    start_time = datetime.now()
    previous_time = None
    for idx, row in enumerate(report_periods_df.iter_rows(named=True)):        
        if idx % 50 == 0:
            previous_time = calculate_elapsed_delta_time(idx, start_time, previous_time)
            

        report_type = row["report_type"]
        report_date = row["report_date"]
        start_date = row["start_date"]
        end_date = row["end_date"]

        report_df = get_report_periods_data(
            data_folder=data_folder,
            report_start_date=start_date,
            report_end_date=end_date,
            report_type=report_type
        )
        # drop 'location' columns if exist since it's struct type and causes issues in saving
        if 'location' in report_df.columns:
            report_df = report_df.drop('location')
        
        # create output file name
        start_date = start_date.strftime('%Y_%m')
        end_date = end_date.strftime('%Y_%m')
        file_name = f"silver_{report_type.replace(' ', '_').lower()}_{start_date}_{end_date}.zip"
        
        # determine output folder
        if output_folder is None:
            # create output folder if not exists
            # parent of data_folder
            parent_folder = os.path.dirname(data_folder)
            output_folder = os.path.join(parent_folder, "silver_report_period_crime_data")
            os.makedirs(output_folder, exist_ok=True)
        if idx == 5:
            # save a sample of the report_df for inspection
            # sample file name
            sample_file_name = f"{report_type.replace(' ', '_').lower()}_sample_report_df.csv"
            sample_file_path = os.path.join(output_folder, sample_file_name)
            # save as csv
            report_df.write_csv(sample_file_path)
        output_file = os.path.join(output_folder, file_name)
        zip_parquet_file(report_df, output_file)
def main():
    # Define the data folder path
    data_folder = r"C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\silver_crime_data"
    # Create report periods table
    report_periods_df = create_report_periods_table(data_folder)
    # create report dataframes for each report period and type
    create_report_periods_df(report_periods_df, data_folder)
if __name__ == "__main__":
    main()
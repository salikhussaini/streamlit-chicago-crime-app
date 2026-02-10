import os
import pandas as pd
from datetime import datetime, timedelta

# Utility function to parse dates from filenames
def parse_date_from_filename(file, prefix):
    try:
        if prefix in file:
            date_str = file.split(".")[0]
            date_str = date_str.replace(prefix, "").replace("_", "-")
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        pass
    return None

# Function to get all dates from filenames in a folder
def get_pulled_dates(folder_path, prefix):
    pulled_dates = set()
    for root, _, files in os.walk(folder_path):
        for file in files:
            date = parse_date_from_filename(file, prefix)
            if date:
                pulled_dates.add(date)
    return pulled_dates

# Utility function to parse report dates from filenames
def parse_report_dates(file, prefix):
    """
    Parse report type, start date, and end date from a filename.

    Args:
        file (str): The filename to parse.
        prefix (str): The prefix to identify the file type (e.g., "gold_" or "silver_").

    Returns:
        tuple: (report_type, start_date, end_date) if parsing is successful, otherwise None.
    """
    try:
        if prefix in file:
            # Remove the prefix and file extension
            date_str = file.replace(prefix, "").split(".")[0]

            # Split the remaining string by underscores
            split_dates = date_str.split("_")

            # Extract report type, start date, and end date
            report_type = split_dates[0].upper()  # e.g., "r12"

            # Handle different date formats
            if len(split_dates[1]) == 6:  # Format: YYYYMM
                report_start = datetime.strptime(split_dates[1], "%Y%m").strftime("%Y-%m-%d")
                report_end = datetime.strptime(split_dates[2], "%Y%m").strftime("%Y-%m-%d")
            elif len(split_dates[1]) == 4:  # Format: YYYY_MM
                report_start = datetime.strptime("_".join(split_dates[1:3]), "%Y_%m").strftime("%Y-%m-%d")
                report_end = datetime.strptime("_".join(split_dates[3:5]), "%Y_%m").strftime("%Y-%m-%d")
            else:
                return None
            # Convert report_start and report_end to string format
            report_start = datetime.strptime(report_start, "%Y-%m-%d")
            report_end = datetime.strptime(report_end, "%Y-%m-%d")
            return report_type, report_start, report_end
    except Exception as e:
        print(f"Error parsing file {file}: {e}")
        return None
    return None

# Function to get report pulled dates from filenames in a folder
def get_report_pulled_dates(folder_path, prefix):
    pulled_dates = set()
    for root, _, files in os.walk(folder_path):
        for file in files:
            report_data = parse_report_dates(file, prefix)
            if report_data:
                pulled_dates.add(report_data)
    # create DataFrame from pulled_dates
    pulled_dates_df = pd.DataFrame(list(pulled_dates), columns=["report_type", "start_date", "end_date"])
    return pulled_dates_df

# Function to generate report periods
def generate_report_periods(min_date, max_date):
    months = []
    current = min_date
    while current <= max_date:
        months.append(current)
        year = current.year
        month = current.month
        current = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    
    records = []
    for report_date in months:
        year = report_date.year
        if year < 2:  # Skip invalid prior years
            continue
        ytd_start = datetime(year, 1, 1)
        r12_end = report_date
        r12_start = r12_end - timedelta(days=365)
        records.extend([
            {"report_type": "R12", "report_date": report_date, "report_date_yyyymm": int(f"{report_date.year}{str(report_date.month).zfill(2)}"),
             "start_date": r12_start, "end_date": r12_end},
            {"report_type": "YTD", "report_date": report_date, "report_date_yyyymm": int(f"{report_date.year}{str(report_date.month).zfill(2)}"),
             "start_date": ytd_start, "end_date": report_date}
        ])
    df = pd.DataFrame(records)
    # ensure start_date and end_date are in datetime format
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    return df

# Function to create a DataFrame for data status
def create_data_status_df(all_dates, api_dates, silver_dates):
    return pd.DataFrame({
        "date": all_dates,
        "raw_data_status": [1 if date in api_dates else 0 for date in all_dates],
        "silver_data_status": [1 if date in silver_dates else 0 for date in all_dates]
    })

def create_report_date_status_df(all_report_dates, silver_report_dates, gold_report_dates):
    """
    Create a DataFrame for report date statuses.
    """
    silver_report_dates['silver_status'] = 1
    gold_report_dates['gold_status'] = 1
    # Merge all_report_dates with silver_report_dates to get silver_status
    merged_df = all_report_dates.merge(
        silver_report_dates,
        on=["report_type", "start_date", "end_date"],
        how="left"
    )

    # Merge the result with gold_report_dates to get gold_status
    merged_df = merged_df.merge(
        gold_report_dates,
        on=["report_type", "start_date", "end_date"],
        how="left"
    )

    # Fill NaN values in silver_status and gold_status with 0
    merged_df["silver_status"] = merged_df["silver_status"].fillna(0).astype(int)
    merged_df["gold_status"] = merged_df["gold_status"].fillna(0).astype(int)

    return merged_df

# Main function to orchestrate the workflow
def main():
    # Define the folder paths
    api_folder = r"c:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\api_crime_data"
    silver_folder = r"c:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\silver_crime_data"
    silver_report_folder = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\silver_report_period_crime_data'
    gold_report_folder = r'C:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\gold_data'

    # Define the start and end dates
    start_date = datetime(2001, 1, 1)
    end_date = datetime.now()

    # Generate all dates from start_date to end_date
    all_dates = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()

    # Generate report periods
    report_periods = generate_report_periods(start_date, end_date)

    # Get pulled dates from folders
    api_dates = get_pulled_dates(api_folder, "crime_data_")
    silver_dates = get_pulled_dates(silver_folder, "silver_")
    silver_report_dates = get_report_pulled_dates(silver_report_folder, "silver_")
    gold_report_dates = get_report_pulled_dates(gold_report_folder, "gold_")

    print(silver_report_dates.head())
    # Create data status DataFrame
    data_status = create_data_status_df(all_dates, api_dates, silver_dates)

    # Save the DataFrame to a CSV file
    output_csv_path = r"c:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\raw_data_status.csv"
    data_status.to_csv(output_csv_path, index=False)

    report_date_status = create_report_date_status_df(report_periods, silver_report_dates, gold_report_dates)
    output_report_csv_path = r"c:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app\data\raw_data\report_date_status.csv"
    report_date_status.to_csv(output_report_csv_path, index=False)

# Entry point
if __name__ == "__main__":
    main()
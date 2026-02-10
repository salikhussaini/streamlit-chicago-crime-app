
# import 
import requests
import pandas as pd
import zipfile
import os
import time

# Define the base directory for the project
BASE_DIR = os.getenv("BASE_DIR", r"c:\Users\salik\Documents\PROJECTS\20260131_chicago_crimes\streamlit-chicago-crime-app")

# Define the path to store raw API crime data
os_path = os.path.join(BASE_DIR, "data", "raw_data", "api_crime_data")

# Define the path to the raw data status CSV file
raw_data_status_path = os.path.join(BASE_DIR, "data", "raw_data", "raw_data_status.csv")

# Function to fetch crime data for a specific day
def get_crime_data_by_day(date):
    """

    """
    API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
    params = {
        "$limit": 90000000,
        "$where": f"date between '{date}T00:00:00' and '{date}T23:59:59.999'"
    }

    try:
        # Make a GET request to the API
        response = requests.get(API_URL, params=params)
        # Raise an error for HTTP issues
        response.raise_for_status()

        # Handle rate limiting by retrying after a delay
        if response.status_code == 429:
            print("Rate limit exceeded. Retrying after a delay...")
            time.sleep(60)  # Wait for 60 seconds before retrying
        else:
            df = pd.DataFrame(response.json())
    except Exception as e:
        print(f"Error fetching data for date {date}: {e}")
        df = pd.DataFrame()
    return df

# Function to export crime data for a specific day
def export_crime_data_by_day(df, date):
    if df.empty:
        print(f"No data available for {date}")
        return

    # Define file paths
    zip_file_name = f"{os_path}/crime_data_{date}.zip"
    parquet_file = f"{os_path}/crime_data_{date}.parquet"

    # Export the dataframe to a parquet file and compress it into a zip file
    try:
        df.to_parquet(parquet_file)
        with zipfile.ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(parquet_file, arcname=os.path.basename(parquet_file))  # Use only the file name for arcname
        os.remove(parquet_file)  # Remove the parquet file after adding it to the zip
        print(f"Successfully exported data for {date}")
    except Exception as e:
        print(f"Error exporting data for {date}: {e}")

def old():
    # Function to fetch crime data for the specified year
    def get_crime_data_by_year(year):
        API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
        params = {"$limit": 90000000,
                "$where": f"year = {year}"          
        }

        try:
            response = requests.get(API_URL, params=params)
            df = pd.DataFrame(response.json())
        
        except Exception as e:
            print(f"Error fetching data for year {year}: {e}")
            df = pd.DataFrame()
        return df
    # for every day in the year export by day as a parquet file
    def export_crime_data_by_day_old(df, year):
        for day in pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31"):
            #2023-01-03T16:44:00.000
            # create a new column with datetime objects
            df['date_pd'] = pd.to_datetime(df['date'].str[:10], format='%Y-%m-%d')
            # filter the dataframe for the current day
            df_day = df[df['date_pd'].dt.date == day.date()]
            if df_day.empty:
                continue
            # drop the temporary datetime column
            df_day = df_day.drop(columns=['date_pd'])
            
            # Define file paths
            zip_file_name = f"{os_path}/crime_data_{day.strftime('%Y-%m-%d')}.zip"
            parquet_file = f"{os_path}/crime_data_{day.strftime('%Y-%m-%d')}.parquet"

            # Export the dataframe to a parquet file and compress it into a zip file
            try:
                df_day.to_parquet(parquet_file)
                with zipfile.ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(parquet_file, arcname=os.path.basename(parquet_file))  # Use only the file name for arcname
                os.remove(parquet_file)  # Remove the parquet file after adding it to the zip
            except Exception as e:
                print(f"Error exporting data for {day.strftime('%Y-%m-%d')}: {e}")

    def manual_run():
        for year in range(2026, 2027):
            df = get_crime_data_by_year(year)
            start_time = pd.Timestamp.now()
            export_crime_data_by_day_old(df, year)
            end_time = pd.Timestamp.now()
            print(f"Finished exporting crime data for year {year} in {end_time - start_time}")
            del df

# Main function to process data for specific dates
def main():
    # Read the raw_data_status.csv file to get the list of dates to process
    raw_data_status_df = pd.read_csv(raw_data_status_path)

    # Filter dates where raw_data_status is 0 (indicating data needs to be processed)
    dates_to_process = raw_data_status_df[raw_data_status_df['raw_data_status'] == 0]['date'].tolist()

    # Process data for each date in the list
    for date in dates_to_process:
        print(f"Processing data for {date}")
        df = get_crime_data_by_day(date)  # Fetch crime data for the date
        export_crime_data_by_day(df, date)  # Export the data for the date

# Entry point of the script
if __name__ == "__main__":
    main()
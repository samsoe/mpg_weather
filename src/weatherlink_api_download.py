"""
Script to download weather data from WeatherLink API.
This file will contain code for interacting with the WeatherLink API
and downloading weather data.
"""

import collections
import hashlib
import hmac
import time
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen
import json
import pandas as pd
from pathlib import Path

# import weatherlink_config
import pandas as pd

# import pandas_gbq
from google.oauth2 import service_account

# Placeholder for your notebook code


def process_station_response(response, station_id):
    """
    Process a single station's API response into a DataFrame.

    Args:
        response (dict): The API response from a weather station
        station_id (int): The station ID to include in the DataFrame

    Returns:
        pandas.DataFrame: Processed weather data
    """
    try:
        # Extract sensors data
        sensors_data = response.get("sensors", [])
        all_data = []

        for sensor in sensors_data:
            data = sensor.get("data", [])
            if data:
                # Flatten each data point and add station_id
                for point in data:
                    point["station_id"] = station_id
                    point["sensor_type"] = sensor.get("sensor_type")
                    all_data.append(point)

        # Create DataFrame from all data points
        if all_data:
            df = pd.DataFrame(all_data)
            return df
        return None
    except Exception as e:
        print(f"Error processing data for station {station_id}: {str(e)}")
        return None


def collect_station_data(selected_datetime):
    """
    Collect weather data from multiple WeatherLink stations at a specified datetime.

    Args:
        selected_datetime: The datetime for which to collect weather data

    Returns:
        pandas.DataFrame: Concatenated DataFrame containing data from all stations
    """
    # WeatherLink Station IDs
    station_ids = [25921, 25917, 25931, 25945, 30931, 30942]

    all_stations_data = []

    for station_id in station_ids:
        try:
            api_response = call_api(station_id, selected_datetime)
            df = process_station_response(api_response, station_id)
            if df is not None:
                all_stations_data.append(df)
        except Exception as e:
            print(f"Error processing station {station_id}: {str(e)}")
            continue

    if all_stations_data:
        # Concatenate all DataFrames
        return pd.concat(all_stations_data, ignore_index=True)
    else:
        print("No data collected from any station")
        return None


def call_api(id, selected_datetime):
    """
    Call the WeatherLink API for a specific station ID and datetime.

    Args:
        id (int): The WeatherLink station ID
        selected_datetime: The datetime for which to retrieve data

    Returns:
        dict: Parsed JSON response from the WeatherLink API

    Raises:
        URLError: If there's an error connecting to the API
        JSONDecodeError: If the response cannot be parsed as JSON
    """
    try:
        f = urlopen(generate_url(id, selected_datetime))
        json_string = f.read()
        parsed_json = json.loads(json_string)
        return parsed_json
    except Exception as e:
        print(f"Error calling API for station {id}: {str(e)}")
        raise


def generate_url(station_id, selected_datetime):
    """
    Generate a signed URL for the WeatherLink API historic data endpoint.

    Args:
        station_id (int): The WeatherLink station ID
        selected_datetime (datetime): The base datetime for the data request

    Returns:
        str: Signed URL for the WeatherLink API request

    Notes:
        - Generates timestamps for a 24-hour period ending 1 minute before the selected datetime
        - Uses API credentials from weatherlink_config
        - Implements WeatherLink's API signature requirements
    """
    # Calculate time window
    selected_datetime = selected_datetime + timedelta(1)
    end_timestamp = selected_datetime - timedelta(minutes=1)
    start_timestamp = end_timestamp - timedelta(1)

    # Build parameters dictionary
    parameters = {
        "api-key": "lnuxvde51izg3tyxuylrkcew6xnoufcd",
        "end-timestamp": int(end_timestamp.timestamp()),
        "start-timestamp": int(start_timestamp.timestamp()),
        "station-id": station_id,
        "t": int(time.time()),
    }

    # Sort parameters for consistent signature generation
    parameters = collections.OrderedDict(sorted(parameters.items()))

    # Generate API signature
    apiSecret = "ksmanmc1koj79f61qqzmgnxkrewl7ttf"
    data = "".join(key + str(value) for key, value in parameters.items())

    apiSignature = hmac.new(
        apiSecret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    # Construct final URL
    url = "https://api.weatherlink.com/v2/historic/{station_id}?api-key={api_key}&t={t}&start-timestamp={start}&end-timestamp={end}&api-signature={signature}".format(
        station_id=parameters["station-id"],
        api_key=parameters["api-key"],
        t=parameters["t"],
        start=parameters["start-timestamp"],
        end=parameters["end-timestamp"],
        signature=apiSignature,
    )

    return url


def collect_date_range_data(start_date_str, end_date=None, batch_days=7):
    """
    Collect weather data for a range of dates and save in batches.

    Args:
        start_date_str (str): Start date in 'YYYY-MM-DD' format
        end_date (datetime, optional): End date. Defaults to yesterday.
        batch_days (int): Number of days to process before saving to CSV

    Returns:
        None
    """
    # Parse start date
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )

    # Set end date to yesterday if not provided
    if end_date is None:
        end_date = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

    # Create output directory
    output_dir = Path("data/interim")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "combined_weather_data.csv"

    # Initialize batch data
    batch_data = []
    total_records = 0
    batch_start_date = start_date

    current_date = start_date
    while current_date <= end_date:
        print(f"Collecting data for: {current_date}")

        try:
            df = collect_station_data(current_date)
            if df is not None:
                batch_data.append(df)
                records = len(df)
                total_records += records
                print(f"Successfully collected {records} records")
            else:
                print(f"No data collected for {current_date}")
        except Exception as e:
            print(f"Error collecting data for {current_date}: {str(e)}")

        # Move to next date
        current_date += timedelta(days=1)

        # Check if we should save the batch
        days_processed = (current_date - batch_start_date).days
        end_of_range = current_date > end_date

        if days_processed >= batch_days or end_of_range:
            if batch_data:
                # Combine batch data
                batch_df = pd.concat(batch_data, ignore_index=True)

                # Append to CSV file
                mode = "a" if output_file.exists() else "w"
                header = not output_file.exists()
                batch_df.to_csv(output_file, mode=mode, header=header, index=False)

                print(
                    f"Saved batch from {batch_start_date.date()} to {(current_date - timedelta(days=1)).date()}"
                )
                print(f"Batch records: {len(batch_df)}")
                print(f"Total records so far: {total_records}")

                # Clear batch data to free memory
                batch_data = []
                batch_start_date = current_date

        # Add delay to avoid hitting API rate limits
        time.sleep(1)

    print(f"Data collection complete. Total records: {total_records}")


if __name__ == "__main__":
    # Start date is 2024-10-27
    start_date = "2024-10-27"

    # Collect data from start date until yesterday, saving in weekly batches
    collect_date_range_data(start_date, batch_days=7)

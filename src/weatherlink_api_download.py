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
import weatherlink_config
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

# Placeholder for your notebook code


def collect_station_data(selected_datetime):
    """
    Collect weather data from multiple WeatherLink stations at a specified datetime.

    Args:
        selected_datetime: The datetime for which to collect weather data

    Returns:
        list: A list of API responses from each weather station

    Note:
        Station IDs are hardcoded for specific weather stations:
        - 25921, 25917, 25931, 25945, 30931, 30942
    """
    # WeatherLink Station IDs
    station_ids = [25921, 25917, 25931, 25945, 30931, 30942]

    station_responses = []

    for id in station_ids:
        api_response = call_api(id, selected_datetime)
        station_responses.append(api_response)

    return station_responses


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
        "api-key": weatherlink_config.api_key,
        "end-timestamp": int(end_timestamp.timestamp()),
        "start-timestamp": int(start_timestamp.timestamp()),
        "station-id": station_id,
        "t": int(time.time()),
    }

    # Sort parameters for consistent signature generation
    parameters = collections.OrderedDict(sorted(parameters.items()))

    # Generate API signature
    apiSecret = weatherlink_config.api_secret
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

    print(url)  # Consider replacing with logging
    return url

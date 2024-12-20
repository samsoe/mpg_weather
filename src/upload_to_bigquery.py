#!/usr/bin/env python3
"""
Script to upload weather data from CSV to BigQuery and create a backup.
The script:
1. Uploads the CSV data to BigQuery table weather.weatherlink-api-update
2. Creates a backup of the table in GCS bucket mpg-data-warehouse-backups/backups/weather
"""

import pandas as pd
import pandas_gbq
from pathlib import Path
import sys
import subprocess
from datetime import datetime

# BigQuery settings
PROJECT_ID = "mpg-data-warehouse"
DATASET_TABLE = "weather.weatherlink-api-update"
BACKUP_BUCKET = "gs://mpg-data-warehouse-backups/backups/weather"

# Data type conversion dictionary
CONVERT_DICT = {
    "ts": "int",
    "arch_int": "int",
    "rev_type": "int",
    "temp_out": "float64",
    "temp_out_hi": "float64",
    "temp_out_lo": "float64",
    "temp_in": "float64",
    "hum_in": "float64",
    "hum_out": "float64",
    "rainfall_in": "float64",
    "rainfall_clicks": "str",
    "rainfall_mm": "str",
    "rain_rate_hi_in": "float64",
    "rain_rate_hi_clicks": "str",
    "rain_rate_hi_mm": "str",
    "et": "float64",
    "bar": "float64",
    "solar_rad_avg": "float64",
    "solar_rad_hi": "float64",
    "uv_index_avg": "float64",
    "uv_index_hi": "float64",
    "wind_num_samples": "int",
    "wind_speed_avg": "float64",
    "wind_speed_hi": "int",
    "wind_dir_of_hi": "float64",
    "wind_dir_of_prevail": "float64",
    "moist_soil_1": "str",
    "moist_soil_2": "str",
    "moist_soil_3": "str",
    "moist_soil_4": "str",
    "temp_soil_1": "str",
    "temp_soil_2": "str",
    "temp_soil_3": "str",
    "temp_soil_4": "str",
    "wet_leaf_1": "str",
    "wet_leaf_2": "str",
    "temp_leaf_1": "str",
    "temp_leaf_2": "str",
    "temp_extra_1": "str",
    "temp_extra_2": "str",
    "temp_extra_3": "str",
    "hum_extra_1": "str",
    "hum_extra_2": "str",
    "forecast_rule": "str",
    "forecast_desc": "str",
    "abs_press": "float64",
    "bar_noaa": "float64",
    "bar_alt": "float64",
    "air_density": "str",
    "dew_point_out": "float64",
    "dew_point_in": "float64",
    "emc": "float64",
    "heat_index_out": "float64",
    "heat_index_in": "float64",
    "wind_chill": "float64",
    "wind_run": "float64",
    "deg_days_heat": "float64",
    "deg_days_cool": "float64",
    "solar_energy": "float64",
    "uv_dose": "float64",
    "thw_index": "float64",
    "thsw_index": "float64",
    "wet_bulb": "float64",
    "night_cloud_cover": "float64",
    "iss_reception": "str",
    "station": "int",  # Station ID column
}


def backup_bigquery_table():
    """
    Create a backup of the BigQuery table in Google Cloud Storage.
    Uses the bq command-line tool to extract the table to GCS.
    Skips backup if one already exists for today.
    """
    try:
        # Generate backup filename with timestamp (just date for checking)
        today = datetime.now().strftime("%Y%m%d")

        # Check if backup exists for today
        cmd_ls = [
            "gsutil",
            "ls",
            f"{BACKUP_BUCKET}/weatherlink_api_update_{today}*.csv",
        ]

        result = subprocess.run(cmd_ls, capture_output=True, text=True)

        if result.returncode == 0 and result.stdout.strip():
            print(f"Backup already exists for today ({today}). Skipping backup.")
            return

        # If no backup exists, create one with full timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{BACKUP_BUCKET}/weatherlink_api_update_{timestamp}.csv"

        # Construct and execute bq extract command
        cmd = [
            "bq",
            "extract",
            "--project_id",
            PROJECT_ID,
            "--format=csv",
            "--compression=NONE",
            f"{DATASET_TABLE}",
            backup_path,
        ]

        print(f"Creating backup at: {backup_path}")
        subprocess.run(cmd, check=True)
        print("Backup completed successfully")

    except subprocess.CalledProcessError as e:
        print(f"Error creating backup: {str(e)}")
        sys.exit(1)


def process_and_upload_data(csv_path, batch_size=10000):
    """
    Process CSV data in batches and upload to BigQuery.

    Args:
        csv_path (str): Path to the CSV file
        batch_size (int): Number of rows to process in each batch
    """
    try:
        # Check if file exists
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Process data in chunks
        chunk_iterator = pd.read_csv(csv_path, chunksize=batch_size)
        total_rows = 0

        for chunk_number, chunk in enumerate(chunk_iterator, 1):
            print(f"Processing batch {chunk_number}...")

            # Remove unwanted columns
            columns_to_drop = ["sensor_type", "tz_offset"]
            chunk = chunk.drop(
                columns=[col for col in columns_to_drop if col in chunk.columns],
                errors="ignore",
            )

            # Rename station_id to station if it exists
            if "station_id" in chunk.columns:
                chunk = chunk.rename(columns={"station_id": "station"})

            # Convert data types
            for column, dtype in CONVERT_DICT.items():
                if column in chunk.columns:
                    try:
                        # Convert to specified type while preserving NA as None/NULL
                        chunk[column] = pd.to_numeric(
                            chunk[column], errors="coerce"
                        ).astype(dtype)
                    except Exception as e:
                        print(
                            f"Warning: Could not convert column {column} to {dtype}: {str(e)}"
                        )

            # Upload to BigQuery
            pandas_gbq.to_gbq(
                chunk, DATASET_TABLE, project_id=PROJECT_ID, if_exists="append"
            )

            total_rows += len(chunk)
            print(f"Uploaded {len(chunk)} rows (Total: {total_rows} rows)")

        print(f"Upload complete. Total rows uploaded: {total_rows}")

    except Exception as e:
        print(f"Error processing and uploading data: {str(e)}")
        sys.exit(1)


def main():
    # Path to the CSV file
    csv_path = "data/interim/combined_weather_data.csv"

    # Create backup of existing table
    print("Creating backup of existing table...")
    backup_bigquery_table()

    # Process and upload data
    print("\nUploading new data...")
    process_and_upload_data(csv_path)


if __name__ == "__main__":
    main()

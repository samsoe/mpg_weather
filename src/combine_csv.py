import pandas as pd
import glob
import chardet
import os


def combine_csv_files():
    # Define the columns we want to keep
    columns_to_keep = [
        "Date & Time",
        "Barometer - in Hg",
        "Inside Temp - °F",
        "Inside Hum - %",
        "Inside Dew Point - °F",
        "Inside Heat Index - °F",
        "Inside EMC",
        "Temp - °F",
        "High Temp - °F",
        "Low Temp - °F",
        "Hum - %",
        "Dew Point - °F",
        "Wet Bulb - °F",
        "Wind Speed - mph",
        "Wind Direction",
        "Wind Run - mi",
        "High Wind Speed - mph",
        "High Wind Direction",
        "Wind Chill - °F",
        "Heat Index - °F",
        "THW Index - °F",
        "THSW Index - °F",
        "Rain - in",
        "Rain Rate - in/h",
        "Solar Rad - W/m^2",
        "Solar Energy - Ly",
        "High Solar Rad - W/m^2",
        "ET - in",
        "UV Index",
        "UV Dose - MEDs",
        "High UV Index",
        "Heating Degree Days",
        "Cooling Degree Days",
    ]

    # Get list of CSV files
    csv_files = glob.glob("data/external/*.csv")
    print(f"Found files: {csv_files}")

    if not csv_files:
        raise Exception(
            "No CSV files found in data/external directory. Please check the path and file extensions."
        )

    # Read and process each CSV
    all_data = []
    for file in csv_files:
        print(f"\nProcessing: {file}")
        station_name = os.path.basename(file).split("_")[0].replace("_", " ")

        try:
            # Detect file encoding
            with open(file, "rb") as raw_file:
                result = chardet.detect(raw_file.read())

            # Read the CSV with detected encoding
            df = pd.read_csv(
                file,
                encoding=result["encoding"] or "latin1",
                usecols=columns_to_keep,
                low_memory=False,
            )

            # Add station identifier
            df["Station"] = station_name
            all_data.append(df)
            print(f"Shape after cleaning: {df.shape}")

        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
            continue

    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\nFinal combined shape: {combined_df.shape}")
        print(f"Columns in combined data: {', '.join(combined_df.columns)}")

        # Create interim directory if it doesn't exist
        os.makedirs("data/interim", exist_ok=True)

        # Save to interim directory
        output_path = "data/interim/combined_weather_data.csv"
        combined_df.to_csv(output_path, index=False)
        print(f"\nSaved combined data to: {output_path}")
    else:
        print("No data was successfully processed")


if __name__ == "__main__":
    combine_csv_files()

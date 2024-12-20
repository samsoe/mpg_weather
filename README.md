# Weather Data Update Project

## Overview
This project aims to identify the last available date in the BigQuery table `mpg-data-warehouse.weather_summaries.mpg_ranch` and update it with new available weather data. The most recent data available is from November 13, 2023.

## Documentation
Current documentation can be found [here](https://docs.google.com/document/d/1WKzE0v4DiwlfKYjMvTEVgzp_-_6jAv4l4CAtqqy-aII/edit?usp=sharing).

## Project Goals
- Determine the last date of weather data in the existing BigQuery table (currently 2023-11-13)
- Identify new weather data available since the last update
- Process and load new weather data into the table
- Maintain data consistency and quality

## Table Information
**Dataset:** mpg-data-warehouse
**Table:** weather_summaries.mpg_ranch

## Getting Started
[This section will be populated with setup instructions and prerequisites]

## Usage
[This section will be populated with instructions on how to run the update process]

## Contributing
[This section will be populated with contribution guidelines]

## License
[License information to be added]

## Data Processing Workflow
1. Data Collection
   - Start date: 2023-11-12 (one day before last available date)
   - Download data from all weather stations
   - Ensure complete 24-hour periods for data consistency

2. Data Integration
   - Combine datasets from multiple weather stations
   - Standardize data formats and units
   - Perform quality checks on merged data

3. BigQuery Upload
   - Validate data structure matches existing table schema
   - Upload consolidated dataset to `mpg-data-warehouse.weather_summaries.mpg_ranch`
   - Verify successful data integration with no duplicates

## Implementation Notes
- Using one day overlap (2023-11-12) to ensure data continuity
- Process includes deduplication to handle the overlap day
- Quality checks will be performed before and after upload 

## Directory Structure
### data/external/
- Location for manually downloaded weather station data
- Files should cover period from 2023-11-12 onwards
- Raw data files will be processed from this location

## Data Files
1. Place downloaded weather station files in `data/external/`
2. Ensure files include the overlap day (2023-11-12)
3. Files will be processed and combined before BigQuery upload
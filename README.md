# FlightRadar ELT pipeline

## About the project
This projects implements an ELT (Extract, Load, Transform) pipeline using the FlightRadar24 API which exposes real-time flights data, airports and airlines data.

The goal of this project was to build a pipeline architecture able to provide answers to the following questions:
1. Which airline has the most live flights?
2. Which live flight has the longest route?
3. Which aircraft manufacturer has the most live flights?
4. For each airline, what are the 3 most used aircraft models?

## Architecture

### Extract

Flights data is fetched using the [FlightRadarAPI package](https://github.com/JeanExtreme002/FlightRadarAPI/tree/main/python): a first call to the API allows to retrieve 5,000 live flights maximum. Then a second call to a different url, allows to get more details on a specific flight. Since the API allows to fetch live flights in a specific geographical area by passing bounds (4 lat, lon points) as an argument, it is possible to overcome the limit of 5,000 by iterating over a list of bounds. The list of bounds is obtained by binning the latitude and longitude ranges (e.g. 10° bins for latitude and 30° bins for longitude).

Responses from the API are saved in a timestamped json file into a "date-partitioned" GCS bucket `gs://DATA_BUCKET_NAME/bronze/tech_year=YYYY/tech_month=MM/tech_day=DD/tech_hour=HH/flights_YYYYMMDDHHMMSSmmm.json`

### Load

Prior to be loaded into BigQuery, bronze data goes through a normalization step: they go from a nested dictionary structure to a flat dictionary structure.

They are then parsed into a PySpark DataFrame, partitioned by tech_year, tech_month, tech_day, tech_hour and saved as a Parquet file in a GCS bucket `gs://DATA_BUCKET_NAME/silver/flights.parquet`.

That same DataFrame is uploaded into a BigQuery table `flights`.

### Transform

Transformations are done directly in BigQuery using dbt models.

## Usage

1. Install the package
```pip3 install git+https://github.com/oli2v/flight-radar.git@migrate-to-gcp```

2. Define environment variables in a .env file then run `source .env`

.env file
```
PROJECT_ID="flight-radar"
DATA_BUCKET_NAME="flight-radar-bucket"
BQ_DATASET_NAME="flight_radar_bucket"
```

3. Run the pipeline

`python3 run.py`

## Aknowledgment

- [FlightRadar24 website](https://www.flightradar24.com/)
- [FlightRadarAPI](https://github.com/JeanExtreme002/FlightRadarAPI/tree/main/python)

## Author

[Olivier Valenduc](https://github.com/oli2v)
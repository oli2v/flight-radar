# FlightRadar ETL/ELT pipeline
## About the project
This projects implements both an ETL (Extract, Transform, Load) and an ELT (Extract, Load, Transform) pipeline using FlightRadar24 API which exposes real-time flights data.

The goal of this project was to build a pipeline architecture able to provide answers to the following questions:
1. Which airline has the most live flights?
2. Which live flight has the longest route?
3. Which aircraft manufacturer has the most live flights?
4. For each airline, what are the 3 most used aircraft models?

## Architecture
You can either choose to implement the ETL version of the pipeline or the ELT version. The ETL version is an "on premise" implementation whereas the ELT one is based on Google Cloud Platform services (mostly Storage and BigQuery).

### 1. On premise implementation (ETL)
#### Extract
Flights data is fetched using the [FlightRadarAPI package](https://github.com/JeanExtreme002/FlightRadarAPI/tree/main/python): a first call to the API allows to retrieve 5,000 live flights maximum. Then a second call to a different url, allows to get more details on a specific flight. Since the API allows to fetch live flights in a specific geographical area by passing bounds (4 lat, lon points) as an argument, it is possible to overcome the limit of 5,000 by iterating over a list of bounds. The list of bounds is obtained by binning the latitude and longitude ranges (e.g. 10° bins for latitude and 30° bins for longitude).

Responses from the API are saved in a timestamped json file into `flight_radar/src/data/bronze/tech_year=YYYY/tech_month=MM/tech_day=DD/tech_hour=HH/flights_YYYYMMDDHHMMSSmmm.json`.

#### Transform
Raw data go through a normalization step: they go from a nested dictionary structure to a flat dictionary structure. They are then parsed into a PySpark DataFrame, partitioned by tech_year, tech_month, tech_day, tech_hour and saved as a Parquet file under `flight_radar/src/data/silver/flights.parquet`.

Main transformations are done at this stage locally using PySpark.

#### Load
Final results are stored as .csv files under `flight_radar/src/data/gold/tech_year=YYYY/tech_month=MM/tech_day=DD/tech_hour=HH/`.

The final folder structure looks like this:
```
.
└── data/
    ├── bronze/
    │   └── tech_year=2024/
    │       └── tech_month=02/
    │           └── tech_day=09/
    │               ├── tech_hour=10
    │               └── tech_hour=11/
    │                   └── flights_20240209110519852.json
    ├── silver/
    │   └── flights.parquet
    └── gold/
        └── tech_year=2024/
            └── tech_month=02/
                └── tech_day=09/
                    ├── tech_hour=10
                    └── tech_hour=11/
                        ├── live_flights_count_by_airline_20240209110519852.csv
                        ├── live_flights_by_distance_20240209110519852.csv
                        ├── flights_count_by_manufacturer_20240209110519852.csv
                        └── model_count_by_airline_20240209110519852.csv
```

### 2. GCP based (ELT)
#### Extract
This step is exactly the same as for the on premise implementation. The only difference is that responses from the API are saved in a "date-partitioned" GCS bucket `gs://DATA_BUCKET_NAME/bronze/tech_year=YYYY/tech_month=MM/tech_day=DD/tech_hour=HH/flights_YYYYMMDDHHMMSSmmm.json`

#### Load
Prior to being loaded into BigQuery, raw data also go through normalization. They are then parsed into a PySpark DataFrame, partitioned by tech_year, tech_month, tech_day, tech_hour and saved as a Parquet file in a GCS bucket `gs://DATA_BUCKET_NAME/silver/flights.parquet`. That same DataFrame is then uploaded into a BigQuery table called `flights`.

#### Transform
Main transformations are done directly in BigQuery using dbt models after the data is loaded.

## Usage
1. Install requirements
`pip3 install -r requirements.txt`

2. (Optional) If you want to run the GCP implementation, you first need to define environment variables in a .env file then run `source .env`, otherwise you can skip to step 3.

.env file
```
PROJECT_ID=your-project-id
DATA_BUCKET_NAME=your-data-bucket-name
BQ_DATASET_NAME=your_bigquery_dataset_name
GOOGLE_APPLICATION_CREDENTIALS=path-to-your-service-account-file.json
```

3. Run the pipeline

`python3 run.py`

## Aknowledgments

- [FlightRadar24 website](https://www.flightradar24.com/)
- [FlightRadarAPI](https://github.com/JeanExtreme002/FlightRadarAPI/tree/main/python)

## Author

[Olivier Valenduc](https://github.com/oli2v)
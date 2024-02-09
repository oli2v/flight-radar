# FlightRadar ETL pipeline

## About the project
This projects implements an ETL (Extract, Transform, Load) pipeline using the FlightRadar24 API which exposes real-time flights data, airports and airlines data.

The goal of this project was to build a pipeline architecture able to provide answers to the following questions:
1. Which airline has the most live flights?
2. Which live flight has the longest route?
3. Which aircraft manufacturer has the most live flights?
4. For each airline, what are the 3 most used aircraft models?

## Architecture

The pipeline architecture is a medallion architecure:
1. Raw data are extracted using the API and stored in a "bronze" folder
2. Raw data are then normalized and stored in a "silver" folder
3. Normalized data are then analyzed and stored in a "gold" folder

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

The extraction, processing and analysis are done in Python and PySpark in 2 Docker containers: a Spark master and one worker. Data is generated inside the worker container and volumes are binded between the container and the host machine.

## Usage

To start the containers, simply run `make start`. The containers should now be up and running.
To run the pipeline, execute `make run`.

If you want to shutdown the containers, run `make shutdown`. If you want to remove all the containers, the volumes and the images, run `make remove`.

## Results

The pipeline outputs 4 .csv files in the "gold" folder:

```
.
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

Each file corresponds to 1 of the 4 questions.

## Aknowledgment

- [FlightRadar24 website](https://www.flightradar24.com/)
- [FlightRadarAPI](https://github.com/JeanExtreme002/FlightRadarAPI/tree/main/python)
- [docker-spark-airflow](https://github.com/yTek01/docker-spark-airflow/tree/main)

## Author

[Olivier Valenduc](https://github.com/oli2v)
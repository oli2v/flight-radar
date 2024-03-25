import logging
from datetime import datetime

from pyspark.sql import DataFrame
from FlightRadar24 import FlightRadar24API

from google.cloud import storage, bigquery

from .constants import (
    GCS_BUCKET_NAME,
    BQ_FLIGHTS_TABLE_ID,
    BQ_FLIGHTS_TABLE_NAME,
    PARTITION_BY_COL_LIST,
    LATITUDE_RANGE,
    LONGITUDE_RANGE,
)
from .schema import FLIGHTS_SCHEMA
from .utils import (
    get_json_from_gcs,
    normalize_data,
    create_sdf_from_dict_list,
    write_sdf_to_gcs,
    load_parquet_to_bq,
    get_directory,
    get_raw_filename,
    init_spark,
    split_map,
)

from .extractor import FlightRadarExtractor


class FlightRadarPipeline:
    fr_api = FlightRadar24API()
    spark = init_spark("flight-radar-spark")
    bq_client = bigquery.Client()
    bucket = storage.Client().bucket(GCS_BUCKET_NAME)
    bounds_list = split_map(LATITUDE_RANGE, LONGITUDE_RANGE)
    bounds_rdd = spark.sparkContext.parallelize(bounds_list)

    def __init__(self):
        self.current_time = datetime.now()
        self.directory = get_directory(self.current_time)
        self.flight_raw_filename = get_raw_filename("flights", self.current_time)

    def init_extractor(self):
        extractor = FlightRadarExtractor(
            self.directory,
            self.flight_raw_filename,
            self.bounds_rdd,
        )
        return extractor

    def run(self) -> None:
        extractor = self.init_extractor()
        extractor.extract(self.fr_api, self.bucket)
        flights_sdf = self.transform()
        self.load(flights_sdf)

    def transform(self) -> None:
        logging.info("Processing data...")
        raw_flight_dict_list = get_json_from_gcs(
            self.bucket, self.directory, self.flight_raw_filename
        )
        normalized_flight_dict_list = normalize_data(raw_flight_dict_list)
        logging.info(
            "Normalized data about %d flights.", len(normalized_flight_dict_list)
        )
        flights_sdf = create_sdf_from_dict_list(
            self.spark,
            normalized_flight_dict_list,
            FLIGHTS_SCHEMA,
            self.current_time,
        )
        return flights_sdf

    def load(self, any_sdf: DataFrame) -> None:
        logging.info("Uploading flights data to BigQuery...")
        writing_uri = f"gs://{GCS_BUCKET_NAME}/silver/flights.parquet/{self.directory}"
        loading_uri = (
            f"gs://{GCS_BUCKET_NAME}/silver/flights.parquet/{self.directory}/"
            "*.parquet"
        )
        write_sdf_to_gcs(any_sdf, writing_uri, PARTITION_BY_COL_LIST)
        load_parquet_to_bq(loading_uri, self.bq_client, BQ_FLIGHTS_TABLE_ID)
        destination_table = self.bq_client.get_table(BQ_FLIGHTS_TABLE_ID)
        logging.info(
            "Loaded %d rows into BigQuery table %s.",
            destination_table.num_rows,
            BQ_FLIGHTS_TABLE_NAME,
        )

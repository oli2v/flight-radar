import os
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime

from pyspark.sql import DataFrame
from FlightRadar24 import FlightRadar24API
from google.cloud import bigquery

from .common.constants import (
    GCS_BUCKET_NAME,
    BQ_FLIGHTS_TABLE_ID,
    BQ_FLIGHTS_TABLE_NAME,
)
from .common.schema import FLIGHTS_SCHEMA
from .common.utils import (
    normalize_data,
    create_sdf_from_dict_list,
    load_parquet_to_bq,
    get_directory,
    init_spark,
)
from .analyze import analyze
from .extractor import FlightRadarExtractor
from .persistor import FlightRadarPersistor, GoogleFlightRadarPersistor


class FlightRadarPipeline:
    fr_api = FlightRadar24API()

    def __init__(self):
        self.current_time = datetime.now()
        self.directory = get_directory(self.current_time)
        self.persistor = FlightRadarPersistor(self.current_time, self.directory)
        self.spark = init_spark("flight-radar-spark")
        self.extractor = FlightRadarExtractor(self.spark)

    def run(self) -> None:
        extractor = self.extractor
        persistor = self.persistor
        raw_flight_dict_list = extractor.extract(self.fr_api)
        persistor.persist_raw_data(raw_flight_dict_list)
        flights_sdf = self.transform(raw_flight_dict_list)
        persistor.persist_processed_data(flights_sdf)
        self.load(flights_sdf)

    def transform(self, raw_flight_dict_list: List[Optional[Dict[Any, Any]]]) -> None:
        logging.info("Processing data...")
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
        filename_pdf_dict = analyze(any_sdf, self.current_time)
        if not os.path.exists(f"flight_radar/src/data/gold/{self.directory}"):
            os.makedirs(f"flight_radar/src/data/gold/{self.directory}")
        for filename, any_pdf in filename_pdf_dict.items():
            any_pdf.to_csv(
                f"flight_radar/src/data/gold/{self.directory}/{filename}.csv",
                index=False,
            )


class GoogleFlightRadarPipeline(FlightRadarPipeline):
    bq_client = bigquery.Client()

    def __init__(self, spark_config: Optional[Dict[str, str]] = None):
        super().__init__()
        self.persistor = GoogleFlightRadarPersistor(self.current_time, self.directory)
        self.spark = init_spark("flight-radar-spark-gcp", spark_config)

    def load(self, any_sdf: DataFrame) -> None:
        logging.info("Uploading flights data to BigQuery...")
        loading_uri = (
            f"gs://{GCS_BUCKET_NAME}/silver/flights.parquet/{self.directory}/*.parquet"
        )
        load_parquet_to_bq(loading_uri, self.bq_client, BQ_FLIGHTS_TABLE_ID)
        destination_table = self.bq_client.get_table(BQ_FLIGHTS_TABLE_ID)
        logging.info(
            "Loaded %d rows into BigQuery table %s.",
            destination_table.num_rows,
            BQ_FLIGHTS_TABLE_NAME,
        )

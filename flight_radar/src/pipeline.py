import json
import time
import logging
from random import randrange
from datetime import datetime
from typing import List, Dict, Optional, Any
import concurrent.futures
from requests.exceptions import HTTPError, SSLError
from tqdm import tqdm

from pyspark.sql import DataFrame
from FlightRadar24 import FlightRadar24API
from FlightRadar24.errors import CloudflareError
from FlightRadar24.api import Flight
from google.cloud import storage, bigquery


from .constants import (
    FLIGHTS_SCHEMA,
    LATITUDE_RANGE,
    LONGITUDE_RANGE,
    GCS_BUCKET_NAME,
    GOOGLE_PROJECT_NAME,
    BQ_DATASET_NAME,
    BQ_TABLE_NAME,
    PARTITION_BY_COL_LIST,
)
from .utils import (
    init_spark,
    merge_flights,
    split_map,
    get_json_from_gcs,
    normalize_data,
    create_sdf_from_dict_list,
    write_sdf_to_gcs,
    load_parquet_to_bq,
    upload_dict_list_to_gcs,
)


class FlightRadarPipeline:
    # pylint: disable=too-many-instance-attributes
    def __init__(self):
        self.fr_api = FlightRadar24API()
        self.spark = init_spark("flight-radar-spark")
        self.current_time = datetime.now()
        self.current_year = self.current_time.year
        self.current_month = self.current_time.month
        self.current_day = self.current_time.day
        self.current_hour = self.current_time.hour
        self.formatted_time = self.current_time.strftime("%Y%m%d%H%M%S%f")[:-3]
        self.raw_filename = f"flights_{self.formatted_time}.json"
        self.destination_blob_name = (
            f"tech_year={self.current_year}/tech_month={self.current_month}/"
            f"tech_day={self.current_day}/tech_hour={self.current_hour}"
        )
        storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.table_id = f"{GOOGLE_PROJECT_NAME}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}"
        self.bucket = storage_client.bucket(GCS_BUCKET_NAME)

    def run(self) -> None:
        self.extract()
        flights_sdf = self.transform()
        self.load(flights_sdf)

    def extract(self) -> None:
        logging.info("Fetching data from FlightRadar API...")
        flight_list = self._extract_flights()
        flight_dict_list = self._extract_flights_details(flight_list)
        upload_dict_list_to_gcs(
            self.bucket,
            json.dumps(flight_dict_list),
            f"bronze/{self.destination_blob_name}/{self.raw_filename}",
        )
        logging.info(
            "Uploaded %d flights to GCS bucket: %s.",
            len(flight_dict_list),
            GCS_BUCKET_NAME,
        )

    def transform(self) -> None:
        logging.info("Processing data...")
        raw_flight_dict_list = get_json_from_gcs(
            self.bucket, self.destination_blob_name, self.raw_filename
        )
        normalized_flight_dict_list = normalize_data(raw_flight_dict_list)
        logging.info(
            "Normalized data about %d flights.", len(normalized_flight_dict_list)
        )
        flights_sdf = create_sdf_from_dict_list(
            self.spark,
            normalized_flight_dict_list,
            FLIGHTS_SCHEMA,
            self.current_year,
            self.current_month,
            self.current_day,
            self.current_hour,
            self.current_time,
        )
        return flights_sdf

    def load(self, any_sdf: DataFrame) -> None:
        logging.info("Uploading flights data to BigQuery...")
        writing_uri = f"gs://{GCS_BUCKET_NAME}/silver/flights.parquet/{self.destination_blob_name}"
        loading_uri = (
            f"gs://{GCS_BUCKET_NAME}/silver/flights.parquet/{self.destination_blob_name}/"
            "*.parquet"
        )
        write_sdf_to_gcs(any_sdf, writing_uri, PARTITION_BY_COL_LIST)
        load_parquet_to_bq(loading_uri, self.bq_client, self.table_id)
        destination_table = self.bq_client.get_table(self.table_id)
        logging.info(
            "Loaded %d rows into BigQuery table %s.",
            destination_table.num_rows,
            BQ_TABLE_NAME,
        )

    def _extract_flights(self) -> List[Optional[Flight]]:
        bounds_list = split_map(LATITUDE_RANGE, LONGITUDE_RANGE)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_list = [
                executor.submit(self.fr_api.get_flights, **{"bounds": bounds})
                for bounds in bounds_list
            ]
            concurrent.futures.wait(future_list)
        flight_list = merge_flights(future_list)
        logging.info("Extracted %d flights.", len(flight_list))
        return flight_list

    def _extract_flights_details(
        self, flight_list: List[Flight]
    ) -> List[Optional[Dict[Any, Any]]]:
        flight_dict_list = []
        for flight in tqdm(flight_list):
            try:
                flight_details = self.fr_api.get_flight_details(flight)
                flight_dict_list.append(flight_details)
            except HTTPError:
                logging.warning("Exception occurred", exc_info=True)
            except (CloudflareError, SSLError):
                logging.warning("Exception occurred", exc_info=True)
                time.sleep(randrange(0, 1))
        logging.info("Extracted details about %d flights.", len(flight_dict_list))
        return flight_dict_list

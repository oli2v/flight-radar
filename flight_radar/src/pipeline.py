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
    GCS_BUCKET_NAME,
    BQ_FLIGHTS_TABLE_ID,
    BQ_FLIGHTS_TABLE_NAME,
    PARTITION_BY_COL_LIST,
    LATITUDE_RANGE,
    LONGITUDE_RANGE,
)
from .schema import FLIGHTS_SCHEMA
from .utils import (
    merge_flights,
    get_json_from_gcs,
    normalize_data,
    create_sdf_from_dict_list,
    write_sdf_to_gcs,
    load_parquet_to_bq,
    upload_dict_list_to_gcs,
    get_directory,
    get_raw_filename,
    init_spark,
    split_map,
)

fr_api = FlightRadar24API()
spark = init_spark("flight-radar-spark")
bq_client = bigquery.Client()
bucket = storage.Client().bucket(GCS_BUCKET_NAME)
bounds_list = split_map(LATITUDE_RANGE, LONGITUDE_RANGE)


class FlightRadarPipeline:
    # pylint: disable=too-many-instance-attributes
    def __init__(self):
        self.current_time = datetime.now()
        self.directory = get_directory(self.current_time)
        self.flight_raw_filename = get_raw_filename("flights", self.current_time)

    def run(self) -> None:
        self.extract()
        flights_sdf = self.transform()
        self.load(flights_sdf)

    def extract(self) -> None:
        logging.info("Fetching data from FlightRadar API...")
        flight_list = self._extract_flights()
        flight_dict_list = self._extract_flights_details(flight_list)

        upload_dict_list_to_gcs(
            bucket,
            json.dumps(flight_dict_list),
            f"bronze/{self.directory}/{self.flight_raw_filename}",
        )
        logging.info(
            "Uploaded %d flights to GCS bucket: %s.",
            len(flight_dict_list),
            GCS_BUCKET_NAME,
        )

    def transform(self) -> None:
        logging.info("Processing data...")
        raw_flight_dict_list = get_json_from_gcs(
            bucket, self.directory, self.flight_raw_filename
        )
        normalized_flight_dict_list = normalize_data(raw_flight_dict_list)
        logging.info(
            "Normalized data about %d flights.", len(normalized_flight_dict_list)
        )
        flights_sdf = create_sdf_from_dict_list(
            spark,
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
        load_parquet_to_bq(loading_uri, bq_client, BQ_FLIGHTS_TABLE_ID)
        destination_table = bq_client.get_table(BQ_FLIGHTS_TABLE_ID)
        logging.info(
            "Loaded %d rows into BigQuery table %s.",
            destination_table.num_rows,
            BQ_FLIGHTS_TABLE_NAME,
        )

    def _extract_flights(self) -> List[Optional[Flight]]:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_list = [
                executor.submit(fr_api.get_flights, **{"bounds": bounds})
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
                flight_details = fr_api.get_flight_details(flight)
                flight_dict_list.append(flight_details)
            except HTTPError:
                logging.warning("Exception occurred", exc_info=True)
            except (CloudflareError, SSLError):
                logging.warning("Exception occurred", exc_info=True)
                time.sleep(randrange(0, 1))
        logging.info("Extracted details about %d flights.", len(flight_dict_list))
        return flight_dict_list

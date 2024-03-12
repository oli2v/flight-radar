import json
import time
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
    NUM_FLIGHTS_TO_EXTRACT,
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

    def extract(self) -> None:
        flight_list = self._extract_flights()
        flight_dict_list = self._extract_flights_details(flight_list)
        upload_dict_list_to_gcs(
            self.bucket,
            json.dumps(flight_dict_list),
            f"bronze/{self.destination_blob_name}/{self.raw_filename}",
        )

    def transform(self) -> None:
        raw_flight_dict_list = get_json_from_gcs(
            self.bucket, self.destination_blob_name, self.raw_filename
        )
        normalized_flight_dict_list = normalize_data(raw_flight_dict_list)
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
        uri = (
            f"gs://{GCS_BUCKET_NAME}/silver/flights.parquet/{self.destination_blob_name}/"
            "*.parquet"
        )
        write_sdf_to_gcs(any_sdf, uri, PARTITION_BY_COL_LIST)
        load_parquet_to_bq(uri, self.bq_client, self.table_id)

    def _extract_flights(self) -> List[Optional[Flight]]:
        bounds_list = split_map(LATITUDE_RANGE, LONGITUDE_RANGE)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_list = [
                executor.submit(self.fr_api.get_flights, **{"bounds": bounds})
                for bounds in bounds_list
            ]
            concurrent.futures.wait(future_list)
        flight_list = merge_flights(future_list)
        return flight_list

    def _extract_flights_details(
        self, flight_list: List[Flight]
    ) -> List[Optional[Dict[Any:Any]]]:
        flight_dict_list = []
        for flight in tqdm(flight_list[:NUM_FLIGHTS_TO_EXTRACT]):
            try:
                flight_details = self.fr_api.get_flight_details(flight)
                flight_dict_list.append(flight_details)
            except HTTPError:
                pass
            except (CloudflareError, SSLError):
                time.sleep(randrange(0, 1))
        return flight_dict_list

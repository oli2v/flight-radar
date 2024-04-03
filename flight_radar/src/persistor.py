import os
import json
import logging
from typing import List, Dict, Optional, Any

from pyspark.sql import DataFrame
from google.cloud import storage

from .utils import get_raw_filename, upload_dict_list_to_gcs, write_sdf
from .constants import PARTITION_BY_COL_LIST, GCS_BUCKET_NAME


class FlightRadarPersistor:
    def __init__(self, current_time, directory):
        self.directory = directory
        self.flight_raw_filename = get_raw_filename("flights", current_time)

    def persist_raw_data(
        self, flight_dict_list: List[Optional[Dict[Any, Any]]]
    ) -> None:
        if not os.path.exists(f"flight_radar/src/data/bronze/{self.directory}"):
            os.makedirs(f"flight_radar/src/data/bronze/{self.directory}")
        with open(
            f"flight_radar/src/data/bronze/{self.directory}/{self.flight_raw_filename}",
            "w",
        ) as json_fp:
            json.dump(flight_dict_list, json_fp)

    def persist_processed_data(self, any_sdf: DataFrame) -> None:
        if not os.path.exists("flight_radar/src/data/silver"):
            os.makedirs("flight_radar/src/data/silver")
        writing_uri = "flight_radar/src/data/silver/flights.parquet"
        write_sdf(any_sdf, writing_uri, PARTITION_BY_COL_LIST)


class GoogleFlightRadarPersistor(FlightRadarPersistor):
    storage_client = storage.Client()

    def persist_raw_data(
        self, flight_dict_list: List[Optional[Dict[Any, Any]]]
    ) -> None:
        bucket = self.storage_client.get_bucket(GCS_BUCKET_NAME)
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

    def persist_processed_data(self, any_sdf: DataFrame) -> None:
        writing_uri = f"gs://{GCS_BUCKET_NAME}/silver/flights.parquet"
        write_sdf(any_sdf, writing_uri, PARTITION_BY_COL_LIST)

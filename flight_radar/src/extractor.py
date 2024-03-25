import time
import json
import logging
from random import randrange
from typing import List, Dict, Optional, Any
from tqdm import tqdm
from requests.exceptions import HTTPError, SSLError
from FlightRadar24 import FlightRadar24API
from FlightRadar24.api import Flight
from FlightRadar24.errors import CloudflareError
from google.cloud.storage.bucket import Bucket
from pyspark.rdd import RDD
from .utils import upload_dict_list_to_gcs
from .constants import GCS_BUCKET_NAME, MAX_NB_FLIGHTS


class FlightRadarExtractor:
    def __init__(
        self,
        directory: str,
        raw_filename: str,
        bounds_rdd: RDD,
    ):
        self.directory = directory
        self.raw_filename = raw_filename
        self.bounds_rdd = bounds_rdd

    def extract(self, fr_api: FlightRadar24API, bucket: Bucket) -> None:
        logging.info("Fetching data from FlightRadar API...")
        flight_list = self._extract_flights(fr_api)
        flight_dict_list = self._extract_flights_details(flight_list, fr_api)

        upload_dict_list_to_gcs(
            bucket,
            json.dumps(flight_dict_list),
            f"bronze/{self.directory}/{self.raw_filename}",
        )
        logging.info(
            "Uploaded %d flights to GCS bucket: %s.",
            len(flight_dict_list),
            GCS_BUCKET_NAME,
        )

    def _extract_flights(self, fr_api: FlightRadar24API) -> List[Optional[Flight]]:
        def _extract_flights_from_bounds(bounds):
            return fr_api.get_flights(bounds=bounds)

        flights_rdd = self.bounds_rdd.flatMap(_extract_flights_from_bounds)
        flight_list = flights_rdd.collect()
        return flight_list

    def _extract_flights_details(
        self, flight_list: List[Flight], fr_api: FlightRadar24API
    ) -> List[Optional[Dict[Any, Any]]]:
        flight_dict_list = []
        for flight in tqdm(flight_list[:MAX_NB_FLIGHTS]):
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

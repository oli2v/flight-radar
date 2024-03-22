import time
import json
import logging
from random import randrange
from typing import List, Dict, Optional, Any
from tqdm import tqdm
from requests.exceptions import HTTPError, SSLError
from FlightRadar24.api import Flight
from FlightRadar24.errors import CloudflareError
from .utils import upload_dict_list_to_gcs
from .constants import GCS_BUCKET_NAME


class FlightRadarExtractor:
    def __init__(
        self, current_time, directory, raw_filename, fr_api, spark, bucket, bounds_list
    ):
        self.current_time = current_time
        self.directory = directory
        self.raw_filename = raw_filename
        self.fr_api = fr_api
        self.spark = spark
        self.bucket = bucket
        self.bounds_list = bounds_list

    def extract(self) -> None:
        logging.info("Fetching data from FlightRadar API...")
        flight_list = self._extract_flights()
        flight_dict_list = self._extract_flights_details(flight_list)

        upload_dict_list_to_gcs(
            self.bucket,
            json.dumps(flight_dict_list),
            f"bronze/{self.directory}/{self.raw_filename}",
        )
        logging.info(
            "Uploaded %d flights to GCS bucket: %s.",
            len(flight_dict_list),
            GCS_BUCKET_NAME,
        )

    def _extract_flights(self) -> List[Optional[Flight]]:
        bounds_rdd = self.spark.sparkContext.parallelize(self.bounds_list)
        flights_rdd = bounds_rdd.flatMap(
            lambda bounds: self.fr_api.get_flights(bounds=bounds)
        )
        flight_list = flights_rdd.collect()
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

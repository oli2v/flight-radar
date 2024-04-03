import time
import logging
from random import randrange
from typing import List, Dict, Optional, Any
from tqdm import tqdm
from requests.exceptions import HTTPError, SSLError
from FlightRadar24 import FlightRadar24API
from FlightRadar24.api import Flight
from FlightRadar24.errors import CloudflareError
from pyspark.sql import SparkSession
from .common.utils import split_map
from .common.constants import MAX_NB_FLIGHTS, LATITUDE_RANGE, LONGITUDE_RANGE


class FlightRadarExtractor:
    bounds_list = split_map(LATITUDE_RANGE, LONGITUDE_RANGE)

    def __init__(self, spark: SparkSession):
        self.bounds_rdd = spark.sparkContext.parallelize(self.bounds_list)

    def extract(self, fr_api: FlightRadar24API) -> None:
        logging.info("Fetching data from FlightRadar API...")
        flight_list = self._extract_flights(fr_api)
        flight_dict_list = self._extract_flights_details(flight_list, fr_api)
        return flight_dict_list

    def _extract_flights(self, fr_api: FlightRadar24API) -> List[Optional[Flight]]:
        def _extract_flights_from_bounds(bounds: str) -> List[Flight]:
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

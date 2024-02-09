import json
import time
from random import randrange
from datetime import datetime
from requests.exceptions import HTTPError, SSLError
from tqdm import tqdm

from FlightRadar24 import FlightRadar24API
from FlightRadar24.errors import CloudflareError
import pyspark.sql.functions as F
from pyspark.sql.functions import col

from .constants import KEY_TO_KEEP_LIST, NUM_FLIGHTS_TO_EXTRACT
from .utils import init_spark, normalize_nested_dict, make_directories
from .analyze import analyze_flight_data


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
        make_directories(self.destination_blob_name)

    def extract_all(self):
        flight_list = self._extract_flights()
        flight_dict_list = self._extract_flights_details(flight_list)

        with open(
            f"flight_radar/src/data/bronze/{self.destination_blob_name}/{self.raw_filename}",
            "w",
        ) as json_fp:
            json.dump(flight_dict_list, json_fp)

    def process(self):
        with open(
            f"flight_radar/src/data/bronze/{self.destination_blob_name}/{self.raw_filename}",
            "r",
        ) as json_fp:
            raw_flight_dict_list = json.load(json_fp)

        normalized_flight_dict_list = []
        for raw_flight_dict in raw_flight_dict_list:
            normalized_flight_dict = normalize_nested_dict(raw_flight_dict)
            normalized_flight_dict_list.append(normalized_flight_dict)

        normalized_flight_dict_list = [
            {
                key: value
                for key, value in normalized_flight_dict.items()
                if key in KEY_TO_KEEP_LIST
            }
            for normalized_flight_dict in normalized_flight_dict_list
        ]

        flights_sdf = (
            self.spark.createDataFrame(normalized_flight_dict_list)
            .withColumn("tech_year", F.lit(self.current_year))
            .withColumn("tech_month", F.lit(self.current_month))
            .withColumn("tech_day", F.lit(self.current_day))
            .withColumn("tech_hour", F.lit(self.current_hour))
            .withColumn("created_at_ts", F.lit(self.current_time))
            .repartition("tech_year", "tech_month", "tech_day", "tech_hour")
        )

        flights_sdf.write.mode("append").partitionBy(
            "tech_year", "tech_month", "tech_day", "tech_hour"
        ).parquet("flight_radar/src/data/silver/flights.parquet")

    def analyze(self):
        flights_sdf = self.spark.read.parquet(
            "flight_radar/src/data/silver/flights.parquet"
        ).filter(col("created_at_ts") == self.current_time)
        (
            live_flights_count_by_airline_pdf,
            live_flights_by_distance_pdf,
            flights_count_by_manufacturer_pdf,
            model_count_by_airline_pdf,
        ) = analyze_flight_data(flights_sdf)
        filename_pdf_dict = {
            "live_flights_count_by_airline": live_flights_count_by_airline_pdf,
            "live_flights_by_distance": live_flights_by_distance_pdf,
            "flights_count_by_manufacturer": flights_count_by_manufacturer_pdf,
            "model_count_by_airline": model_count_by_airline_pdf,
        }
        for filename, any_pdf in filename_pdf_dict.items():
            any_pdf.to_csv(
                (
                    f"flight_radar/src/data/gold/{self.destination_blob_name}/"
                    f"{filename}_{self.formatted_time}.csv"
                ),
                index=False,
            )

    def _extract_flights(self):
        flight_list = self._extract_any_object(self.fr_api.get_flights)
        return flight_list

    def _extract_airports(self):
        airport_list = self._extract_any_object(self.fr_api.get_airports)
        return airport_list

    def _extract_flights_details(self, flight_list):
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

    def _extract_any_object(self, any_function):
        obj_list = any_function()
        return obj_list

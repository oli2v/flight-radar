from FlightRadar24 import FlightRadar24API
from google.cloud import storage, bigquery

from .utils import init_spark, split_map
from .constants import (
    GCS_BUCKET_NAME,
    LATITUDE_RANGE,
    LONGITUDE_RANGE,
)


class FlightRadarConfig:
    def __init__(self):
        self.fr_api = FlightRadar24API()
        self.spark = init_spark("flight-radar-spark")
        self.bq_client = bigquery.Client()
        self.bucket = storage.Client().bucket(GCS_BUCKET_NAME)
        self.bounds_list = split_map(LATITUDE_RANGE, LONGITUDE_RANGE)

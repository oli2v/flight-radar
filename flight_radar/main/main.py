import os
from flight_radar.src.pipeline import FlightRadarPipeline, GoogleFlightRadarPipeline
from flight_radar.src.common.constants import SPARK_CONFIG


def run_pipeline():
    if os.getenv("PROJECT_ID"):
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            pipeline = GoogleFlightRadarPipeline(spark_config=SPARK_CONFIG)
        pipeline = GoogleFlightRadarPipeline()
    else:
        pipeline = FlightRadarPipeline()
    pipeline.run()

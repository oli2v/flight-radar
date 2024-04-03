import os
from flight_radar.src.pipeline import FlightRadarPipeline, GoogleFlightRadarPipeline


def run_pipeline():
    if os.getenv("PROJECT_ID"):
        pipeline = GoogleFlightRadarPipeline()
    else:
        pipeline = FlightRadarPipeline()
    pipeline.run()

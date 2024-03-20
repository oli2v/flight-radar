from flight_radar.src.config import FlightRadarConfig
from flight_radar.src.pipeline import FlightRadarPipeline

config = FlightRadarConfig()


def run_pipeline():
    pipeline = FlightRadarPipeline(config)
    pipeline.run()

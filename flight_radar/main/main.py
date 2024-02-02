from flight_radar.src.pipeline import FlightRadarPipeline


def run_pipeline():
    pipeline = FlightRadarPipeline()
    print("Fetching data from FlightRadar API...")
    pipeline.extract_all()
    print("Flights data fetched. Processing data...")
    pipeline.process()
    print("Flights data processed. Analyzing data...")
    pipeline.analyze()
    print("Flights data analyzed. Done!")

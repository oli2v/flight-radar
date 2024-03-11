from flight_radar.src.pipeline import FlightRadarPipeline


def run_pipeline():
    pipeline = FlightRadarPipeline()
    print("Fetching data from FlightRadar API...")
    pipeline.extract()
    print("Flights data fetched. Processing data...")
    flights_sdf = pipeline.transform()
    print("Flights data processed. Uploading flights data to BigQuery...")
    pipeline.load(flights_sdf)
    print("Flights data uploaded to BigQuery!")

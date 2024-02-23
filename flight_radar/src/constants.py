from pyspark.sql.types import StructType, StructField, StringType, FloatType

KEY_TO_KEEP_LIST = [
    "identification_id",
    "identification_number",
    "status_live",
    "airline_name",
    "airline_code_icao",
    "airport_origin_name",
    "airport_origin_code_icao",
    "airport_origin_position_latitude",
    "airport_origin_position_longitude",
    "airport_destination_name",
    "airport_destination_code_icao",
    "airport_destination_position_latitude",
    "airport_destination_position_longitude",
    "aircraft_model_code",
    "aircraft_model_text",
    "time_scheduled_departure",
]

NUM_FLIGHTS_TO_EXTRACT = 5000
LATITUDE_RANGE = range(-90, 91, 10)
LONGITUDE_RANGE = range(-180, 181, 30)

FLIGHTS_SCHEMA = StructType(
    [
        StructField("status_live", StringType(), True),
        StructField("airline_name", StringType(), True),
        StructField("airport_origin_position_latitude", FloatType(), True),
        StructField("airport_destination_position_latitude", FloatType(), True),
        StructField("airport_origin_position_longitude", FloatType(), True),
        StructField("airport_destination_position_longitude", FloatType(), True),
        StructField("aircraft_model", StringType(), True),
        StructField("manufacturer", StringType(), True),
    ]
)

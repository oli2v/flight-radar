import os
from pyspark.sql.types import (
    StructType,
    StructField,
    BooleanType,
    IntegerType,
    StringType,
    ArrayType,
)

NUM_FLIGHTS_TO_EXTRACT = 100
LATITUDE_RANGE = range(-90, 91, 10)
LONGITUDE_RANGE = range(-180, 181, 30)

FLIGHTS_SCHEMA = StructType(
    [
        StructField("level", StringType(), True),
        StructField("promote", BooleanType(), True),
        StructField("owner", StringType(), True),
        StructField("airspace", StringType(), True),
        StructField("ems", StringType(), True),
        StructField("availability", ArrayType(StringType()), True),
        StructField(
            "trail",
            ArrayType(
                StructType(
                    [
                        StructField("lat", StringType(), True),
                        StructField("lng", StringType(), True),
                        StructField("alt", IntegerType(), True),
                        StructField("spd", IntegerType(), True),
                        StructField("ts", StringType(), True),
                        StructField("hd", IntegerType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("firstTimestamp", StringType(), True),
        StructField("s", StringType(), True),
        StructField("identification_id", StringType(), True),
        StructField("identification_row", StringType(), True),
        StructField("identification_callsign", StringType(), True),
        StructField("status_live", BooleanType(), True),
        StructField("status_text", StringType(), True),
        StructField("status_icon", StringType(), True),
        StructField("status_estimated", StringType(), True),
        StructField("status_ambiguous", BooleanType(), True),
        StructField("aircraft_countryId", StringType(), True),
        StructField("aircraft_registration", StringType(), True),
        StructField("aircraft_age", StringType(), True),
        StructField("aircraft_msn", StringType(), True),
        StructField("aircraft_hex", StringType(), True),
        StructField("airline_name", StringType(), True),
        StructField("airline_short", StringType(), True),
        StructField("airline_url", StringType(), True),
        StructField("airport_origin", StringType(), True),
        StructField("airport_destination", StringType(), True),
        StructField("airport_real", StringType(), True),
        StructField(
            "flightHistory_aircraft",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "identification",
                            StructType(
                                [
                                    StructField("id", StringType(), True),
                                    StructField(
                                        "number",
                                        StructType(
                                            [StructField("default", StringType(), True)]
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            "airport",
                            StructType(
                                [
                                    StructField("origin", StringType(), True),
                                    StructField("destination", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            "time",
                            StructType(
                                [
                                    StructField(
                                        "real",
                                        StructType(
                                            [
                                                StructField(
                                                    "departure", StringType(), True
                                                )
                                            ]
                                        ),
                                        True,
                                    )
                                ]
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
        StructField("time_historical", StringType(), True),
        StructField("identification_number_default", StringType(), True),
        StructField("identification_number_alternative", StringType(), True),
        StructField("aircraft_model_code", StringType(), True),
        StructField("aircraft_model_text", StringType(), True),
        StructField(
            "aircraft_images_thumbnails",
            ArrayType(
                StructType(
                    [
                        StructField("src", StringType(), True),
                        StructField("link", StringType(), True),
                        StructField("copyright", StringType(), True),
                        StructField("source", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "aircraft_images_medium",
            ArrayType(
                StructType(
                    [
                        StructField("src", StringType(), True),
                        StructField("link", StringType(), True),
                        StructField("copyright", StringType(), True),
                        StructField("source", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "aircraft_images_large",
            ArrayType(
                StructType(
                    [
                        StructField("src", StringType(), True),
                        StructField("link", StringType(), True),
                        StructField("copyright", StringType(), True),
                        StructField("source", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("airline_code_iata", StringType(), True),
        StructField("airline_code_icao", StringType(), True),
        StructField("time_scheduled_departure", IntegerType(), True),
        StructField("time_scheduled_arrival", IntegerType(), True),
        StructField("time_real_departure", StringType(), True),
        StructField("time_real_arrival", StringType(), True),
        StructField("time_estimated_departure", StringType(), True),
        StructField("time_estimated_arrival", StringType(), True),
        StructField("time_other_eta", IntegerType(), True),
        StructField("time_other_updated", IntegerType(), True),
        StructField("status_generic_status_text", StringType(), True),
        StructField("status_generic_status_color", StringType(), True),
        StructField("status_generic_status_type", StringType(), True),
    ]
)

GC_CREDENTIALS_FP = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCS_BUCKET_NAME = "flight-radar-bucket"

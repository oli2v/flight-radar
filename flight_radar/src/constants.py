from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    ArrayType,
)

NUM_FLIGHTS_TO_EXTRACT = 100
LATITUDE_RANGE = range(-90, 91, 10)
LONGITUDE_RANGE = range(-180, 181, 30)
PARTITION_BY_COL_LIST = ["tech_year", "tech_month", "tech_day", "tech_hour"]

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
                                            [
                                                StructField(
                                                    "default", StringType(), True
                                                ),
                                            ]
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
                                    StructField(
                                        "origin",
                                        StructType(
                                            [
                                                StructField("name", StringType(), True),
                                                StructField(
                                                    "code",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "iata",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "icao",
                                                                StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                StructField(
                                                    "position",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "latitude",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "longitude",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "altitude",
                                                                IntegerType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "country",
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            "id",
                                                                            IntegerType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            "name",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            "code",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            "codeLong",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                ),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "region",
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            "city",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                ),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                StructField(
                                                    "timezone",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "name",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "offset",
                                                                IntegerType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "offsetHours",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "abbr",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "abbrName",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "isDst",
                                                                BooleanType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                StructField(
                                                    "visible", BooleanType(), True
                                                ),
                                                StructField(
                                                    "website", StringType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        "destination",
                                        StructType(
                                            [
                                                StructField("name", StringType(), True),
                                                StructField(
                                                    "code",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "iata",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "icao",
                                                                StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                StructField(
                                                    "position",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "latitude",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "longitude",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "altitude",
                                                                IntegerType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "country",
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            "id",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            "name",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            "code",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                ),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "region",
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            "city",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                ),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                StructField(
                                                    "timezone",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "name",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "offset",
                                                                IntegerType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "offsetHours",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "abbr",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "abbrName",
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "isDst",
                                                                BooleanType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                StructField(
                                                    "visible", BooleanType(), True
                                                ),
                                                StructField(
                                                    "website", StringType(), True
                                                ),
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
                                                                "departure",
                                                                StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
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
        StructField("airport_origin_name", StringType(), True),
        StructField("airport_origin_visible", BooleanType(), True),
        StructField("airport_origin_website", StringType(), True),
        StructField("airport_destination_name", StringType(), True),
        StructField("airport_destination_visible", BooleanType(), True),
        StructField("airport_destination_website", StringType(), True),
        StructField("time_scheduled_departure", StringType(), True),
        StructField("time_scheduled_arrival", StringType(), True),
        StructField("time_real_departure", StringType(), True),
        StructField("time_real_arrival", StringType(), True),
        StructField("time_estimated_departure", StringType(), True),
        StructField("time_estimated_arrival", StringType(), True),
        StructField("time_other_eta", StringType(), True),
        StructField("time_other_updated", StringType(), True),
        StructField("time_historical_flighttime", StringType(), True),
        StructField("time_historical_delay", StringType(), True),
        StructField("status_generic_status_text", StringType(), True),
        StructField("status_generic_status_color", StringType(), True),
        StructField("status_generic_status_type", StringType(), True),
        StructField("status_generic_eventTime_utc", StringType(), True),
        StructField("status_generic_eventTime_local", StringType(), True),
        StructField("airport_origin_code_iata", StringType(), True),
        StructField("airport_origin_code_icao", StringType(), True),
        StructField("airport_origin_position_latitude", StringType(), True),
        StructField("airport_origin_position_longitude", StringType(), True),
        StructField("airport_origin_position_altitude", IntegerType(), True),
        StructField("airport_origin_timezone_name", StringType(), True),
        StructField("airport_origin_timezone_offset", IntegerType(), True),
        StructField("airport_origin_timezone_offsetHours", StringType(), True),
        StructField("airport_origin_timezone_abbr", StringType(), True),
        StructField("airport_origin_timezone_abbrName", StringType(), True),
        StructField("airport_origin_timezone_isDst", BooleanType(), True),
        StructField("airport_origin_info_terminal", StringType(), True),
        StructField("airport_origin_info_baggage", StringType(), True),
        StructField("airport_origin_info_gate", StringType(), True),
        StructField("airport_destination_code_iata", StringType(), True),
        StructField("airport_destination_code_icao", StringType(), True),
        StructField("airport_destination_position_latitude", StringType(), True),
        StructField("airport_destination_position_longitude", StringType(), True),
        StructField("airport_destination_position_altitude", IntegerType(), True),
        StructField("airport_destination_timezone_name", StringType(), True),
        StructField("airport_destination_timezone_offset", IntegerType(), True),
        StructField("airport_destination_timezone_offsetHours", StringType(), True),
        StructField("airport_destination_timezone_abbr", StringType(), True),
        StructField("airport_destination_timezone_abbrName", StringType(), True),
        StructField("airport_destination_timezone_isDst", BooleanType(), True),
        StructField("airport_destination_info_terminal", StringType(), True),
        StructField("airport_destination_info_baggage", StringType(), True),
        StructField("airport_destination_info_gate", StringType(), True),
        StructField("airport_origin_position_country_id", StringType(), True),
        StructField("airport_origin_position_country_name", StringType(), True),
        StructField("airport_origin_position_country_code", StringType(), True),
        StructField("airport_origin_position_region_city", StringType(), True),
        StructField("airport_destination_position_country_id", StringType(), True),
        StructField("airport_destination_position_country_name", StringType(), True),
        StructField("airport_destination_position_country_code", StringType(), True),
        StructField(
            "airport_destination_position_country_codeLong", StringType(), True
        ),
        StructField("airport_destination_position_region_city", StringType(), True),
    ]
)


GCS_BUCKET_NAME = "flight-radar-bucket"
GOOGLE_PROJECT_NAME = "flight-radar-415911"
BQ_DATASET_NAME = "flight_radar_dataset"
BQ_TABLE_NAME = "flights"

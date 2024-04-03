from datetime import datetime
from typing import Tuple, Dict
from pandas import DataFrame as PandasDataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F

from pyspark.sql import DataFrame as SparkDataFrame


def analyze(
    flights_sdf: SparkDataFrame, current_time: datetime
) -> Dict[str, PandasDataFrame]:
    (
        live_flights_count_by_airline_pdf,
        live_flights_by_distance_pdf,
        flights_count_by_manufacturer_pdf,
        model_count_by_airline_pdf,
    ) = analyze_flight_data(flights_sdf.filter(col("created_at_ts") == current_time))
    filename_pdf_dict = {
        "live_flights_count_by_airline": live_flights_count_by_airline_pdf,
        "live_flights_by_distance": live_flights_by_distance_pdf,
        "flights_count_by_manufacturer": flights_count_by_manufacturer_pdf,
        "model_count_by_airline": model_count_by_airline_pdf,
    }
    return filename_pdf_dict


def analyze_flight_data(flights_sdf: SparkDataFrame) -> Tuple[PandasDataFrame]:
    live_flights_count_by_airline_pdf = _get_live_flights_count_by_airline_pdf(
        flights_sdf
    )
    live_flights_by_distance_pdf = _get_live_flights_by_distance_pdf(flights_sdf)
    flights_count_by_manufacturer_pdf = _get_flights_count_by_manufacturer_pdf(
        flights_sdf
    )
    model_count_by_airline_pdf = _get_model_count_by_airline_pdf(flights_sdf)
    return (
        live_flights_count_by_airline_pdf,
        live_flights_by_distance_pdf,
        flights_count_by_manufacturer_pdf,
        model_count_by_airline_pdf,
    )


def _get_live_flights_count_by_airline_pdf(
    flights_sdf: SparkDataFrame,
) -> PandasDataFrame:
    return (
        flights_sdf.filter(col("status_live"))
        .groupBy("airline_name")
        .count()
        .orderBy(col("count").desc())
        .toPandas()
    )


def _get_live_flights_by_distance_pdf(flights_sdf: SparkDataFrame) -> PandasDataFrame:
    return (
        flights_sdf.filter(col("status_live"))
        .withColumn(
            "distance_in_kms",
            F.round(
                (
                    F.acos(
                        (
                            F.sin(F.radians(col("airport_origin_position_latitude")))
                            * F.sin(
                                F.radians(col("airport_destination_position_latitude"))
                            )
                        )
                        + (
                            (
                                F.cos(
                                    F.radians(col("airport_origin_position_latitude"))
                                )
                                * F.cos(
                                    F.radians(
                                        col("airport_destination_position_latitude")
                                    )
                                )
                            )
                            * (
                                F.cos(
                                    F.radians(col("airport_origin_position_longitude"))
                                    - F.radians(
                                        col("airport_destination_position_longitude")
                                    )
                                )
                            )
                        )
                    )
                    * F.lit(6371.0)
                ),
                4,
            ),
        )
        .orderBy(col("distance_in_kms").desc())
        .toPandas()
    )


def _get_flights_count_by_manufacturer_pdf(
    flights_sdf: SparkDataFrame,
) -> PandasDataFrame:
    return (
        flights_sdf.filter(col("status_live"))
        .withColumn("manufacturer", F.split(col("aircraft_model_text"), " ").getItem(0))
        .groupBy("manufacturer")
        .count()
        .orderBy(col("count").desc())
        .toPandas()
    )


def _get_model_count_by_airline_pdf(flights_sdf: SparkDataFrame) -> PandasDataFrame:
    return (
        flights_sdf.groupBy(["airline_name", "aircraft_model_text"])
        .count()
        .orderBy(col("count").desc())
        .toPandas()
    )

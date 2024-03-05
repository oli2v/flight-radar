from pyspark.sql import SparkSession
from .constants import GC_CREDENTIALS_FP


def init_spark(name):
    spark = (
        SparkSession.builder.appName(name)
        .config("spark.driver.memory", "4g")
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
        .config("spark.jars", "gs://hadoop-lib/gcs/gcs-connector-latest-hadoop3.jar")
        .getOrCreate()
    )
    spark.conf.set(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    spark.conf.set("spark.hadoop.fs.gs.auth.service.account.enable", "true")
    spark.conf.set(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile", GC_CREDENTIALS_FP
    )
    return spark


def normalize_nested_dict(nested_dict, sep="_"):
    normalized_dict = {}
    queue_list = list(nested_dict.items())
    while queue_list:
        current_node = queue_list[0]
        key, value = current_node
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                child_node = (f"{key}{sep}{sub_key}", sub_value)
                queue_list.append(child_node)
        else:
            normalized_dict[key] = value
        queue_list.remove(current_node)
    return normalized_dict


def split_map(latitude_range, longitude_range):
    bounds_list = []
    for minimum_latitude, maximum_latitude in zip(latitude_range, latitude_range[1:]):
        for minimum_longitude, maximum_longitude in zip(
            longitude_range, longitude_range[1:]
        ):
            bounds = (
                f"{maximum_latitude},{minimum_latitude},{minimum_longitude}"
                f",{maximum_longitude}"
            )
            bounds_list.append(bounds)
    return bounds_list


def merge_flights(future_list):
    merged_flight_list = []
    for future in future_list:
        flight_list = future.result()
        for flight in flight_list:
            merged_flight_list.append(flight)
    return merged_flight_list

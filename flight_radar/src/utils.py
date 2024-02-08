import os
from pyspark.sql import SparkSession


def init_spark(name):
    spark = SparkSession.builder.appName(name).getOrCreate()
    return spark


def normalize_nested_dict(nested_dict, parent_key="", sep="_"):
    normalized_dict = {}
    for key, value in nested_dict.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            normalized_dict.update(normalize_nested_dict(value, new_key, sep))
        else:
            normalized_dict[new_key] = value

    return normalized_dict


def make_directories(destination_blob_name):
    if not os.path.exists(f"flight_radar/src/data/bronze/{destination_blob_name}"):
        os.makedirs(f"flight_radar/src/data/bronze/{destination_blob_name}")
    if not os.path.exists(f"flight_radar/src/data/gold/{destination_blob_name}"):
        os.makedirs(f"flight_radar/src/data/gold/{destination_blob_name}")

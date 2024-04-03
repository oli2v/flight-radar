import json
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.storage.bucket import Bucket


def init_spark(
    name: str, spark_config: Optional[Dict[str, str]] = None
) -> SparkSession:
    spark = (
        SparkSession.builder.appName(name)
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
        .config("spark.jars", "gs://hadoop-lib/gcs/gcs-connector-latest-hadoop3.jar")
        .getOrCreate()
    )
    if spark_config:
        for key, value in spark_config.items():
            spark.conf.set(key, value)

    return spark


def get_directory(current_time: datetime) -> str:
    year = current_time.year
    month = current_time.month
    day = current_time.day
    hour = current_time.hour
    return f"tech_year={year}/tech_month={month}/tech_day={day}/tech_hour={hour}"


def normalize_data(
    any_dict_list: List[Optional[Dict[Any, Any]]],
) -> List[Optional[Dict[str, Any]]]:
    normalized_dict_list = []
    for any_dict in any_dict_list:
        normalized_flight_dict = _normalize_nested_dict(any_dict)
        normalized_dict_list.append(normalized_flight_dict)
    return normalized_dict_list


def split_map(latitude_range: range, longitude_range: range) -> List[str]:
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


def get_json_from_gcs(
    bucket: Bucket, destination_blob_name: str, raw_filename: str
) -> List[Optional[Dict[Any, Any]]]:
    blob = bucket.blob(f"bronze/{destination_blob_name}/{raw_filename}")
    any_dict_list = json.loads(blob.download_as_string(client=None))
    return any_dict_list


def create_sdf_from_dict_list(
    spark: SparkSession,
    any_dict_list: List[Optional[Dict[str, Any]]],
    schema: StructType,
    current_time: datetime,
) -> DataFrame:
    year, month, day, hour = _unpack_datetime(current_time)
    return (
        spark.createDataFrame(any_dict_list, schema=schema)
        .withColumn("tech_year", F.lit(year))
        .withColumn("tech_month", F.lit(month))
        .withColumn("tech_day", F.lit(day))
        .withColumn("tech_hour", F.lit(hour))
        .withColumn("created_at_ts", F.lit(current_time))
    )


def write_sdf(any_sdf: DataFrame, uri: str, partition_by_col_list: List[str]) -> None:
    any_sdf.write.mode("append").partitionBy(partition_by_col_list).parquet(uri)


def load_parquet_to_bq(uri: str, bq_client: Client, table_id: str) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()


def upload_dict_list_to_gcs(
    bucket: Bucket, contents: str, destination_blob_name: str
) -> None:
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents)


def get_raw_filename(data_type: str, current_time: datetime) -> str:
    formatted_time = _get_formatted_time(current_time)
    raw_filename = f"{data_type}_{formatted_time}.json"
    return raw_filename


def _get_formatted_time(current_time: datetime) -> str:
    return current_time.strftime("%Y%m%d%H%M%S%f")[:-3]


def _normalize_nested_dict(
    nested_dict: Dict[Any, Any], sep: str = "_"
) -> Dict[str, Any]:
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


def _unpack_datetime(any_datetime: datetime) -> Tuple[int]:
    year = any_datetime.year
    month = any_datetime.month
    day = any_datetime.day
    hour = any_datetime.hour
    return year, month, day, hour

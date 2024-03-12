import json
from datetime import datetime
from typing import List, Dict, Optional, Any
from concurrent.futures import Future

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.storage.bucket import Bucket

from FlightRadar24.api import Flight


def init_spark(name: str) -> SparkSession:
    spark = SparkSession.builder.appName(name).getOrCreate()
    return spark


def normalize_data(
    any_dict_list: List[Optional[Dict[Any:Any]]],
) -> List[Optional[Dict[str:Any]]]:
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


def merge_flights(future_list: List[Optional[List[Future]]]) -> List[Optional[Flight]]:
    merged_flight_list = []
    for future in future_list:
        flight_list = future.result()
        for flight in flight_list:
            merged_flight_list.append(flight)
    return merged_flight_list


def get_json_from_gcs(
    bucket: Bucket, destination_blob_name: str, raw_filename: str
) -> List[Optional[Dict[Any:Any]]]:
    blob = bucket.blob(f"bronze/{destination_blob_name}/{raw_filename}")
    any_dict_list = json.loads(blob.download_as_string(client=None))
    return any_dict_list


def create_sdf_from_dict_list(
    spark: SparkSession,
    any_dict_list: List[Optional[Dict[str:Any]]],
    schema: StructType,
    current_year: int,
    current_month: int,
    current_day: int,
    current_hour: int,
    current_time: datetime,
) -> DataFrame:
    return (
        spark.createDataFrame(any_dict_list, schema=schema)
        .withColumn("tech_year", F.lit(current_year))
        .withColumn("tech_month", F.lit(current_month))
        .withColumn("tech_day", F.lit(current_day))
        .withColumn("tech_hour", F.lit(current_hour))
        .withColumn("created_at_ts", F.lit(current_time))
    )


def write_sdf_to_gcs(
    any_sdf: DataFrame, uri: str, partition_by_col_list: List[str]
) -> None:
    any_sdf.write.mode("append").partitionBy(partition_by_col_list).parquet(uri)


def load_parquet_to_bq(uri: str, bq_client: Client, table_id: str) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    destination_table = bq_client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows.")


def upload_dict_list_to_gcs(bucket: Bucket, contents: str, destination_blob_name: str):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents)


def _normalize_nested_dict(nested_dict: Dict[Any:Any], sep: str = "_") -> Dict[str:Any]:
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

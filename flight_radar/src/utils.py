import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from google.cloud import bigquery

# from .constants import GC_CREDENTIALS_FP


def init_spark(name):
    spark = (
        SparkSession.builder.appName(name)
        # .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
        # .config("spark.jars", "gs://hadoop-lib/gcs/gcs-connector-latest-hadoop3.jar")
        .getOrCreate()
    )
    # spark.conf.set(
    #     "spark.hadoop.fs.gs.impl",
    #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    # )
    # spark.conf.set("spark.hadoop.fs.gs.auth.service.account.enable", "true")
    # spark.conf.set(
    #     "spark.hadoop.google.cloud.auth.service.account.json.keyfile", GC_CREDENTIALS_FP
    # )
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


def get_json_from_gcs(bucket, destination_blob_name, raw_filename):
    blob = bucket.blob(f"bronze/{destination_blob_name}/{raw_filename}")
    any_dict_list = json.loads(blob.download_as_string(client=None))
    return any_dict_list


def normalize_data(raw_flight_dict_list):
    normalized_flight_dict_list = []
    for raw_flight_dict in raw_flight_dict_list:
        normalized_flight_dict = normalize_nested_dict(raw_flight_dict)
        normalized_flight_dict_list.append(normalized_flight_dict)
    return normalized_flight_dict_list


def create_sdf_from_dict_list(
    spark,
    any_dict_list,
    schema,
    current_year,
    current_month,
    current_day,
    current_hour,
    current_time,
):
    return (
        spark.createDataFrame(any_dict_list, schema=schema)
        .withColumn("tech_year", F.lit(current_year))
        .withColumn("tech_month", F.lit(current_month))
        .withColumn("tech_day", F.lit(current_day))
        .withColumn("tech_hour", F.lit(current_hour))
        .withColumn("created_at_ts", F.lit(current_time))
    )


def write_sdf_to_gcs(any_sdf, uri, partition_by_col_list):
    any_sdf.write.mode("append").partitionBy(partition_by_col_list).parquet(uri)


def load_parquet_to_bq(uri, bq_client, table_id):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    destination_table = bq_client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows.")


def upload_dict_list_to_gcs(bucket, contents, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents)

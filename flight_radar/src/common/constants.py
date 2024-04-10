import os

NUM_FLIGHTS_TO_EXTRACT = 100
LATITUDE_RANGE = range(-90, 91, 10)
LONGITUDE_RANGE = range(-180, 181, 30)
PARTITION_BY_COL_LIST = ["tech_year", "tech_month", "tech_day", "tech_hour"]

GOOGLE_PROJECT_NAME = os.getenv("PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("DATA_BUCKET_NAME")
BQ_DATASET_NAME = os.getenv("BQ_DATASET_NAME")
GC_CREDENTIALS_FP = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BQ_FLIGHTS_TABLE_NAME = "flights"
BQ_FLIGHTS_TABLE_ID = f"{GOOGLE_PROJECT_NAME}.{BQ_DATASET_NAME}.{BQ_FLIGHTS_TABLE_NAME}"
MAX_NB_FLIGHTS = 50


SPARK_CONFIG = {
    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "spark.hadoop.fs.gs.auth.service.account.enable": "true",
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": GC_CREDENTIALS_FP,
}
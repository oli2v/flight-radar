import datetime

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

WORFKLOW_DAG_ID = "flight_radar_dag"
WORFKFLOW_START_DATE = datetime.datetime(2024, 2, 9)
WORKFLOW_SCHEDULE_INTERVAL = "0 */2 * * *"

WORKFLOW_DEFAULT_ARGS = {
    "owner": "Olivier Valenduc",
    "start_date": WORFKFLOW_START_DATE,
    "retries": 0,
}

dag = DAG(
    dag_id=WORFKLOW_DAG_ID,
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
    catchup=False,
)

spark_submit_operator = SparkSubmitOperator(
    application="/opt/airflow/dags/run.py",
    conn_id="spark_local",
    task_id="spark_submit_task",
    dag=dag,
)

spark_submit_operator

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

MINIO_CONNECTION_ID = 'aws_default'
SPARK_CONN = "spark_default"
BRONZE_BUCKET = 'bronze'
BRONZE_PREFIX = 'kolkata/'
S3_WILDCARD = BRONZE_PREFIX + "*.csv"
SILVER_BUCKET = 'silver'
SPARK_APP = "/opt/airflow/spark_jobs/transform_aqi.py"
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="process_kolkata_aqi_silver_classic",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["aqi", "kolkata", "silver", "spark"]
) as dag:
    
    start = EmptyOperator(task_id="start")

    wait_for_bronze_file = S3KeySensor(
        task_id="wait_for_any_bronze_csv",
        bucket_key=S3_WILDCARD,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=MINIO_CONNECTION_ID,
        wildcard_match=True,
        deferrable=True,
        timeout=60 * 60 * 6,
        poke_interval=30,
        mode="reschedule",
    )

    wait_for_prefix_stable = S3KeysUnchangedSensor(
        task_id="wait_for_bronze_prefix_stable",
        bucket_name=BRONZE_BUCKET,
        prefix=BRONZE_PREFIX,
        poke_interval=300,
        mode="poke",
        timeout=60 * 60 * 6,
    )

    transform_with_spark = SparkSubmitOperator(
        task_id="spark_transform_aqi",
        application=SPARK_APP,
        application_args=[
            "--bronze-bucket", BRONZE_BUCKET,
            "--bronze-prefix", BRONZE_PREFIX,
            "--silver-bucket", SILVER_BUCKET,
        ],
        conn_id=SPARK_CONN,
        verbose=False,
        conf={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
        },
    )

    done = EmptyOperator(task_id="done", trigger_rule="all_success")

    start >> wait_for_bronze_file >> wait_for_prefix_stable >> transform_with_spark >> done
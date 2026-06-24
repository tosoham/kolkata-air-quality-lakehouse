from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

MINIO_CONNECTION_ID = "aws_default"
SPARK_CONN = "spark_default"
BRONZE_BUCKET = "bronze"
BRONZE_PREFIX = "kolkata/"
S3_WILDCARD = BRONZE_PREFIX + "*.csv"
SILVER_BUCKET = "silver"
SPARK_APP = "/opt/airflow/spark_jobs/transform_aqi.py"

# MinIO is reachable on the docker network; these match the compose credentials.
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS = "admin"
MINIO_SECRET = "password"
# hadoop-aws 3.3.4 matches the hadoop client bundled with pyspark 3.5.x.
HADOOP_AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"

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
    tags=["aqi", "kolkata", "silver", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_bronze_file = S3KeySensor(
        task_id="wait_for_any_bronze_csv",
        bucket_key=S3_WILDCARD,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=MINIO_CONNECTION_ID,
        wildcard_match=True,
        deferrable=True,
        timeout=60 * 30,
        poke_interval=15,
    )

    wait_for_prefix_stable = S3KeysUnchangedSensor(
        task_id="wait_for_bronze_prefix_stable",
        bucket_name=BRONZE_BUCKET,
        prefix=BRONZE_PREFIX,
        aws_conn_id=MINIO_CONNECTION_ID,
        inactivity_period=20,
        poke_interval=15,
        mode="poke",
        timeout=60 * 30,
    )

    transform_with_spark = SparkSubmitOperator(
        task_id="spark_transform_aqi",
        application=SPARK_APP,
        application_args=[
            "--bronze-bucket", BRONZE_BUCKET,
            "--bronze-prefix", BRONZE_PREFIX,
            "--silver-bucket", SILVER_BUCKET,
            "--minio-endpoint", MINIO_ENDPOINT,
            "--minio-access", MINIO_ACCESS,
            "--minio-secret", MINIO_SECRET,
        ],
        conn_id=SPARK_CONN,
        verbose=False,
        packages=HADOOP_AWS_PKG,
        conf={
            "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS,
            "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.jars.ivy": "/tmp/.ivy2",
            "spark.driver.memory": "1g",
            "spark.sql.shuffle.partitions": "8",
        },
    )

    done = EmptyOperator(task_id="done", trigger_rule="all_success")

    start >> wait_for_bronze_file >> wait_for_prefix_stable >> transform_with_spark >> done

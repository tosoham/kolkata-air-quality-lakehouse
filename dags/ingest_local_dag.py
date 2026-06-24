import os
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

MINIO_CONNECTION_ID = "aws_default"
BRONZE_BUCKET = "bronze"
BRONZE_PREFIX = "kolkata/"
LOCAL_DATA_DIR = "/opt/data"


@dag(
    dag_id="ingest_local_files_to_bronze_v2",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "bronze_layer", "local_files"],
    doc_md="""
        ### Local Files → Bronze DAG

        Scans the local `/opt/data` directory (mounted into the container) and uploads
        every `*.csv` file to the MinIO `bronze` bucket under the `kolkata/` prefix.
    """,
)
def ingest_local_to_bronze_pipeline() -> None:
    @task
    def upload_local_files_to_minio() -> list[str]:
        """Upload all CSV files under LOCAL_DATA_DIR to s3://bronze/kolkata/."""
        hook = S3Hook(aws_conn_id=MINIO_CONNECTION_ID)

        # Make sure the bucket exists (idempotent; minio-init also creates it).
        if not hook.check_for_bucket(BRONZE_BUCKET):
            hook.create_bucket(bucket_name=BRONZE_BUCKET)

        if not os.path.isdir(LOCAL_DATA_DIR):
            raise FileNotFoundError(f"Local data dir not found: {LOCAL_DATA_DIR}")

        csv_files = sorted(
            f for f in os.listdir(LOCAL_DATA_DIR) if f.lower().endswith(".csv")
        )
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {LOCAL_DATA_DIR}")

        uploaded: list[str] = []
        for filename in csv_files:
            local_path = os.path.join(LOCAL_DATA_DIR, filename)
            key = f"{BRONZE_PREFIX}{filename}"
            hook.load_file(
                filename=local_path,
                key=key,
                bucket_name=BRONZE_BUCKET,
                replace=True,
            )
            print(f"Uploaded {local_path} -> s3://{BRONZE_BUCKET}/{key}")
            uploaded.append(key)

        print(f"Uploaded {len(uploaded)} file(s) to the bronze bucket.")
        return uploaded

    upload_local_files_to_minio()


ingest_local_to_bronze_pipeline()

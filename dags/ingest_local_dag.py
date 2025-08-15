import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# import logging

# logging.basicConfig(
#    level=logging.DEBUG,
#    format='%(asctime)s - %(levelname)s - %(message)s',
#)

# logger = logging.getLogger(__name__)

MINIO_CONNECTION_ID = 'aws_default'
BRONZE_BUCKET = 'bronze'
LOCAL_DATA_DIR = '/opt/data'

@dag(
    dag_id='ingest_local_files_to_bronze_v2',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['ingestion', 'bronze_layer', 'local_files'],
    doc_md="""
        ### Local Files To Bronze Level DAG

        This DAG automatically scans a local directory (mounted into the container)
        and uploads all files found to the MinIO 'bronze' bucket.
    """
)
def ingest_local_to_bronze_pipeline() -> None:
    """
    This pipeline defines the task to upload local files to MinIO.
    """

    @task
    def upload_local_files_to_minio() -> None:
        """
        Scans the local data directory and uploads each file to the MinIO bronze bucket.
        """
        print(f"Scanning files in {LOCAL_DATA_DIR}")
        s3_hook = S3Hook(aws_conn_id=MINIO_CONNECTION_ID)

        try:
            files_to_upload = [f for f in os.listdir(LOCAL_DATA_DIR) if os.path.isfile(os.path.join(LOCAL_DATA_DIR, f))]
        except FileNotFoundError:
            print(f"Directory not found at {LOCAL_DATA_DIR}. Make sure the volume is mounted correctly.")
            return
        
        if files_to_upload is None:
            print(f"Directory is empty nothing to upload!!")
            return
        
        print(f"Files Scanned Succesfully in {LOCAL_DATA_DIR}")
        print("---------------Starting Ingestion---------------")

        for filename in files_to_upload:
            filepath = os.path.join(LOCAL_DATA_DIR, filename)
            minio_key = filename

            s3_hook.load_file(
                filename=filepath,
                key=minio_key,
                bucket_name=BRONZE_BUCKET,
                replace=True,
                #encrypt=True,
            )
            print(f"Successfully uploaded {filename}.")

    upload_local_files_to_minio()

ingest_local_to_bronze_pipeline()
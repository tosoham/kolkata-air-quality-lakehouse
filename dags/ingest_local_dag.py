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
        A simple test task to check the worker's stability.
        """
        import time
        print("Hello from the worker! Task is starting.")
        time.sleep(30) # Run for 30 seconds
        print("Task finished successfully.")
    
    upload_local_files_to_minio()

ingest_local_to_bronze_pipeline()
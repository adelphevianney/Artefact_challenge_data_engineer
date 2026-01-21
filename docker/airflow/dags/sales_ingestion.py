from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from modules_dag import sales_ingestion_task

default_args = {
    "owner": "artefact",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}

@dag(
    dag_id="sales_ingestion_modulaire",
    description="Ingestion incrémentale idempotente générique et modulaire",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "ingestion"],
    params={"target_date": "20250616"},
)
def sales_ingestion_dag():

    @task
    def ingestion_task(target_date: str):
        minio_hook = S3Hook(aws_conn_id="minio_default")
        pg_hook = PostgresHook(postgres_conn_id="postgres_ecommerce")

        df = sales_ingestion_task.read_sales_data(minio_hook, "foldersource", "fashion_store_sales.csv")
        df_filtered = sales_ingestion_task.filter_sales_data(df, target_date)
        result = sales_ingestion_task.insert_sales_data(pg_hook, df_filtered)
        return result

    ingestion_task("{{ params.target_date }}")

sales_ingestion_dag()

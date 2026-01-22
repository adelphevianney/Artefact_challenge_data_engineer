from datetime import datetime, timedelta

import pandas as pd
from io import BytesIO
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from modules_dag.sales_ingestion.reader import read_sales_data
from modules_dag.sales_ingestion.transformer import filter_sales_data
from modules_dag.sales_ingestion.loader import insert_sales_data

default_args = {
    "owner": "artefact",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

@dag(
    dag_id="sales_ingestion_modulaire",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "ingestion"],
)
def sales_ingestion_dag():

    @task
    def get_target_date():
        context = get_current_context()
        return context["dag_run"].conf.get(
            "target_date",
            context["ds_nodash"]
        )

    @task
    def extract():
        minio = S3Hook(aws_conn_id="minio_default")

        df = read_sales_data(
            minio,
            bucket="foldersource",
            key="fashion_store_sales.csv"
        )

        if df is None or df.empty:
            raise ValueError("Aucune donnée extraite")

        # Sauvegarde intermédiaire
        output_key = "staging/sales_raw.parquet"

        minio.load_bytes(
            df.to_parquet(index=False),
            key=output_key,
            bucket_name="foldersource",
            replace=True
        )

        return output_key  # ✅ petit XCom

    @task
    def transform(input_key: str, target_date: str):
        minio = S3Hook(aws_conn_id="minio_default")

        obj = minio.get_key(input_key, bucket_name="foldersource")

        buffer = BytesIO(obj.get()["Body"].read())  # ✅ rend le flux seekable
        df = pd.read_parquet(buffer)

        df_filtered = filter_sales_data(df, target_date)

        output_key = "staging/sales_filtered.parquet"

        minio.load_bytes(
            df_filtered.to_parquet(index=False),
            key=output_key,
            bucket_name="foldersource",
            replace=True
        )

        return output_key

    @task
    def load(input_key: str):
        minio = S3Hook(aws_conn_id="minio_default")

        obj = minio.get_key(input_key, bucket_name="foldersource")

        buffer = BytesIO(obj.get()["Body"].read())
        df = pd.read_parquet(buffer)

        if df.empty:
            return "NO_DATA"

        pg = PostgresHook(postgres_conn_id="postgres_ecommerce")
        return insert_sales_data(pg, df)

    target_date = get_target_date()
    load(transform(extract(), target_date))

sales_ingestion_dag()

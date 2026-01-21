from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "vianney",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}

@dag(
    dag_id="sales_ingestion_incremental_1",
    description="Ingestion incrémentale des ventes e-commerce (Artefact Challenge)",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "ingestion", "artefact-challenge"],
    params={
        "target_date": "20250616"  # surcharge possible au trigger
    },
)
def sales_ingestion_dag():
    @task
    def ingest_sales_for_date():

        context = get_current_context()
        params = context["params"]
        logical_date = context["logical_date"]

        target_date_str = params.get("target_date") or logical_date.strftime("%Y%m%d")

        logger.info(f"Ingestion démarrée pour la date : {target_date_str}")

        # 2. Hooks
        minio_hook = S3Hook(aws_conn_id="minio_default")
        pg_hook = PostgresHook(postgres_conn_id="postgres_ecommerce")

        # 3. Lecture MinIO
        bucket_name = "foldersource"
        object_key = "fashion_store_sales.csv"

        s3_object = minio_hook.get_key(key=object_key, bucket_name=bucket_name)
        df = pd.read_csv(BytesIO(s3_object.get()["Body"].read()))
        logger.info(f"{len(df)} lignes lues depuis MinIO")

        # 4. Filtrage par date
        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
        target_date = pd.to_datetime(target_date_str, format="%Y%m%d").date()
        df_filtered = df[df["sale_date"] == target_date].copy()

        if df_filtered.empty:
            logger.info("Aucune donnée à ingérer pour cette date")
            return "NO_DATA"

        df_filtered["date_id"] = df_filtered["sale_date"].apply(
            lambda d: int(pd.Timestamp(d).strftime("%Y%m%d"))
        )

        logger.info(f"{len(df_filtered)} lignes à ingérer")

        # 5. Insertion PostgreSQL
        conn = pg_hook.get_conn()
        try:
            cursor = conn.cursor()

            # --- dim_dates ---
            dates = df_filtered["sale_date"].unique()
            date_records = []
            for d in dates:
                dt = pd.Timestamp(d)
                date_records.append((
                    int(dt.strftime("%Y%m%d")),
                    d,
                    dt.year,
                    dt.month,
                    dt.day,
                    dt.strftime("%A"),
                    dt.isocalendar()[1],
                    (dt.month - 1) // 3 + 1,
                    dt.weekday() >= 5,
                    False,
                ))

            cursor.executemany("""
                INSERT INTO sales.dim_dates (
                    date_id, full_date, year, month, day,
                    day_of_week, week_number, quarter, is_weekend, is_holiday
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (date_id) DO NOTHING
            """, date_records)

            # --- customers ---
            customer_cols = [
                "customer_id", "first_name", "last_name", "email",
                "gender", "age_range", "signup_date", "country"
            ]
            customers = (
                df_filtered[customer_cols]
                .drop_duplicates("customer_id")
                .where(pd.notnull, None)
                .values.tolist()
            )

            cursor.executemany("""
                INSERT INTO sales.customers (
                    customer_id, first_name, last_name, email,
                    gender, age_range, signup_date, country
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    gender = EXCLUDED.gender,
                    age_range = EXCLUDED.age_range,
                    signup_date = EXCLUDED.signup_date,
                    country = EXCLUDED.country
            """, customers)

            # --- products ---
            products = (
                df_filtered[[
                    "product_id", "product_name", "category", "brand",
                    "color", "size", "catalog_price", "cost_price"
                ]]
                .drop_duplicates("product_id")
                .values.tolist()
            )

            cursor.executemany("""
                INSERT INTO sales.products (
                    product_id, product_name, category, brand,
                    color, size, catalog_price, cost_price
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    brand = EXCLUDED.brand,
                    color = EXCLUDED.color,
                    size = EXCLUDED.size,
                    catalog_price = EXCLUDED.catalog_price,
                    cost_price = EXCLUDED.cost_price
            """, products)

            # --- orders ---
            orders = (
                df_filtered[[
                    "sale_id", "sale_date", "date_id",
                    "customer_id", "channel", "channel_campaigns"
                ]]
                .drop_duplicates("sale_id")
                .values.tolist()
            )

            cursor.executemany("""
                INSERT INTO sales.orders (
                    sale_id, sale_date, date_id,
                    customer_id, channel, channel_campaigns
                )
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sale_id) DO UPDATE SET
                    sale_date = EXCLUDED.sale_date,
                    date_id = EXCLUDED.date_id,
                    customer_id = EXCLUDED.customer_id,
                    channel = EXCLUDED.channel,
                    channel_campaigns = EXCLUDED.channel_campaigns
            """, orders)

            # --- order_items ---
            temp_df = df_filtered.copy()

            temp_df["discount_percent"] = pd.to_numeric(
                temp_df["discount_percent"].astype(str).str.replace("%", ""),
                errors="coerce"
            ).fillna(0.0)

            temp_df["discounted"] = temp_df["discounted"].astype(bool)
            temp_df = temp_df.where(pd.notnull, None)

            items = temp_df[[
                "item_id", "sale_id", "product_id", "quantity",
                "original_price", "unit_price",
                "discount_applied", "discount_percent", "discounted"
            ]].values.tolist()

            cursor.executemany("""
                INSERT INTO sales.order_items (
                    item_id, sale_id, product_id, quantity,
                    original_price, unit_price,
                    discount_applied, discount_percent, discounted
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (item_id) DO UPDATE SET
                    sale_id = EXCLUDED.sale_id,
                    product_id = EXCLUDED.product_id,
                    quantity = EXCLUDED.quantity,
                    original_price = EXCLUDED.original_price,
                    unit_price = EXCLUDED.unit_price,
                    discount_applied = EXCLUDED.discount_applied,
                    discount_percent = EXCLUDED.discount_percent,
                    discounted = EXCLUDED.discounted
            """, items)

            conn.commit()
            logger.info("Ingestion terminée avec succès")
            return "SUCCESS"

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    ingest_sales_for_date()

sales_ingestion_dag()

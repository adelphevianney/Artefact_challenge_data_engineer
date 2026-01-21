# modules/sales_ingestion_task.py
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

def read_sales_data(minio_hook, bucket_name: str, object_key: str) -> pd.DataFrame:
    """Lecture des données depuis S3/MinIO"""
    s3_object = minio_hook.get_key(key=object_key, bucket_name=bucket_name)
    df = pd.read_csv(BytesIO(s3_object.get()["Body"].read()))
    logger.info("%s lignes lues depuis MinIO", len(df))
    return df

def filter_sales_data(df: pd.DataFrame, target_date_str: str) -> pd.DataFrame:
    """Filtrage et normalisation des données"""
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
    target_date = pd.to_datetime(target_date_str, format="%Y%m%d").date()
    df_filtered = df[df["sale_date"] == target_date].copy()
    if df_filtered.empty:
        return pd.DataFrame()

    df_filtered["date_id"] = df_filtered["sale_date"].apply(lambda d: int(pd.Timestamp(d).strftime("%Y%m%d")))
    df_filtered["discount_percent"] = pd.to_numeric(
        df_filtered["discount_percent"].astype(str).str.replace("%", ""), errors="coerce"
    ).fillna(0.0)
    df_filtered["discounted"] = df_filtered["discounted"].astype(bool)
    df_filtered = df_filtered.where(pd.notnull, None)
    return df_filtered

"""
# --- configuration des tables ---
TABLE_CONFIG = {
    "dim_dates": {
        "columns": [
            "date_id", "full_date", "year", "month", "day",
            "day_of_week", "week_number", "quarter", "is_weekend", "is_holiday"
        ],
        "primary_key": "date_id",
        "update_columns": [],  # pas de mise à jour nécessaire
        "prepare": lambda df: [
            (
                int(pd.Timestamp(d).strftime("%Y%m%d")),
                d,
                d.year,
                d.month,
                d.day,
                d.strftime("%A"),
                d.isocalendar()[1],
                (d.month - 1) // 3 + 1,
                d.weekday() >= 5,
                False,
            ) for d in df["sale_date"].unique()
        ]
    },
    "customers": {
        "columns": ["customer_id", "first_name", "last_name", "email",
                    "gender", "age_range", "signup_date", "country"],
        "primary_key": "customer_id",
        "update_columns": ["first_name", "last_name", "email", "gender", "age_range", "signup_date", "country"],
        "prepare": lambda df: df[[
            "customer_id", "first_name", "last_name", "email",
            "gender", "age_range", "signup_date", "country"
        ]].drop_duplicates("customer_id").values.tolist()
    },
    "products": {
        "columns": ["product_id", "product_name", "category", "brand",
                    "color", "size", "catalog_price", "cost_price"],
        "primary_key": "product_id",
        "update_columns": ["product_name", "category", "brand", "color", "size", "catalog_price", "cost_price"],
        "prepare": lambda df: df[[
            "product_id", "product_name", "category", "brand",
            "color", "size", "catalog_price", "cost_price"
        ]].drop_duplicates("product_id").values.tolist()
    },
    "orders": {
        "columns": ["sale_id", "sale_date", "date_id", "customer_id", "channel", "channel_campaigns"],
        "primary_key": "sale_id",
        "update_columns": ["sale_date", "date_id", "customer_id", "channel", "channel_campaigns"],
        "prepare": lambda df: df[[
            "sale_id", "sale_date", "date_id", "customer_id", "channel", "channel_campaigns"
        ]].drop_duplicates("sale_id").values.tolist()
    },
    "order_items": {
        "columns": ["item_id", "sale_id", "product_id", "quantity",
                    "original_price", "unit_price", "discount_applied",
                    "discount_percent", "discounted"],
        "primary_key": "item_id",
        "update_columns": ["sale_id", "product_id", "quantity", "original_price",
                           "unit_price", "discount_applied", "discount_percent", "discounted"],
        "prepare": lambda df: df[[
            "item_id", "sale_id", "product_id", "quantity",
            "original_price", "unit_price",
            "discount_applied", "discount_percent", "discounted"
        ]].values.tolist()
    }
}

def insert_table(pg_hook, table_name: str, df_filtered: pd.DataFrame) -> None:
    if df_filtered.empty:
        logger.info("Aucune donnée à insérer pour %s", table_name)
        return

    config = TABLE_CONFIG[table_name]
    records = config["prepare"](df_filtered)
    if not records:
        return

    columns = config["columns"]
    pk = config["primary_key"]
    updates = config["update_columns"]

    placeholders = ",".join(["%s"] * len(columns))
    columns_sql = ",".join(columns)
    if updates:
        update_sql = ",".join([f"{col} = EXCLUDED.{col}" for col in updates])
        on_conflict_sql = f"ON CONFLICT ({pk}) DO UPDATE SET {update_sql}"
    else:
        on_conflict_sql = f"ON CONFLICT ({pk}) DO NOTHING"

    sql = f"INSERT INTO sales.{table_name} ({columns_sql}) VALUES ({placeholders}) {on_conflict_sql}"

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, records)
        conn.commit()
    logger.info("Insertion terminée pour %s (%s lignes)", table_name, len(records))
"""

def insert_sales_data(pg_hook, df_filtered: pd.DataFrame) -> str:
    """Insertion idempotente dans PostgreSQL pour toutes les tables"""
    if df_filtered.empty:
        return "NO_DATA"

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # ------------------ dim_dates ------------------
            date_records = [
                (
                    int(pd.Timestamp(d).strftime("%Y%m%d")),
                    d,
                    d.year,
                    d.month,
                    d.day,
                    d.strftime("%A"),
                    d.isocalendar()[1],
                    (d.month - 1) // 3 + 1,
                    d.weekday() >= 5,
                    False,
                )
                for d in df_filtered["sale_date"].unique()
            ]
            cursor.executemany("""
                INSERT INTO sales.dim_dates (
                    date_id, full_date, year, month, day,
                    day_of_week, week_number, quarter, is_weekend, is_holiday
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (date_id) DO NOTHING
            """, date_records)

            # ------------------ customers ------------------
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

            # ------------------ products ------------------
            product_cols = [
                "product_id", "product_name", "category", "brand",
                "color", "size", "catalog_price", "cost_price"
            ]
            products = (
                df_filtered[product_cols]
                .drop_duplicates("product_id")
                .where(pd.notnull, None)
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

            # ------------------ orders ------------------
            order_cols = [
                "sale_id", "sale_date", "date_id",
                "customer_id", "channel", "channel_campaigns"
            ]
            orders = (
                df_filtered[order_cols]
                .drop_duplicates("sale_id")
                .where(pd.notnull, None)
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

            # ------------------ order_items ------------------
            temp_df = df_filtered.copy()
            items_cols = [
                "item_id", "sale_id", "product_id", "quantity",
                "original_price", "unit_price",
                "discount_applied", "discount_percent", "discounted"
            ]
            items = temp_df[items_cols].values.tolist()
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
    logger.info("Insertion terminée pour toutes les tables")
    return "SUCCESS"


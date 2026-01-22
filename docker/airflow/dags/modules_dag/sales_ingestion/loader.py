import pandas as pd
import logging

logger = logging.getLogger(__name__)

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


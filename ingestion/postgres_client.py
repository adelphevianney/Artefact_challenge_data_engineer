import pandas as pd
import os
from dotenv import load_dotenv
import psycopg
from config import POSTGRES, SCHEMA
from logger import setup_logger

logger = setup_logger(__name__)
# Charger le fichier .env
load_dotenv(".env")

# Récupérer les variables
password = os.getenv("POSTGRES_PASSWORD")


def get_connection():
    # Si vous utilisez localhost, assurez-vous que le conteneur Docker tourne.
    return psycopg.connect(
        conninfo=f"dbname=ecommerce user=postgres password={password} host=localhost port=5432"
    )


def upsert_customers(conn, df: pd.DataFrame):
    if df.empty:
        return

    # 1. Sélection des colonnes
    cols = ['customer_id', 'first_name', 'last_name', 'email', 'gender',
                'age_range', 'signup_date', 'country']

    # 2. Nettoyage des données
    # Remplace les NaN de Pandas par None pour PostgreSQL
    temp_df = df[cols].replace({pd.NA: None, float('nan'): None})

    # 3. Dédoublonnage
    # On garde une seule ligne par customer_id
    customers = temp_df.drop_duplicates(subset=['customer_id']).values.tolist()
    query = f"""
        INSERT INTO {SCHEMA}.customers (
            customer_id, first_name, last_name, email, gender,
            age_range, signup_date, country
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            gender = EXCLUDED.gender,
            age_range = EXCLUDED.age_range,
            signup_date = EXCLUDED.signup_date,
            country = EXCLUDED.country
    """
    with conn.cursor() as cur:
        cur.executemany(query, customers)
    logger.info(f"{len(customers)} clients upsertés")

def upsert_products(conn, df: pd.DataFrame):
    """
    Upsert des produits (table sales.products)
    """
    if df.empty:
        return

    # Sélection et dédoublonnage sur product_id
    cols = [
        'product_id', 'product_name', 'category', 'brand',
        'color', 'size', 'catalog_price', 'cost_price'
    ]
    products = df[cols].drop_duplicates(subset=['product_id']).values.tolist()

    query = f"""
        INSERT INTO {SCHEMA}.products (
            product_id, product_name, category, brand,
            color, size, catalog_price, cost_price
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            product_name   = EXCLUDED.product_name,
            category       = EXCLUDED.category,
            brand          = EXCLUDED.brand,
            color          = EXCLUDED.color,
            size           = EXCLUDED.size,
            catalog_price  = EXCLUDED.catalog_price,
            cost_price     = EXCLUDED.cost_price
    """

    with conn.cursor() as cur:
        cur.executemany(query, products)

    logger.info(f"{len(products)} produits upsertés")


def upsert_orders(conn, df: pd.DataFrame):
    """
    Upsert des en-têtes de commandes (table sales.orders)
    Attention : total_amount est une colonne GENERATED → on ne l'insert pas
    """
    if df.empty:
        return

    # On dédoublonne sur sale_id
    # On prend la première occurrence par sale_id (les autres champs devraient être identiques)
    cols = [
        'sale_id', 'sale_date', 'date_id', 'customer_id',
        'channel', 'channel_campaigns'
    ]
    orders = df[cols].drop_duplicates(subset=['sale_id']).values.tolist()

    query = f"""
        INSERT INTO {SCHEMA}.orders (
            sale_id, sale_date, date_id, customer_id, channel, channel_campaigns
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (sale_id) DO UPDATE SET
            sale_date          = EXCLUDED.sale_date,
            date_id          = EXCLUDED.date_id,
            customer_id        = EXCLUDED.customer_id,
            channel            = EXCLUDED.channel,
            channel_campaigns  = EXCLUDED.channel_campaigns
        -- total_amount est recalculé automatiquement via GENERATED
    """

    with conn.cursor() as cur:
        cur.executemany(query, orders)

    logger.info(f"{len(orders)} commandes (en-têtes) upsertées")


def upsert_dim_dates(conn, df: pd.DataFrame):
    """
    Upsert des dates présentes dans les données (table sales.dim_dates)
    """
    if df.empty:
        return

    # Extraction des dates uniques depuis sale_date
    dates = pd.to_datetime(df['sale_date']).dt.date.unique()

    date_records = []
    for d in dates:
        dt = pd.Timestamp(d)
        date_records.append((
            int(dt.strftime('%Y%m%d')),  # date_id
            d,  # full_date
            dt.year,
            dt.month,
            dt.day,
            dt.strftime('%A'),  # Monday, Tuesday...
            dt.isocalendar()[1],  # week number
            (dt.month - 1) // 3 + 1,  # quarter
            dt.weekday() > 5,  # is_weekend
            False  # is_holiday (à enrichir plus tard si besoin)
        ))

    query = f"""
        INSERT INTO {SCHEMA}.dim_dates (
            date_id, full_date, year, month, day,
            day_of_week, week_number, quarter, is_weekend, is_holiday
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
    """

    with conn.cursor() as cur:
        cur.executemany(query, date_records)

    logger.info(f"{len(date_records)} dates upsertées dans dim_dates")

def upsert_order_items(conn, df: pd.DataFrame):
    if df.empty:
        return

    temp_df = df.copy()

    # --- NETTOYAGE DU POURCENTAGE ---
    if temp_df['discount_percent'].dtype == 'object':
        # 1. On retire le signe %
        # 2. On convertit en float
        # Résultat : "15.50%" devient 15.50 (compatible DECIMAL(5,2))
        """
        temp_df['discount_percent'] = (
            temp_df['discount_percent']
            .str.replace('%', '', regex=False)
            .astype(float)
        )
        """
        temp_df['discount_percent'] = pd.to_numeric(
            temp_df['discount_percent'].str.replace('%', '', regex=False),
            errors='coerce'
        )

    # Sécurité supplémentaire : s'assurer qu'on ne dépasse pas 100
    temp_df['discount_percent'] = temp_df['discount_percent'].fillna(0.0)

    # --- AUTRES NETTOYAGES ---
    temp_df['discounted'] = temp_df['discounted'].astype(bool)
    temp_df = temp_df.where(pd.notnull(temp_df), None)


    cols_to_insert = [
        'item_id', 'sale_id', 'product_id', 'quantity',
        'original_price', 'unit_price', 'discount_applied',
        'discount_percent', 'discounted'
    ]
    items = temp_df[cols_to_insert].values.tolist()


    query = f"""
        INSERT INTO {SCHEMA}.order_items (
            item_id, sale_id, product_id, quantity, original_price,
            unit_price, discount_applied, discount_percent, discounted
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (item_id) DO UPDATE SET
            sale_id          = EXCLUDED.sale_id,
            product_id       = EXCLUDED.product_id,
            quantity         = EXCLUDED.quantity,
            original_price   = EXCLUDED.original_price,
            unit_price       = EXCLUDED.unit_price,
            discount_applied = EXCLUDED.discount_applied,
            discount_percent = EXCLUDED.discount_percent,
            discounted       = EXCLUDED.discounted
    """

    with conn.cursor() as cur:
        cur.executemany(query, items)

    logger.info(f"{len(items)} lignes de ventes upsertées")
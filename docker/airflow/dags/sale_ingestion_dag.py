from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # Compatible MinIO
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier #provider slack pour l'alerte e-mail
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'vianney',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Attendre 5 minutes entre chaque retry
    'retry_exponential_backoff': True,    # Augmente le délai progressivement
    'max_retry_delay': timedelta(hours=1), # Délai maximum entre retries
    'email': ['fofvianney15@gmail.com'],          # ← ton email
    'email_on_failure': True,                  # Envoie un email si échec définitif
    'email_on_retry': False,                    #  Email à chaque retry
}

with DAG(
        dag_id='sales_ingestion_incremental',
        default_args=default_args,
        description='Ingestion incrémentale des ventes e-commerce (Artefact Challenge)',
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['ecommerce', 'ingestion', 'artefact-challenge'],
        params={
            'target_date': '20250616'  # Date par défaut, modifiable lors du trigger
        }
) as dag:
    def ingest_sales_for_date(**context):
        """Tâche principale : ingestion pour une date donnée"""
        # 1. Récupération de la date cible (priorité : paramètre > execution_date)
        target_date_str = context['params'].get('target_date')
        if not target_date_str:
            target_date_str = context['execution_date'].strftime('%Y%m%d')

        logger.info(f"Ingestion démarrée pour la date : {target_date_str}")

        # 2. Connexions Airflow (configurées dans l'UI)
        minio_hook = S3Hook(aws_conn_id='minio_default')  # Connexion MinIO
        pg_hook = PostgresHook(postgres_conn_id='postgres_ecommerce')  # Connexion Postgres

        # 3. Lecture depuis MinIO
        bucket_name = 'foldersource'
        object_key = 'fashion_store_sales.csv'  # ← nom exact de ton fichier

        try:
            s3_object = minio_hook.get_key(key=object_key, bucket_name=bucket_name)
            df = pd.read_csv(BytesIO(s3_object.get()['Body'].read()))
            logger.info(f"Fichier lu : {len(df)} lignes totales")
        except Exception as e:
            logger.error(f"Erreur lecture MinIO : {str(e)}")
            raise

        # 4. Filtrage par date
        # Normalisation
        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce').dt.date

        # Filtrage incrémental
        target_date = pd.to_datetime(target_date_str, format='%Y%m%d').date()
        df_filtered = df[df['sale_date'] == target_date].copy()

        # Création de la clé date
        df_filtered['date_id'] = df_filtered['sale_date'].apply(
            lambda d: int(pd.Timestamp(d).strftime('%Y%m%d'))
        )

        if df_filtered.empty:
            logger.info("Aucune donnée pour cette date")
            return True

        logger.info(f"{len(df_filtered)} lignes à ingérer")

        # 5. Insertion / Upsert dans PostgreSQL
        conn = pg_hook.get_conn()
        try:
            cursor = conn.cursor()

            # Ordre important : respecter les FK
            # 1. dim_dates
            dates = pd.to_datetime(df_filtered['sale_date']).dt.date.unique()
            date_records = []
            for d in dates:
                dt = pd.Timestamp(d)
                date_records.append((
                    int(dt.strftime('%Y%m%d')),
                    d,
                    dt.year,
                    dt.month,
                    dt.day,
                    dt.strftime('%A'),
                    dt.isocalendar()[1],
                    (dt.month - 1) // 3 + 1,
                    dt.weekday() >= 5,
                    False
                ))

            cursor.executemany("""
                INSERT INTO sales.dim_dates (
                    date_id, full_date, year, month, day,
                    day_of_week, week_number, quarter, is_weekend, is_holiday
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_id) DO NOTHING
            """, date_records)

            # 2. customers
            cols = ['customer_id', 'first_name', 'last_name', 'email', 'gender',
                    'age_range', 'signup_date', 'country']
            temp_df = df_filtered[cols].replace({pd.NA: None, float('nan'): None})
            customers = temp_df.drop_duplicates(subset=['customer_id']).values.tolist()

            cursor.executemany("""
                INSERT INTO sales.customers (
                    customer_id, first_name, last_name, email,
                    gender, age_range, signup_date, country
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    gender = EXCLUDED.gender,
                    age_range = EXCLUDED.age_range,
                    signup_date = EXCLUDED.signup_date,
                    country = EXCLUDED.country
            """, customers)

            # 3. products
            products = df_filtered[[
                'product_id', 'product_name', 'category', 'brand',
                'color', 'size', 'catalog_price', 'cost_price'
            ]].drop_duplicates(subset=['product_id']).values.tolist()

            cursor.executemany("""
                INSERT INTO sales.products (
                    product_id, product_name, category, brand,
                    color, size, catalog_price, cost_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    brand = EXCLUDED.brand,
                    color = EXCLUDED.color,
                    size = EXCLUDED.size,
                    catalog_price = EXCLUDED.catalog_price,
                    cost_price = EXCLUDED.cost_price
            """, products)

            # 4. orders
            orders = df_filtered[[
                'sale_id', 'sale_date','date_id','customer_id', 'channel', 'channel_campaigns'
            ]].drop_duplicates(subset=['sale_id']).values.tolist()

            cursor.executemany("""
                INSERT INTO sales.orders (
                    sale_id, sale_date, date_id, customer_id, channel, channel_campaigns
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (sale_id) DO UPDATE SET
                    sale_date = EXCLUDED.sale_date,
                    date_id = EXCLUDED.date_id,
                    customer_id = EXCLUDED.customer_id,
                    channel = EXCLUDED.channel,
                    channel_campaigns = EXCLUDED.channel_campaigns
            """, orders)

            # 5. order_items
            temp_df = df.copy()

            # --- NETTOYAGE DU POURCENTAGE ---
            if temp_df['discount_percent'].dtype == 'object':
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


            temp_df['discount_percent'] = temp_df['discount_percent'].fillna(0.0)
            temp_df['discounted'] = temp_df['discounted'].astype(bool)
            temp_df = temp_df.where(pd.notnull(temp_df), None)

            cols_to_insert = [
                'item_id', 'sale_id', 'product_id', 'quantity',
                'original_price', 'unit_price', 'discount_applied',
                'discount_percent', 'discounted'
            ]
            items = temp_df[cols_to_insert].values.tolist()

            cursor.executemany("""
                INSERT INTO sales.order_items (
                    item_id, sale_id, product_id, quantity, original_price,
                    unit_price, discount_applied, discount_percent, discounted
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur insertion PostgreSQL : {str(e)}")
            raise
        finally:
            conn.close()


    # Définition de la tâche unique
    def slack_failure_callback(context):
        notifier = SlackNotifier(
            slack_conn_id='slack_default',
            text=(
                ":x: *Échec du DAG*\n"
                "*DAG* : {{ dag.dag_id }}\n"
                "*Task* : {{ task_instance.task_id }}\n"
                "*Execution date* : {{ ds }}\n"
                "*Erreur* : {{ exception }}"
            ),
            channel='#alertes-data',
        )
        notifier.notify(context)


    ingest_task = PythonOperator(
        task_id='ingest_sales_data',
        python_callable=ingest_sales_for_date,
        provide_context=True,
        email_on_failure=True,
        on_failure_callback=slack_failure_callback,
    )

    ingest_task
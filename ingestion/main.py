import sys
import argparse
from datetime import datetime
from logger import setup_logger
from minio_client import read_sales_file
from postgres_client import upsert_dim_dates, upsert_products, upsert_orders, upsert_customers, upsert_order_items
from postgres_client import get_connection


logger = setup_logger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Ingestion incrémentale des ventes")
    parser.add_argument("date", type=str, help="Date au format YYYYMMDD")
    return parser.parse_args()

def validate_date(date_str: str) -> str:
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return date_str
    except ValueError:
        logger.error("Format de date invalide. Utilisez YYYYMMDD (ex: 20250616)")
        sys.exit(1)

def main():
    args = parse_arguments()
    target_date = validate_date(args.date)

    logger.info(f"══════ DÉBUT INGESTION - Date: {target_date} ══════")

    try:
        # 1. Lecture et filtrage depuis MinIO
        df = read_sales_file(target_date)
        logger.info(f"Dataset filtré : {len(df)} lignes pour la date {target_date}")

        if df.empty :
            logger.info("Aucune donnée à ingérer pour cette date → fin du traitement")
            return

        # 2. Connexion PostgreSQL avec gestion transactionnelle complète
        conn = get_connection()

        try:

            logger.info("Connexion PostgreSQL établie → début des upserts (ordre respectant les FK)")

            # Ordre critique : respecter les dépendances de clés étrangères
            upsert_dim_dates(conn, df)
            logger.info("dim_dates → OK")

            upsert_customers(conn, df)
            logger.info("customers → OK")

            upsert_products(conn, df)
            logger.info("products → OK")

            upsert_orders(conn, df)
            logger.info("orders (en-têtes) → OK")

            upsert_order_items(conn, df)
            logger.info("order_items (lignes) → OK")

            # Tout s'est bien passé → on valide la transaction
            conn.commit()
            logger.info("💾 COMMIT FINAL → Toutes les données ont été persistées avec succès")

        except Exception as upsert_error:
            # En cas d'erreur → rollback pour éviter état incohérent
            conn.rollback()
            logger.error(f"❌ Échec lors d'un upsert : {str(upsert_error)}", exc_info=True)
            raise  # On relance pour que le bloc except global soit déclenché

        finally:
            # Toujours fermer la connexion, même en cas d'erreur
            conn.close()
            logger.debug("Connexion PostgreSQL fermée proprement")

        logger.info("🎉 ══════ INGESTION TERMINÉE AVEC SUCCÈS ══════")

    except Exception as global_error:
        logger.error(f"💥 ÉCHEC GLOBAL DE L'INGESTION : {str(global_error)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
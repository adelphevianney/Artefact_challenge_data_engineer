from pathlib import Path
import os
from dotenv import load_dotenv

# Charger le fichier .env
load_dotenv("../docker/.env")

# Récupérer les variables
access_key = os.getenv("ACCESS_KEY")
secret_key = os.getenv("SECRET_KEY")
password = os.getenv("PASSWORD")

# Configuration MinIO
MINIO = {
    "endpoint": "localhost:9000",
    "access_key": access_key,
    "secret_key": secret_key,
    "secure": False,
    "bucket": "foldersource",
    "file_key": "fashion_store_sales.csv"   #  nom exact du fichier uploadé
}

# Configuration PostgreSQL
POSTGRES = {
    "dbname": "ecommerce",
    "user": "postgres",
    "password": password,
    "host": "localhost",
    "port": "5432"
}

# Schéma cible
SCHEMA = "sales"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
from minio import Minio
from minio.error import S3Error
import pandas as pd
import io
from config import MINIO
from logger import setup_logger

logger = setup_logger(__name__)


def get_minio_client():
    return Minio(
        endpoint=MINIO["endpoint"],
        access_key=MINIO["access_key"],
        secret_key=MINIO["secret_key"],
        secure=MINIO["secure"]
    )



def read_sales_file(date_str: str) -> pd.DataFrame:
    """Lit le fichier CSV depuis MinIO et filtre par date"""
    client = get_minio_client()
    bucket = MINIO["bucket"]
    object_name = MINIO["file_key"]

    try:
        logger.info(f"Tentative de lecture de {object_name} dans bucket {bucket}")
        response = client.get_object(bucket, object_name)

        # Lecture complète
        df = pd.read_csv(io.BytesIO(response.read()))
        response.close()
        response.release_conn()

        # Conversion et filtrage
        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce').dt.date
        target_date = pd.to_datetime(date_str, format="%Y%m%d").date()

        filtered = df[df['sale_date'] == target_date].copy()

        filtered['date_id'] = filtered['sale_date'].apply(
            lambda d: int(pd.Timestamp(d).strftime('%Y%m%d'))
        )
        logger.info(f"{len(filtered)} lignes trouvées pour la date {target_date}")

        return filtered

    except S3Error as e:
        logger.error(f"Erreur MinIO: {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la lecture/filtrage: {e}")
        raise
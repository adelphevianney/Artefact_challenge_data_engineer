import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

def read_sales_data(minio_hook, bucket: str, key: str) -> pd.DataFrame:
    s3_object = minio_hook.get_key(key=key, bucket_name=bucket)
    df = pd.read_csv(BytesIO(s3_object.get()["Body"].read()))
    logger.info("%s lignes lues depuis MinIO", len(df))
    return df

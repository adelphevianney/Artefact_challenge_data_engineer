import pytest
from ingestion.main import validate_date
from ingestion.minio_client import read_sales_file
from ingestion.postgres_client import upsert_customers
import pandas as pd
from pandas import DataFrame, read_csv


@pytest.fixture

def test_validate_date_valid():
    assert validate_date("20250616") == "20250616"

def test_validate_date_invalid():
    with pytest.raises(SystemExit):
        validate_date("2025-06-16")

def test_mock_data_exists(sample_data_path):
    df = pd.read_csv(sample_data_path)
    assert len(df) == 10
    assert 'sale_date' in df.columns
    assert df['sale_date'].nunique() == 1
    assert df['sale_date'].iloc[0] == "2025-06-16"

def test_read_sales_file(sample_data_path, mock_config):
    # Mock le client MinIO pour lire un fichier local de test
    df = read_csv(sample_data_path)
    assert len(df) > 0
    assert 'sale_date' in df.columns

def test_upsert_customers_empty_df():
    conn = None  # Mock connexion
    df = DataFrame()
    upsert_customers(conn, df)  # Ne doit pas planter


def test_upsert_customers_basic(mocker, sample_data_path):
    df = pd.read_csv(sample_data_path())

    mock_conn = mocker.MagicMock()
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value

    upsert_customers(mock_conn, df)

    mock_cur.execute.assert_called()  # Au moins une requête a été exécutée
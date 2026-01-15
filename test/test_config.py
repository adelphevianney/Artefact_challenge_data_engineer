import pytest
from pathlib import Path

@pytest.fixture
def sample_data_path():
    """Chemin vers un petit fichier CSV de test"""
    return Path(__file__).parent / "data" / "mock_sales_20250616.csv"

@pytest.fixture
def mock_config(monkeypatch):
    """Mock des configurations sensibles"""
    monkeypatch.setattr("ingestion.config.MINIO", {
        "endpoint": "localhost:9000",
        "access_key": "test",
        "secret_key": "test",
        "secure": False,
        "bucket": "test_bucket",
        "file_key": "mock_sales_20250616.csv"
    })
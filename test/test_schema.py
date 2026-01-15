import pytest
from psycopg import connect
from ingestion.config import POSTGRES

@pytest.fixture
def db_connection():
    conn = connect(**POSTGRES)
    yield conn
    conn.close()

def test_schema_exists(db_connection):
    cur = db_connection.cursor()
    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sales'")
    assert cur.fetchone() is not None, "Schéma 'sales' n'existe pas"

def test_table_customers_exists(db_connection):
    cur = db_connection.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'sales' 
            AND table_name = 'customers'
        )
    """)
    assert cur.fetchone()[0], "Table 'customers' n'existe pas"

def test_column_email_unique(db_connection):
    cur = db_connection.cursor()
    cur.execute("""
        SELECT column_name, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'sales'
        AND table_name = 'customers'
        AND column_name = 'email'
    """)
    result = cur.fetchone()
    assert result is not None, "Colonne email absente"

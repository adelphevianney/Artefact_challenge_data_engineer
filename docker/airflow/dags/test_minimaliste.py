from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import logging

logger = logging.getLogger(__name__)

@dag(
    dag_id="test_minimal_airflow3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "debug"],
)
def test_minimal_airflow3():

    @task
    def hello_world():
        context = get_current_context()

        logger.info("🚀 Task démarrée")
        logger.info(f"DAG ID      : {context['dag'].dag_id}")
        logger.info(f"Run ID      : {context['run_id']}")
        logger.info(f"LogicalDate : {context['logical_date']}")
        logger.info("✅ Tout fonctionne correctement")

        return "OK"

    hello_world()

test_minimal_airflow3()

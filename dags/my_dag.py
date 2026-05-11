from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def process_data():
    logger.info("Starting data processing task")
    result = sum(range(1000))
    logger.info(f"Processing complete. Result: {result}")
    return result


def submit_spark_job():
    logger.info("Simulating Spark job submission via spark_submit")
    logger.info("spark_submit --master spark://spark-master:7077 --class org.example.App app.jar")
    logger.info("Spark job finished successfully")


def failing_task():
    logger.error("ERROR: Critical failure in pipeline step")
    raise ValueError("Simulated pipeline failure for monitoring demo")


with DAG(
    dag_id="my_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["lab4"],
) as dag:

    process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    spark_job = PythonOperator(
        task_id="spark_submit",
        python_callable=submit_spark_job,
    )

    fail = PythonOperator(
        task_id="failing_task",
        python_callable=failing_task,
    )

    process >> spark_job >> fail

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


with DAG(
    "gathering_posi_news_spark",
    description="Gathering news about POSI from smart-lab via Spark",
    schedule=timedelta(minutes=30),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["spark", "smartlab"],
) as dag:
    gather_posi_news = BashOperator(
        task_id="gather_posi_news_spark_job",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--name gather_posi_news_spark_job "
            "/opt/airflow/spark/smartlab_links_job.py"
        ),
    )

    gather_posi_news

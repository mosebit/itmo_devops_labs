# создаю по примеру из доки - https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html

import textwrap
from datetime import datetime, timedelta
# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG

from lxml import html
import requests
import json
from typing import Optional
import os

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "priority": "u=0, i",
    # "referer": f"https://smart-lab.ru/forum/news/{ticker}/page{doc_index-1}/",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
}
def fetch_raw_smartlab_post_links(ticker: str, doc_index: int) -> Optional[str]:
    url = f"https://smart-lab.ru/forum/news/{ticker}/page{doc_index}/"

    try:
        response = requests.get(url, headers=headers)
        return response.text
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None
    
def get_pretty_post_links(html_content):
    tree = html.fromstring(html_content)

    links = tree.xpath('//ul[@class="temp_headers temp_headers--have-numbers"]//a/@href')

    # /blog/1118401.php -> https://smart-lab.ru/blog/news/1118401.php
    full_links = [f"https://smart-lab.ru/blog/news/{link.split('/')[2]}" for link in links]

    return full_links

def load_links(saving_path, links):
    if not os.path.exists(saving_path):
        os.makedirs(os.path.dirname(saving_path), exist_ok=True)

    with open(saving_path, 'w', encoding='UTF-8') as f:
        json.dump(links, f, indent=4)

# СОЗДАНИЕ DAG
with DAG(
    "gathering_posi_news",
    # # These args will get passed on to each operator
    # # You can override them on a per-task basis during operator initialization
    # default_args={
    #     "depends_on_past": False,
    #     "retries": 1,
    #     "retry_delay": timedelta(minutes=5),
    #     # 'queue': 'bash_queue',
    #     # 'pool': 'backfill',
    #     # 'priority_weight': 10,
    #     # 'end_date': datetime(2016, 1, 1),
    #     # 'wait_for_downstream': False,
    #     # 'execution_timeout': timedelta(seconds=300),
    #     # 'on_failure_callback': some_function, # or list of functions
    #     # 'on_success_callback': some_other_function, # or list of functions
    #     # 'on_retry_callback': another_function, # or list of functions
    #     # 'sla_miss_callback': yet_another_function, # or list of functions
    #     # 'on_skipped_callback': another_function, #or list of functions
    #     # 'trigger_rule': 'all_success'
    # },
    description="Gathering news about POSI from smart-lab",
    schedule=timedelta(hours=0.5),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # определение операторов с тасками внутри 

    #     curl 'https://smart-lab.ru/forum/news/POSI/' \
    #   -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7' \
    #   -H 'accept-language: ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7' \
    #   -H 'cache-control: max-age=0' \
    #   -b '_count_uid=d1776711906636041xmqm1q6rkmixda4fwk5za5d2ec6rb6f3; cookies_warning=true' \
    #   -H 'priority: u=0, i' \
    #   -H 'sec-ch-ua: "Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"' \
    #   -H 'sec-ch-ua-mobile: ?0' \
    #   -H 'sec-ch-ua-platform: "Windows"' \
    #   -H 'sec-fetch-dest: document' \
    #   -H 'sec-fetch-mode: navigate' \
    #   -H 'sec-fetch-site: none' \
    #   -H 'sec-fetch-user: ?1' \
    #   -H 'upgrade-insecure-requests: 1' \
    #   -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'

    # Etl - extract
    raw_news = PythonOperator(
        task_id="fetch_smart-lab_posi_news",
        python_callable=fetch_raw_smartlab_post_links,
        op_kwargs={"ticker": "POSI", "doc_index": 0},
    )

    # eTl - transform
    new_posts_links = PythonOperator(
        task_id="parse_posts_links",
        python_callable=get_pretty_post_links,
        op_kwargs={"html_content": raw_news.output},
    )

    # etL - load
    saving_result = PythonOperator(
        task_id="save_links",
        python_callable=load_links,
        op_kwargs={
            "saving_path": "/opt/airflow/output/smartlab_new_links.json",
            "links": new_posts_links.output,
        },
    )
    
    # не до конца понял механизм зависимостей, пишу дефолтный вид
    raw_news >> new_posts_links >> saving_result

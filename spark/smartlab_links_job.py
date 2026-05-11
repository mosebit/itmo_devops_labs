import json
import os
from html.parser import HTMLParser
from typing import Iterable, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

from pyspark import SparkConf
from pyspark.sql import SparkSession


HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
}


class SmartlabNewsLinksParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._inside_target_list = False
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attrs_dict = dict(attrs)

        if tag == "ul":
            classes = set((attrs_dict.get("class") or "").split())
            self._inside_target_list = {
                "temp_headers",
                "temp_headers--have-numbers",
            }.issubset(classes)
            return

        if tag == "a" and self._inside_target_list:
            href = attrs_dict.get("href")
            if href:
                self.links.append(href)

    def handle_endtag(self, tag: str) -> None:
        if tag == "ul" and self._inside_target_list:
            self._inside_target_list = False


def fetch_raw_smartlab_post_links(ticker: str, doc_index: int) -> Optional[str]:
    url = f"https://smart-lab.ru/forum/news/{ticker}/page{doc_index}/"
    request = Request(url, headers=HEADERS)

    try:
        with urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8", errors="replace")
    except URLError as exc:
        print(f"Request failed: {exc}")
        return None


def parse_post_links(html_content: str) -> list[str]:
    parser = SmartlabNewsLinksParser()
    parser.feed(html_content)
    return [
        f"https://smart-lab.ru/blog/news/{link.split('/')[2]}"
        for link in parser.links
        if link.startswith("/blog/") and len(link.split("/")) > 2
    ]


def save_links(saving_path: str, links: Iterable[str]) -> None:
    os.makedirs(os.path.dirname(saving_path), exist_ok=True)

    with open(saving_path, "w", encoding="utf-8") as file:
        json.dump(list(links), file, indent=4, ensure_ascii=False)


def main() -> None:
    conf = (
        SparkConf()
        .setAppName("Smartlab POSI News Links")
        .setMaster("spark://spark-master:7077")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    try:
        html_content = fetch_raw_smartlab_post_links(ticker="POSI", doc_index=0)
        if not html_content:
            save_links("/opt/airflow/output/smartlab_new_links.json", [])
            return

        links = parse_post_links(html_content)
        if not links:
            save_links("/opt/airflow/output/smartlab_new_links.json", [])
            return

        save_links("/opt/airflow/output/smartlab_new_links.json", links)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

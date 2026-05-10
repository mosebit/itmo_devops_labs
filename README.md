### Описание репозитория
Dockerfile - файл для сборки Airflow, инструкция COPY не используется, так как DAG прокидывается через том в compose-файле:
```
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
```

### Описание разработанного DAG-файла

В качестве примера разработан даг, стягивающий последние новости по компании Positive Technologies на портале Smart-Lab. Скрипт получает страницу свежих новостей (extract) -> вытаскивает ссылки на конкретные посты (transform) -> сохраняет (load)

### Запуск

Одной командой (на Windows запускал через командную строку WSL):
```
sudo docker compose up -d
```

Пример отработанного дага:

[aiflow_web_screenshot](https://github.com/mosebit/itmo_devops_labs/blob/main/aiflow_web_screenshot.png)

# FROM apache/airflow:2.7.1
FROM apache/airflow:3.2.0

WORKDIR /opt/airflow

# перенос не требуется, тк в compose-файле уже есть "... /dags:/opt/airflow/dags"
# COPY dag_*.py /opt/airflow/dags/
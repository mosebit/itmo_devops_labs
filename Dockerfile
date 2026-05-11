# FROM apache/airflow:2.7.1
FROM apache/airflow:3.2.0

WORKDIR /opt/airflow

# перенос не требуется, тк в compose-файле уже есть "... /dags:/opt/airflow/dags"
# COPY dag_*.py /opt/airflow/dags/

# Переключаемся на админского пользователя, тк этого требует apt
USER root 

# это требуется, чтобы в контейнере airflow мог запускаться spark-job (spark-submit)
RUN apt update && apt -y install procps default-jre 
# Переключаемся обратно на юзера airflow, чтоб сам сервис работал корректно и не сломались пермишены
USER airflow 

# COPY ./dags/* ./dags/
# COPY ./spark/* ./spark/

# точно рабочие версии
RUN pip3 install apache-airflow-providers-apache-spark==4.1.1 pyspark==3.5.0 
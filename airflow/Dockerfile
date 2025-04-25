FROM apache/airflow:2.10.5

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-mysql \
    pandas \
    requests


USER root


RUN apt-get update \
 && apt-get install -y docker.io docker-compose-plugin \
 && rm -rf /var/lib/apt/lists/*

RUN usermod -aG docker airflow

USER airflow

# Bluetrends
Capstone project for Dev10 Data Engineering 2025-3 Cohort


# Steps to run:
1. Start up docker - `docker compose up -d` or `docker compose up --build -d`

2. Start the producer ` docker compose exec python python jetstream_producer.py`

3. Start spark processor `docker exec -it spark-master spark-submit /opt/spark-apps/jobs/jetstream_processor.py`

I have no name!@5622bbdb1314:/opt/spark-apps/jobs$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 jetstream_consumer.py


docker compose up -d --build dash

create airflow user;

```

```


docker compose exec airflow airflow db init

docker compose exec airflow airflow users create \
  --username admin \
  --firstname Taylor \
  --lastname Merwin \
  --role Admin \
  --email youremail@example.com \
  --password yourpassword

docker compose restart airflow

run airflow `docker compose up -d airflow` for webserver and `docker compose up -d airflow-scheduler`

airflow needs both the webserver and the scheduler to run.


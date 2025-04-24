# Bluetrends
Capstone project for Dev10 Data Engineering 2025-3 Cohort


# Steps to run:
1. Start up docker - `docker compose up -d` or `docker compose up --build -d`

2. Start the producer ` docker compose exec python python jetstream_producer.py`

3. Start spark processor `docker exec -it spark-master spark-submit /opt/spark-apps/jobs/jetstream_processor.py`

I have no name!@5622bbdb1314:/opt/spark-apps/jobs$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 jetstream_consumer.py


(bluetrends-py3.10) 16:08:49 taylor@pop-os:~/code/dev10/Bluetrends$ docker compose up -d --build dash

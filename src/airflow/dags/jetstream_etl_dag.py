# src/airflow/dags/jetstream_etl.py

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='jetstream_etl_dagg',
    description='Launch the Bluesky Jetstream producer and processor as continuous streams',
    schedule_interval=None,        # no periodic schedule—manual trigger only
    start_date=datetime(2025, 4, 24, 0, 0, 0),
    catchup=False,
    tags=['etl', 'streaming'],
) as dag:
    producer = BashOperator(
        task_id='jetstream_producer',
        bash_command=(
            'docker compose exec python '
            'python /home/app/jetstream_producer.py'
        )
    )
    processor = BashOperator(
        task_id='jetstream_processor',
        bash_command=(
            'docker exec spark-master '
            'spark-submit /opt/spark-apps/jobs/jetstream_processor.py'
        )
    )

    # start producer first, then fire up the processor
    producer >> processor

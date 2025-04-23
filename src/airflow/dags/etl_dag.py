from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# default args for retries, owner, etc.
default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='jetstream_etl',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',    # or '@daily', '@hourly', etc.
    catchup=False,
    tags=['etl']
) as dag:

    start_producer = BashOperator(
        task_id='start_jetstream_producer',
        # -d = detached so Airflow task finishes immediately
        bash_command=(
            "docker exec -d python "
            "python /home/app/jetstream_producer.py"
        )
    )

    start_spark = BashOperator(
        task_id='start_spark_processor',
        bash_command=(
            "docker exec -d spark-master "
            "spark-submit /opt/spark-apps/jobs/jetstream_processor.py"
        )
    )

    # Ensure producer is up before kick‑off Spark job
    start_producer >> start_spark
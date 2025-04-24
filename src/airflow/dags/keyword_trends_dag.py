from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_keyword_trends",
    default_args=default_args,
    description="Generate daily keyword trends from Bluesky posts",
    schedule_interval="@daily",
    start_date=datetime(2025, 4, 23),
    catchup=False,
    tags=["spark", "etl", "keywords"]
) as dag:

    run_trends = SparkSubmitOperator(
        task_id="spark_keyword_trends",
        application="/path/to/keyword_trends_job.py",
        name="keyword_trends_job",
        conf={"spark.driver.memory": "2g"},
        application_args=[
            # Airflow macros to pass yesterday’s window
            "--start", "{{ ds }} 00:00:00",
            "--end",   "{{ next_ds }} 00:00:00"
        ],
        # adjust these to match your cluster
        conn_id="spark_default",
        verbose=False
    )

    run_trends

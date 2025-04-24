# src/airflow/dags/keyword_trends_dag.py

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'taylor',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='keyword_trends_hourly',
    default_args=default_args,
    description='Aggregate last hour of posts every hour',
    schedule_interval='0 * * * *',            # on the hour
    start_date=datetime(2025, 4, 24, 6, 0, 0), # first run at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'spark'],
    render_template_as_native_obj=True,        # so data_interval_* stay datetime objects
) as dag:

    run_spark = BashOperator(
        task_id='submit_keyword_trends_job',
        bash_command=(
            'spark-submit '
            '--master spark://spark-master:7077 '
            '--conf spark.jars.packages=mysql:mysql-connector-java:8.0.33 '
            '/opt/spark-apps/jobs/keyword_trends_job.py '
            '--start "{{ data_interval_start.strftime(\'%Y-%m-%d %H:%M:%S\') }}" '
            '--end   "{{ data_interval_end.strftime(\'%Y-%m-%d %H:%M:%S\') }}"'
        ),
    )
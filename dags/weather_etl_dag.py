from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
import os


iceberg_data_path = "/Users/milesjyoung/developer/cal_solar/test_lead_gen/workflow-test/iceberg/data" #for local dev

import sys
sys.path.append("/opt/airflow")  # Allows importing etl.extract etc.

from etl.extract import extract_csv
from etl.transform import transform_data
from etl.load import load_to_postgres

with DAG(
    dag_id="weather_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl"],
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract_csv)
    t2 = PythonOperator(task_id="transform", python_callable=transform_data)
    t3 = PythonOperator(task_id="load", python_callable=load_to_postgres)
    tscrape = DockerOperator(
        task_id="scrape_data",
        image="local-scraper",
        command="python scrape.py",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="workflow-test_default",
        mounts=[
            Mount(source=iceberg_data_path, target="/data", type="bind")
        ]
    )
    tappend = DockerOperator(
        task_id="append_to_iceberg",
        image="iceberg-runner",
        command="spark-submit /app/append_batches.py",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",  # Or your compose network
        mount_tmp_dir=False,
        mounts=[
            Mount(source=iceberg_data_path, target="/data", type="bind")
        ]
    )

    t1 >> t2 >> t3 >> tscrape >> tappend
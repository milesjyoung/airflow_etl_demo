from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
import os


iceberg_data_path = "/Users/milesjyoung/developer/cal_solar/test_lead_gen/workflow-test/iceberg/data" #for local dev

with DAG(
    dag_id="permit_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["etl"],
) as dag:

    
    scrape = DockerOperator(
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
    append = DockerOperator(
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
    resolve = DockerOperator(
        task_id="resolve_licenses",
        image="resolve-license-runner",
        command="spark-submit /app/match_license.py",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=iceberg_data_path, target="/data", type="bind")
        ]
    )
    extract = DockerOperator(
        task_id="extract_permit_data",
        image="extract-ml-runner",
        command="spark-submit /app/extract_append.py",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=iceberg_data_path, target="/data", type="bind")
        ]
    )

    scrape >> append >> resolve >> extract
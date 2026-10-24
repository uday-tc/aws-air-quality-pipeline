"""Airflow DAG: hourly-refresh air-quality pipeline.

ingest -> transform -> quality_gate -> load_warehouse -> publish_to_s3

Deploy by copying this file to your Airflow `dags/` folder (or MWAA's S3 dags
bucket) and making the `pipeline` package importable (pip install -e .).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from pipeline import ingest, load, quality, transform

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure": False,  # set True + SMTP in production
}

with DAG(
    dag_id="air_quality_pipeline",
    description="Ingest, clean, validate and load Indian city air-quality data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["aws", "data-engineering", "air-quality"],
) as dag:
    t_ingest = PythonOperator(task_id="ingest_raw", python_callable=ingest.ingest,
                              op_kwargs={"source": "api"})
    t_transform = PythonOperator(task_id="transform_clean", python_callable=transform.run)
    # No retries on the quality gate: re-running cannot fix bad data.
    t_quality = PythonOperator(task_id="quality_gate", python_callable=quality.run, retries=0)
    t_load = PythonOperator(task_id="load_warehouse", python_callable=load.run)
    t_publish = BashOperator(
        task_id="publish_to_s3",
        bash_command="aws s3 sync {{ var.value.get('pipeline_data_dir', 'data') }} "
                     "s3://{{ var.value.get('pipeline_bucket') }}/ --exclude 'warehouse.duckdb'",
    )

    t_ingest >> t_transform >> t_quality >> t_load >> t_publish

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount

HOST_PROJECT_DIR = os.environ["AIRFLOW_PROJ_DIR"]
CONTAINER_PROJECT_DIR = "/opt/project"
SPARK_JOB_IMAGE = os.environ.get("SPARK_JOB_IMAGE", "nyc-taxi-spark-dev")

RAW_DATA_PATH = f"{CONTAINER_PROJECT_DIR}/data/raw/bronze/nyc_taxi/yellow"
BRONZE_PATH = f"{CONTAINER_PROJECT_DIR}/data/output/bronze/nyc_taxi/yellow"
SILVER_PATH = f"{CONTAINER_PROJECT_DIR}/data/output/silver/nyc_taxi/yellow"
GOLD_DAILY_PATH = f"{CONTAINER_PROJECT_DIR}/data/output/gold/nyc_taxi/yellow_daily"
GOLD_ZONE_PATH = f"{CONTAINER_PROJECT_DIR}/data/output/gold/nyc_taxi/yellow_by_pickup_zone"
ZONES_CSV = f"{CONTAINER_PROJECT_DIR}/data/dim/taxi_zone_lookup.csv"

def spark_job(task_id: str, cmd: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=SPARK_JOB_IMAGE,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        network_mode="bridge",
        mounts=[
            Mount(
                source=HOST_PROJECT_DIR,
                target=CONTAINER_PROJECT_DIR,
                type="bind",
            )
        ],
        environment={"TZ": "America/Chicago"},
        command=f"bash -lc '{cmd}'",
    )

with DAG(
    dag_id="nyc_taxi_yellow_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"ingest_month": Param("2024-01", type="string", description="YYYY-MM")},
    tags=["spark", "etl", "nyc-taxi"],
) as dag:

    with TaskGroup("bronze") as bronze:
        download = spark_job(
            "download_raw_data",
        f"python {CONTAINER_PROJECT_DIR}/scripts/jobs/00_download_tlc_yellow.py "
            f"--ingest_month '{{{{ params.ingest_month }}}}' "
            f"--format parquet "
            f"--data_dir '{RAW_DATA_PATH}'",
        )

        ingest = spark_job(
            "ingest_yellow",
            f"python {CONTAINER_PROJECT_DIR}/scripts/jobs/01_bronze_ingest_yellow.py "
            f"--ingest_month '{{{{ params.ingest_month }}}}' "
            f"--format parquet "
            f"--output_path '{BRONZE_PATH}'",
        )

        download >> ingest

    with TaskGroup("silver") as silver:
        clean = spark_job(
            "clean_yellow",
            f"python {CONTAINER_PROJECT_DIR}/scripts/jobs/02_silver_clean_yellow.py "
            f"--ingest_month '{{{{ params.ingest_month }}}}' "
            f"--bronze_path '{BRONZE_PATH}' "
            f"--silver_path '{SILVER_PATH}'",
        )

    with TaskGroup("gold") as gold:
        daily = spark_job(
            "daily_metrics",
            f"python {CONTAINER_PROJECT_DIR}/scripts/jobs/03_gold_daily_metrics_yellow.py "
            f"--ingest_month '{{{{ params.ingest_month }}}}' "
            f"--silver_path '{SILVER_PATH}' "
            f"--gold_path '{GOLD_DAILY_PATH}'",
        )

        by_zone = spark_job(
            "daily_by_pickup_zone",
            f"python {CONTAINER_PROJECT_DIR}/scripts/jobs/04_gold_daily_by_pickup_zone_yellow.py "
            f"--ingest_month '{{{{ params.ingest_month }}}}' "
            f"--silver_path '{SILVER_PATH}' "
            f"--zones_path '{ZONES_CSV}' "
            f"--gold_path '{GOLD_ZONE_PATH}'",
        )

        daily >> by_zone

    bronze >> silver >> gold
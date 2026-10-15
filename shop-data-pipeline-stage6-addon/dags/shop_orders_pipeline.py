"""Run the verified shop jobs sequentially in their own Docker containers."""
import json
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

config_path = Path(os.environ.get("SHOP_JOB_CONFIG", "/opt/project/job-config/jobs.json"))
settings = json.loads(config_path.read_text(encoding="utf-8"))
if settings.get("version") != 1:
    raise ValueError("Re-run stage 6 preparation to generate job settings version 1.")


def job(task_id, service, command=None):
    spec = settings["jobs"][service]
    return DockerOperator(
        task_id=task_id,
        image=spec["image"],
        command=command if command is not None else spec["command"],
        entrypoint=spec["entrypoint"],
        user=spec["user"],
        working_dir=spec["working_dir"],
        environment=spec["environment"],
        private_environment=spec["private_environment"],
        mounts=[Mount(**mount) for mount in spec["mounts"]],
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        docker_conn_id=None,
        network_mode=spec["network"],
        mount_tmp_dir=False,
        auto_remove="force",
        force_pull=False,
        mem_limit="2g" if service != "warehouse" else "512m",
        do_xcom_push=False,
        timeout=120,
        labels={"shop.pipeline": "airflow", "shop.task": task_id},
    )


with DAG(
    dag_id="shop_orders_pipeline",
    description="Kafka → bronze → silver → gold → PostgreSQL, with verification.",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "omar", "retries": 2, "retry_delay": timedelta(seconds=30),
                  "execution_timeout": timedelta(minutes=20)},
    tags=["shop", "data-engineering"],
) as dag:
    bronze = job("ingest_bronze", "spark")
    check_bronze = job("verify_bronze", "spark", ["/opt/project/spark/inspect_bronze.py"])
    silver = job("build_silver", "silver")
    check_silver = job("verify_silver", "silver", ["/opt/project/silver-jobs/inspect_silver.py"])
    gold = job("build_gold", "gold")
    check_gold = job("verify_gold", "gold", ["/opt/project/gold-jobs/inspect_gold.py"])
    warehouse = job("load_postgres", "warehouse")
    check_warehouse = job("verify_postgres", "warehouse", ["inspect_postgres.py"])

    bronze >> check_bronze >> silver >> check_silver >> gold >> check_gold >> warehouse >> check_warehouse

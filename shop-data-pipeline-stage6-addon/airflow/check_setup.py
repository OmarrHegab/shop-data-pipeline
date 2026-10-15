"""Check Docker access, required images/services, and DAG import before startup."""
import json
import os
import runpy
from pathlib import Path
import docker

settings = json.loads(Path(os.environ["SHOP_JOB_CONFIG"]).read_text())
client = docker.from_env(version="auto")
try:
    client.ping()
    for job in settings["jobs"].values():
        client.images.get(job["image"])
        client.networks.get(job["network"])
        for mount in job["mounts"]:
            if mount["type"] == "volume":
                client.volumes.get(mount["source"])
    for service in ["kafka", "postgres"]:
        instances = client.containers.list(filters={"label": [
            "com.docker.compose.project=shop-data-pipeline",
            f"com.docker.compose.service={service}",
        ]})
        if len(instances) != 1:
            raise RuntimeError(f"Start the {service} service before using Airflow.")
        if instances[0].attrs.get("State", {}).get("Health", {}).get("Status") != "healthy":
            raise RuntimeError(f"{service} is not healthy.")
    namespace = runpy.run_path("/opt/airflow/dags/shop_orders_pipeline.py")
    if len(namespace["dag"].tasks) != 8:
        raise RuntimeError("Expected eight pipeline tasks.")
    print("AIRFLOW SETUP CHECK PASSED", flush=True)
finally:
    client.close()

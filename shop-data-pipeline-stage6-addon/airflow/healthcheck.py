"""Report ready only when Airflow's database, scheduler, and parser are healthy."""
import json
from urllib.request import urlopen

with urlopen("http://127.0.0.1:8080/api/v2/monitor/health", timeout=5) as response:
    report = json.load(response)
for component in ["metadatabase", "scheduler", "dag_processor"]:
    if report.get(component, {}).get("status") != "healthy":
        raise SystemExit(f"Airflow {component} is not healthy yet.")

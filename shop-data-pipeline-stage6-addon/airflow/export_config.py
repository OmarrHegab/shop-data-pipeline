"""Translate real Docker inspect output into reusable Airflow job settings."""
import argparse
import json
from pathlib import Path

SERVICES = ["spark", "silver", "gold", "warehouse"]


def export_jobs(containers):
    jobs = {}
    for container in containers:
        config = container["Config"]
        labels = config.get("Labels") or {}
        service = labels.get("com.docker.compose.service")
        if service not in SERVICES:
            continue
        if service in jobs:
            raise ValueError(f"More than one template found for {service}.")
        networks = list(container["NetworkSettings"]["Networks"])
        if len(networks) != 1:
            raise ValueError(f"{service} must use the single project network.")
        mounts = []
        for mount in container["Mounts"]:
            kind = mount["Type"]
            if kind not in ["bind", "volume"]:
                raise ValueError(f"Unsupported {kind} mount on {service}.")
            source = mount["Name"] if kind == "volume" else mount["Source"]
            mounts.append({"source": source, "target": mount["Destination"],
                           "type": kind, "read_only": not mount["RW"]})
        environment, private = {}, {}
        for item in config.get("Env") or []:
            key, value = item.split("=", 1)
            (private if key == "PGPASSWORD" else environment)[key] = value
        jobs[service] = {
            "image": config["Image"], "entrypoint": config.get("Entrypoint"),
            "command": config.get("Cmd"), "user": config.get("User") or None,
            "working_dir": config.get("WorkingDir") or None,
            "network": networks[0], "mounts": mounts,
            "environment": environment, "private_environment": private,
        }
    if set(jobs) != set(SERVICES):
        raise ValueError(f"Missing template services: {set(SERVICES) - set(jobs)}")
    if len({job["network"] for job in jobs.values()}) != 1:
        raise ValueError("Job templates do not share one network.")
    return {"version": 1, "jobs": jobs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input")
    parser.add_argument("output")
    args = parser.parse_args()
    containers = json.loads(Path(args.input).read_text(encoding="utf-8-sig"))
    result = export_jobs(containers)
    if args.output == "-":
        print(json.dumps(result), flush=True)
    else:
        Path(args.output).write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("AIRFLOW JOB CONFIGURATION CREATED", flush=True)


if __name__ == "__main__":
    main()

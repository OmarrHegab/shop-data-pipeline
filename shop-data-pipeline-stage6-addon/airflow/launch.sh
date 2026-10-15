#!/usr/bin/env bash
set -euo pipefail

# Give the normal airflow user access to this Docker Desktop socket's group.
# Root is used only for this startup step, then gosu drops to the airflow user.
if [[ -S /var/run/docker.sock ]]; then
    socket_gid="$(stat -c '%g' /var/run/docker.sock)"
    if ! getent group "${socket_gid}" >/dev/null; then
        groupadd --gid "${socket_gid}" shop-docker
    fi
    usermod --append --groups "${socket_gid}" airflow
fi
mkdir -p /opt/airflow/logs /opt/airflow/auth
if [[ ! -f /opt/airflow/auth/passwords.json ]]; then
    printf '%s\n' '{"omar":"airflow"}' > /opt/airflow/auth/passwords.json
fi
chown -R airflow:0 /opt/airflow/logs /opt/airflow/auth
exec gosu airflow /entrypoint "$@"

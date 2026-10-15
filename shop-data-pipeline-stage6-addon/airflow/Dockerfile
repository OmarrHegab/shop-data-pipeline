FROM apache/airflow:3.3.2-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends gosu \
    && rm -rf /var/lib/apt/lists/*
COPY constraints.txt /tmp/shop-airflow-constraints.txt
USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" \
    "apache-airflow-providers-docker==4.5.9" "psycopg2-binary" \
    --constraint /tmp/shop-airflow-constraints.txt
USER root
COPY launch.sh healthcheck.py export_config.py check_setup.py /opt/project/airflow/
RUN sed -i 's/\r$//' /opt/project/airflow/launch.sh \
    && chmod 755 /opt/project/airflow/launch.sh
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/project/airflow/launch.sh"]
CMD ["standalone"]

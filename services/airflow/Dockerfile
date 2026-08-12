ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.10.5-python3.10
FROM ${AIRFLOW_BASE_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==5.0.0 \
    pyspark==3.5.6 \
    clickhouse-connect==0.8.18 \
    requests==2.32.3

ENV PYTHONPATH=/opt/deliveryflow/src
COPY --chown=airflow:0 src/etl /opt/deliveryflow/src/etl

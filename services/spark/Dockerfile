ARG SPARK_BASE_IMAGE=apache/spark:3.5.6-java17
FROM ${SPARK_BASE_IMAGE}

USER root
RUN pip install --no-cache-dir clickhouse-connect==0.8.18 PyYAML==6.0.2

ENV PYTHONPATH=/opt/deliveryflow/src
COPY configs/platform.yaml /opt/deliveryflow/configs/platform.yaml
COPY configs/datasets /opt/deliveryflow/configs/datasets
COPY src/deliveryflow_config /opt/deliveryflow/src/deliveryflow_config
COPY src/etl /opt/deliveryflow/src/etl

USER spark

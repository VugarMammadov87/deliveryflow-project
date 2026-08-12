ARG SPARK_BASE_IMAGE=apache/spark:3.5.6-java17
FROM ${SPARK_BASE_IMAGE}

USER root
RUN pip install --no-cache-dir clickhouse-connect==0.8.18

ENV PYTHONPATH=/opt/deliveryflow/src
COPY src/etl /opt/deliveryflow/src/etl

USER spark

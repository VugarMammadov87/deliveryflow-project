ARG SUPERSET_BASE_IMAGE=apache/superset:4.1.2
FROM ${SUPERSET_BASE_IMAGE}

USER root
RUN pip install --no-cache-dir clickhouse-connect==0.8.18

USER superset

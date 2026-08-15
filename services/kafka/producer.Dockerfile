FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app/src

COPY src /app/src
COPY scripts /app/scripts
COPY configs/superset /app/configs/superset

RUN pip install --no-cache-dir kafka-python==2.0.2 clickhouse-connect==0.8.18 psycopg2-binary==2.9.10 PyYAML==6.0.2 requests==2.32.3

CMD ["python", "-m", "producers.synthetic_logistics_producer"]

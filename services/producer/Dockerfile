FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY configs/platform.yaml /app/configs/platform.yaml
COPY src/deliveryflow_config /app/src/deliveryflow_config
COPY src/producers /app/src/producers

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.10 \
    PyYAML==6.0.2

CMD ["python", "-m", "producers.synthetic_logistics_producer"]

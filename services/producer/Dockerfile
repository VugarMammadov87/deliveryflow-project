FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY src/producers /app/src/producers

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.10

CMD ["python", "-m", "producers.synthetic_logistics_producer"]

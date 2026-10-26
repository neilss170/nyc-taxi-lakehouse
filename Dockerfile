FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    LAKEHOUSE_DATA=/data

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Resolve the Delta Lake jars once at build time so every run starts offline-ready.
COPY src/lakehouse/spark.py /tmp/warm/lakehouse/spark.py
RUN touch /tmp/warm/lakehouse/__init__.py \
    && PYTHONPATH=/tmp/warm python -c "from lakehouse.spark import get_spark; get_spark('warm').stop()" \
    && rm -rf /tmp/warm

EXPOSE 4040

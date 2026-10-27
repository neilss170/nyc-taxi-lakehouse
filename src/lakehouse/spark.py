"""SparkSession factory with Delta Lake enabled."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Structured Streaming's Kafka source and sink, matched to the pyspark version (Scala 2.13 build)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"


def get_spark(app_name: str = "nyc-taxi-lakehouse", conf: dict[str, str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        # TLC timestamps are NYC wall-clock times with no zone; UTC keeps them unshifted.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

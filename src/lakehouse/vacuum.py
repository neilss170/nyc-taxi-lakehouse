"""Delete data files no longer referenced by any retained Delta version (left by month reloads)."""

from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from lakehouse import config


def run(spark: SparkSession) -> dict:
    # default keeps 7 days of time travel; LAKEHOUSE_VACUUM_HOURS=0 is only safe with no concurrent readers
    hours = float(os.getenv("LAKEHOUSE_VACUUM_HOURS", "168"))
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", str(hours >= 168).lower())
    p = config.paths()
    tables = [p.bronze, p.silver, p.quarantine]
    for path in tables:
        DeltaTable.forPath(spark, path).vacuum(hours)
    return {"tables": len(tables), "retention_hours": hours}

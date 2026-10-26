"""Structured Streaming replay: per-zone 15-minute demand, upserted into Delta.

A month of silver trips is split into one Parquet file per pickup day and fed to a file
stream one file per micro-batch. Windowed counts run under an event-time watermark and
each micro-batch is MERGEd into a Delta table, so replays and restarts stay idempotent.
The result is then checked against the same aggregate computed in batch.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import config

WINDOW = "15 minutes"
WATERMARK = "1 hour"
STREAM_COLUMNS = ["pickup_ts", "pu_location_id", "total_amount"]


def _dirs() -> dict[str, Path]:
    root = config.paths().root / "stream"
    return {"landing": root / "landing", "checkpoint": root / "checkpoint", "staging": root / "staging"}


def stage_days(spark: SparkSession, month: str) -> list[str]:
    """Write one file per pickup day into the landing folder, oldest first (file stream reads by mtime)."""
    d = _dirs()
    for path in d.values():
        shutil.rmtree(path, ignore_errors=True)
    d["landing"].mkdir(parents=True)
    trips = spark.read.format("delta").load(config.paths().silver).where(F.col("source_month") == month)
    days = sorted(str(r.pickup_date) for r in trips.select("pickup_date").distinct().collect())
    for day in days:
        staging = d["staging"] / day
        trips.where(F.col("pickup_date") == day).select(*STREAM_COLUMNS).coalesce(1) \
            .write.mode("overwrite").parquet(str(staging))
        part = next(staging.glob("part-*.parquet"))
        os.replace(part, d["landing"] / f"trips_{day}.parquet")
    shutil.rmtree(d["staging"], ignore_errors=True)
    return days


def windowed_counts(stream: DataFrame) -> DataFrame:
    return (
        stream.withWatermark("pickup_ts", WATERMARK)
        .groupBy(F.window("pickup_ts", WINDOW), F.col("pu_location_id").alias("location_id"))
        .agg(F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue"))
        .select(F.col("window.start").alias("window_start"), "location_id", "trips", "revenue")
    )


def _upsert(target: str):
    def merge(batch: DataFrame, batch_id: int) -> None:
        # update mode emits the full current value of each changed window, so MERGE sets rather than adds
        DeltaTable.forPath(batch.sparkSession, target).alias("t").merge(
            batch.alias("s"), "t.window_start = s.window_start AND t.location_id = s.location_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    return merge


def run_stream(spark: SparkSession, target: str) -> list[dict]:
    """Process every file waiting in landing (availableNow), resuming from the checkpoint."""
    d = _dirs()
    schema = spark.read.parquet(str(d["landing"])).schema
    # state store partitions are fixed by the first run's checkpoint; 200 is far too many per-day batches
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    query = (
        windowed_counts(spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).parquet(str(d["landing"])))
        .writeStream.outputMode("update")
        .foreachBatch(_upsert(target))
        .option("checkpointLocation", str(d["checkpoint"]))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return [p for p in query.recentProgress if p["numInputRows"] > 0]


def dropped_by_watermark(progress: list[dict]) -> int:
    return sum(op.get("numRowsDroppedByWatermark", 0) for p in progress for op in p.get("stateOperators", []))


def replay(spark: SparkSession, month: str, target: str | None = None) -> dict:
    d = _dirs()
    target = target or config.paths().gold("stream_zone_15min")
    days = stage_days(spark, month)

    schema = spark.read.parquet(str(d["landing"])).schema
    windowed_counts(spark.createDataFrame([], schema)) \
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    progress = run_stream(spark, target)

    streamed = spark.read.format("delta").load(target)
    expected = windowed_counts(spark.read.parquet(str(d["landing"])))
    mismatched = (
        expected.alias("e").join(streamed.alias("s"), ["window_start", "location_id"], "full_outer")
        .where(~F.col("e.trips").eqNullSafe(F.col("s.trips")))
        .count()
    )
    rows = sum(p["numInputRows"] for p in progress)
    seconds = sum(p["durationMs"].get("triggerExecution", 0) for p in progress) / 1000
    return {
        "month": month,
        "files_replayed": len(days),
        "micro_batches": len(progress),
        "rows_streamed": rows,
        "rows_per_second": round(rows / seconds) if seconds else None,
        "rows_dropped_by_watermark": dropped_by_watermark(progress),
        "windows_written": streamed.count(),
        "windows_mismatched_vs_batch": mismatched,
        "window": WINDOW,
        "watermark": WATERMARK,
    }


def run(spark: SparkSession) -> dict:
    return replay(spark, os.getenv("LAKEHOUSE_STREAM_MONTH", config.months()[-1]))

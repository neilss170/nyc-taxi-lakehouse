"""Kafka replay: the streaming check again, with trips published to a Kafka topic instead of landing as files.

A month of silver trips is produced to a fresh topic keyed by pickup zone, in event-time order, so every
partition is time-ordered the way a real feed per zone would be. Structured Streaming consumes it with the
same 15-minute windows, watermark and Delta MERGE as the file replay. The month goes in two halves with a
consumer run after each: the second run is a restart that must pick up from the checkpointed offsets.
The result is checked against the batch aggregate and the consumed offsets against what was produced.
"""

from __future__ import annotations

import ast
import os
import shutil
import time
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import config
from lakehouse.streaming import STREAM_COLUMNS, WATERMARK, WINDOW, _upsert, dropped_by_watermark, windowed_counts

VALUE_SCHEMA = "pickup_ts TIMESTAMP, pu_location_id INT, total_amount DOUBLE"
MAX_OFFSETS_PER_TRIGGER = 120_000  # about one day of December 2024 per micro-batch


def bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")


def produce(trips: DataFrame, topic: str) -> int:
    """Publish trips keyed by zone, oldest first. One writer task keeps every partition in event-time order."""
    rows = trips.select(*STREAM_COLUMNS).repartition(1).sortWithinPartitions("pickup_ts")
    rows.select(F.col("pu_location_id").cast("string").alias("key"),
                F.to_json(F.struct(*STREAM_COLUMNS)).alias("value")) \
        .write.format("kafka").option("kafka.bootstrap.servers", bootstrap()).option("topic", topic).save()
    return rows.count()


def consume(spark: SparkSession, topic: str, target: str, checkpoint: str,
            max_offsets: int = MAX_OFFSETS_PER_TRIGGER) -> list[dict]:
    """Process everything in the topic past the checkpointed offsets (availableNow), MERGEing into target."""
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    trips = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap())
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
        .select(F.from_json(F.col("value").cast("string"), VALUE_SCHEMA).alias("t")).select("t.*")
    )
    query = (
        windowed_counts(trips).writeStream.outputMode("update")
        .foreachBatch(_upsert(target))
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return [p for p in query.recentProgress if p["numInputRows"] > 0]


def end_offsets(progress: list[dict], topic: str) -> dict[str, int]:
    """The last micro-batch's end offset per partition: how far the checkpoint has consumed."""
    if not progress:
        return {}
    end = progress[-1]["sources"][0]["endOffset"]
    # pyspark keeps it as str() of the parsed dict (single quotes), which literal_eval reads, as it does JSON
    return (ast.literal_eval(end) if isinstance(end, str) else end)[topic]


def replay(spark: SparkSession, month: str, split_day: int = 16, target: str | None = None,
           max_offsets: int = MAX_OFFSETS_PER_TRIGGER) -> dict:
    root = config.paths().root / "kafka"
    checkpoint = str(root / "checkpoint")
    shutil.rmtree(root, ignore_errors=True)
    target = target or config.paths().gold("kafka_zone_15min")
    topic = f"trips-{month}-{uuid.uuid4().hex[:8]}"  # a fresh topic, so a rerun never sees old messages

    trips = spark.read.format("delta").load(config.paths().silver).where(F.col("source_month") == month)
    windowed_counts(spark.createDataFrame([], trips.select(*STREAM_COLUMNS).schema)) \
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)

    runs, progress, produce_s = [], [], 0.0
    halves = [F.dayofmonth("pickup_ts") < split_day, F.dayofmonth("pickup_ts") >= split_day]
    for i, half in enumerate(halves, start=1):
        started = time.perf_counter()
        produced = produce(trips.where(half), topic)
        produce_s += time.perf_counter() - started
        batch = consume(spark, topic, target, checkpoint, max_offsets)
        progress += batch
        runs.append({"run": i, "messages_produced": produced, "micro_batches": len(batch),
                     "rows_consumed": sum(p["numInputRows"] for p in batch)})

    streamed = spark.read.format("delta").load(target)
    expected = windowed_counts(trips.select(*STREAM_COLUMNS))
    mismatched = (
        expected.alias("e").join(streamed.alias("s"), ["window_start", "location_id"], "full_outer")
        .where(~F.col("e.trips").eqNullSafe(F.col("s.trips")))
        .count()
    )
    produced = sum(r["messages_produced"] for r in runs)
    rows = sum(p["numInputRows"] for p in progress)
    seconds = sum(p["durationMs"].get("triggerExecution", 0) for p in progress) / 1000
    offsets = end_offsets(progress, topic)
    return {
        "month": month,
        "topic_partitions": len(offsets),
        "messages_produced": produced,
        "produce_rows_per_second": round(produced / produce_s) if produce_s else None,
        "consumer_runs": runs,
        "offsets_consumed": sum(offsets.values()),
        "rows_streamed": rows,
        "rows_per_second": round(rows / seconds) if seconds else None,
        "rows_dropped_by_watermark": dropped_by_watermark(progress),
        "windows_written": streamed.count(),
        "windows_mismatched_vs_batch": mismatched,
        "window": WINDOW,
        "watermark": WATERMARK,
        "max_offsets_per_trigger": max_offsets,
    }


def run(spark: SparkSession) -> dict:
    return replay(spark, os.getenv("LAKEHOUSE_STREAM_MONTH", config.months()[-1]))

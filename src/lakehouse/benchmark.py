"""Performance study: each experiment times the same query under two settings on real silver data.

Timing = median of 3 runs after 1 warm-up, forced end-to-end with the `noop` sink so result
collection does not count. Laptop numbers (local[*] in Docker); the ratios are the point.
"""

from __future__ import annotations

import json
import shutil
import statistics
import time
from contextlib import contextmanager
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import config

RUNS = 3


@contextmanager
def _conf(spark: SparkSession, **settings: str):
    keys = {k.replace("__", "."): v for k, v in settings.items()}
    previous = {k: spark.conf.get(k, None) for k in keys}
    for k, v in keys.items():
        spark.conf.set(k, v)
    try:
        yield
    finally:
        for k, v in previous.items():
            spark.conf.unset(k) if v is None else spark.conf.set(k, v)


def _time(spark: SparkSession, build) -> float:
    def once() -> float:
        spark.catalog.clearCache()
        started = time.perf_counter()
        build().write.format("noop").mode("overwrite").save()
        return time.perf_counter() - started
    once()
    return round(statistics.median(once() for _ in range(RUNS)), 2)


def _compare(name: str, question: str, baseline: tuple[str, float], tuned: tuple[str, float], **extra) -> dict:
    (b_label, b_s), (t_label, t_s) = baseline, tuned
    return {"experiment": name, "question": question, "baseline": {"setting": b_label, "seconds": b_s},
            "tuned": {"setting": t_label, "seconds": t_s}, "speedup_x": round(b_s / t_s, 2), **extra}


def active_files(table_path: str) -> list[dict]:
    """Replay the Delta JSON log into the live file list, each with its partition values and min/max stats.

    Only for small benchmark-owned tables: it ignores checkpoints, so it refuses tables that have one.
    """
    log = Path(table_path) / "_delta_log"
    if any(log.glob("*.checkpoint*.parquet")):
        raise ValueError("table has a checkpoint; replay only supports pure JSON logs")
    live: dict[str, dict] = {}
    for commit in sorted(log.glob("*.json")):
        for line in commit.read_text().splitlines():
            action = json.loads(line)
            if "add" in action:
                add = action["add"]
                live[add["path"]] = {"partition": add.get("partitionValues", {}),
                                     "stats": json.loads(add["stats"]) if add.get("stats") else {}}
            elif "remove" in action:
                live.pop(action["remove"]["path"], None)
    return list(live.values())


def files_possibly_matching(files: list[dict], column: str, value) -> int:
    """Files Delta data skipping must still open: those whose [min, max] for `column` contains `value`."""
    return sum(1 for f in files
               if f["stats"].get("minValues", {}).get(column, value) <= value
               <= f["stats"].get("maxValues", {}).get(column, value))


def partition_pruning(spark: SparkSession, month: str) -> dict:
    p = config.paths()
    silver = lambda: spark.read.format("delta").load(p.silver)  # noqa: E731
    revenue = lambda df: df.groupBy("pickup_date").agg(F.sum("total_amount"))  # noqa: E731
    # wrapping the column in a function hides it from both partition pruning and min/max file stats
    wrapped = _time(spark, lambda: revenue(silver().where(F.date_format("pickup_ts", "yyyy-MM") == month)))
    pruned = _time(spark, lambda: revenue(silver().where(F.col("source_month") == month)))
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{p.silver}`").first()
    in_partition = spark.sql(f"SELECT count(*) FROM (SELECT DISTINCT input_file_name() FROM delta.`{p.silver}` "
                             f"WHERE source_month = '{month}')").first()[0]
    return _compare("partition_pruning", f"daily revenue for {month} out of 24 months",
                    ("filter date_format(pickup_ts) = month (function hides the column)", wrapped),
                    ("filter on partition column source_month", pruned),
                    files_scanned={"baseline": detail.numFiles, "tuned": in_partition})


def file_compaction(spark: SparkSession, months: list[str]) -> dict:
    p = config.paths()
    bench = str(p.root / "bench" / "compaction")
    # the layout silver first had: ~200 small files per month straight out of a shuffle
    spark.read.format("delta").load(p.silver).where(F.col("source_month").isin(months))         .repartition(200 * len(months)).write.format("delta").mode("overwrite").save(bench)
    query = lambda: spark.read.format("delta").load(bench).groupBy("payment_type").agg(  # noqa: E731
        F.avg("tip_pct"), F.sum("total_amount"))
    before_files, before = DeltaTable.forPath(spark, bench).detail().first().numFiles, _time(spark, query)
    DeltaTable.forPath(spark, bench).optimize().executeCompaction()
    after_files, after = DeltaTable.forPath(spark, bench).detail().first().numFiles, _time(spark, query)
    shutil.rmtree(bench, ignore_errors=True)
    return _compare("file_compaction", f"scan {len(months)} months grouped by payment type",
                    (f"{before_files} small files", before), (f"OPTIMIZE bin-packed to {after_files} files", after),
                    files_scanned={"baseline": before_files, "tuned": after_files})


def broadcast_join(spark: SparkSession) -> dict:
    p = config.paths()

    def enriched():
        trips = spark.read.format("delta").load(p.silver)
        zones = spark.read.format("delta").load(p.zones)
        return trips.join(zones, trips.pu_location_id == zones.location_id).groupBy("borough").count()

    with _conf(spark, spark__sql__autoBroadcastJoinThreshold="-1"):
        sort_merge = _time(spark, enriched)
    broadcast = _time(spark, enriched)  # 265-row dimension is under the default 10 MB threshold
    return _compare("broadcast_join", "trips by pickup borough: join 78M trips to the 265-row zone table",
                    ("sort-merge join (broadcast disabled)", sort_merge), ("broadcast hash join", broadcast))


def adaptive_execution(spark: SparkSession) -> dict:
    p = config.paths()
    query = lambda: spark.read.format("delta").load(p.silver).groupBy(  # noqa: E731
        "pu_location_id", "do_location_id", "pickup_hour").agg(F.avg("fare_amount"), F.count("*"))
    with _conf(spark, spark__sql__adaptive__enabled="false", spark__sql__shuffle__partitions="1000"):
        static = _time(spark, query)
    with _conf(spark, spark__sql__adaptive__enabled="true", spark__sql__shuffle__partitions="1000"):
        adaptive = _time(spark, query)
    return _compare("adaptive_query_execution", "origin x destination x hour aggregate with 1000 shuffle partitions",
                    ("AQE off: 1000 fixed reduce tasks", static), ("AQE on: partitions coalesced at runtime", adaptive))


def columnar_format(spark: SparkSession, month: str) -> dict:
    p = config.paths()
    csv_path = str(p.root / "bench" / "csv_month")
    month_df = spark.read.format("delta").load(p.silver).where(F.col("source_month") == month).drop("dq_warnings")
    month_df.write.mode("overwrite").option("header", True).csv(csv_path)
    schema = month_df.schema
    query = lambda df: df.groupBy("pu_location_id").agg(F.avg("tip_amount"))  # noqa: E731
    csv = _time(spark, lambda: query(spark.read.option("header", True).schema(schema).csv(csv_path)))
    delta = _time(spark, lambda: query(spark.read.format("delta").load(p.silver).where(F.col("source_month") == month)))
    csv_mb = sum(f.stat().st_size for f in (p.root / "bench" / "csv_month").glob("*.csv")) / 1e6
    delta_mb = sum(f.stat().st_size for f in (p.root / "silver" / "yellow_trips" / f"source_month={month}")
                   .glob("*.parquet")) / 1e6
    shutil.rmtree(csv_path, ignore_errors=True)
    return _compare("columnar_format", f"average tip per zone for {month}: 2 of 30 columns needed",
                    ("CSV (row format, whole rows parsed)", csv), ("Delta/Parquet (column pruning)", delta),
                    storage_mb={"csv": round(csv_mb, 1), "delta_parquet": round(delta_mb, 1)})


def zorder_skipping(spark: SparkSession, months: list[str], zone: int = 132) -> dict:
    p = config.paths()
    bench = str(p.root / "bench" / "zorder")
    spark.read.format("delta").load(p.silver).where(F.col("source_month").isin(months))         .repartition(48).write.format("delta").mode("overwrite").save(bench)
    query = lambda: spark.read.format("delta").load(bench).where(  # noqa: E731
        F.col("pu_location_id") == zone).agg(F.count("*"), F.avg("fare_amount"))

    before_files = active_files(bench)
    before = _time(spark, query)
    # cap output files so the clustered data spans many files with narrow zone ranges
    with _conf(spark, spark__databricks__delta__optimize__maxFileSize=str(32 * 1024 * 1024)):
        DeltaTable.forPath(spark, bench).optimize().executeZOrderBy("pu_location_id")
    after_files = active_files(bench)
    after = _time(spark, query)
    shutil.rmtree(bench, ignore_errors=True)
    return _compare("zorder_data_skipping", f"JFK pickups over {len(months)} months: one zone out of 265",
                    (f"{len(before_files)} randomly ordered files", before),
                    (f"OPTIMIZE ZORDER BY pu_location_id ({len(after_files)} files)", after),
                    files_scanned={"baseline": files_possibly_matching(before_files, "pu_location_id", zone),
                                   "tuned": files_possibly_matching(after_files, "pu_location_id", zone)})


def run(spark: SparkSession) -> dict:
    months = config.months()
    return {
        "method": f"median of {RUNS} runs after 1 warm-up; noop sink; local[*] Spark in Docker (8 GB VM)",
        "results": [
            partition_pruning(spark, months[-1]),
            columnar_format(spark, months[-1]),
            broadcast_join(spark),
            adaptive_execution(spark),
            file_compaction(spark, months[-6:]),
            zorder_skipping(spark, months[-6:]),
        ],
    }

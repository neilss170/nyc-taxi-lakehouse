"""Bronze: land each monthly file in one Delta table under a canonical schema.

TLC files drift between months (2023-01 stores IDs as int64 and passenger_count as
double, later months use int32/int64 and rename airport_fee -> Airport_fee). Bronze
normalises names and types only; it never drops or edits rows.
"""

from __future__ import annotations

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import config

# canonical name -> (Spark type, source column names it may appear under)
CANONICAL_SCHEMA: dict[str, tuple[str, tuple[str, ...]]] = {
    "vendor_id": ("int", ("VendorID",)),
    "pickup_ts": ("timestamp", ("tpep_pickup_datetime",)),
    "dropoff_ts": ("timestamp", ("tpep_dropoff_datetime",)),
    "passenger_count": ("int", ("passenger_count",)),
    "trip_distance": ("double", ("trip_distance",)),
    "ratecode_id": ("int", ("RatecodeID",)),
    "store_and_fwd_flag": ("string", ("store_and_fwd_flag",)),
    "pu_location_id": ("int", ("PULocationID",)),
    "do_location_id": ("int", ("DOLocationID",)),
    "payment_type": ("int", ("payment_type",)),
    "fare_amount": ("double", ("fare_amount",)),
    "extra": ("double", ("extra",)),
    "mta_tax": ("double", ("mta_tax",)),
    "tip_amount": ("double", ("tip_amount",)),
    "tolls_amount": ("double", ("tolls_amount",)),
    "improvement_surcharge": ("double", ("improvement_surcharge",)),
    "total_amount": ("double", ("total_amount",)),
    "congestion_surcharge": ("double", ("congestion_surcharge",)),
    "airport_fee": ("double", ("airport_fee", "Airport_fee")),
}


def normalise(df: DataFrame) -> DataFrame:
    """Map a raw TLC frame onto the canonical schema; absent columns become typed nulls."""
    present = {c.lower(): c for c in df.columns}
    cols = []
    for name, (dtype, sources) in CANONICAL_SCHEMA.items():
        source = next((present[s.lower()] for s in sources if s.lower() in present), None)
        col = F.col(f"`{source}`") if source else F.lit(None)
        cols.append(col.cast(dtype).alias(name))
    return df.select(*cols)


def ingest_month(spark: SparkSession, month: str, bronze_path: str | None = None) -> int:
    """Idempotently (re)load one month: replaceWhere overwrites only that month's partition."""
    p = config.paths()
    src = p.raw / config.raw_file_name(month)
    df = (
        normalise(spark.read.parquet(str(src)))
        .withColumn("source_file", F.lit(src.name))
        .withColumn("source_month", F.lit(month))
        .withColumn("ingested_at", F.current_timestamp())
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"source_month = '{month}'")
        .partitionBy("source_month")
        .save(bronze_path or p.bronze)
    )
    return pq.ParquetFile(src).metadata.num_rows


def ingest_zones(spark: SparkSession) -> int:
    p = config.paths()
    zones = (
        spark.read.option("header", True).csv(str(p.raw / "taxi_zone_lookup.csv"))
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone"),
        )
    )
    zones.write.format("delta").mode("overwrite").save(p.zones)
    return zones.count()


def run(spark: SparkSession, months: list[str] | None = None) -> dict:
    p = config.paths()
    months = months or config.months()
    source_rows = {m: ingest_month(spark, m) for m in months}
    landed = {
        r["source_month"]: r["count"]
        for r in spark.read.format("delta").load(p.bronze)
        .where(F.col("source_month").isin(months))
        .groupBy("source_month").count().collect()
    }
    mismatches = {m: (source_rows[m], landed.get(m, 0)) for m in months if source_rows[m] != landed.get(m, 0)}
    if mismatches:
        raise RuntimeError(f"bronze row counts do not reconcile with source files: {mismatches}")
    return {
        "months": len(months),
        "rows": sum(landed.values()),
        "rows_by_month": dict(sorted(landed.items())),
        "zones": ingest_zones(spark),
        "reconciled_with_source": True,
    }

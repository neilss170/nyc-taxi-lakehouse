"""Gold: business aggregates over silver, plus small JSON extracts for the static dashboard."""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import config

SITE_DATA = config.REPO_ROOT / "site" / "data"
AIRPORT_ZONES = {1: "EWR", 132: "JFK", 138: "LGA"}
ANOMALY_WINDOW_DAYS = 7
ANOMALY_RATIO = 0.5


def _trip_metrics() -> list:
    return [
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_miles"),
        F.round(F.avg("trip_minutes"), 2).alias("avg_minutes"),
        # tips are only recorded for card payments (payment_type 1)
        F.round(F.avg(F.when(F.col("payment_type") == 1, F.col("tip_pct"))), 2).alias("avg_card_tip_pct"),
    ]


def flag_volume_anomalies(daily: DataFrame) -> DataFrame:
    """Flag days under half the median of the surrounding +/-7 days.

    Catches gaps in the published source (e.g. 2023-09-21..24, where both vendors' metered
    trips are missing and only Flex Fare rows remain) so charts and models can skip them.
    """
    around = Window.orderBy("pickup_date").rowsBetween(-ANOMALY_WINDOW_DAYS, ANOMALY_WINDOW_DAYS)
    return (
        # median() rejects window frames; take the middle of the sorted frame instead
        daily.withColumn("_frame", F.sort_array(F.collect_list("trips").over(around)))
        .withColumn("neighbour_median_trips", F.expr("_frame[(size(_frame) - 1) DIV 2]"))
        .drop("_frame")
        .withColumn("volume_anomaly", F.col("trips") < ANOMALY_RATIO * F.col("neighbour_median_trips"))
    )


def _airport_name(col: str):
    name = None
    for location_id, code in AIRPORT_ZONES.items():
        name = (F if name is None else name).when(F.col(col) == location_id, code)
    return name


def build(spark: SparkSession) -> dict[str, DataFrame]:
    p = config.paths()
    trips = spark.read.format("delta").load(p.silver)
    zones = spark.read.format("delta").load(p.zones)
    pu = F.broadcast(zones.select(F.col("location_id").alias("pu_location_id"),
                                  F.col("borough").alias("pu_borough"), F.col("zone").alias("pu_zone")))
    do = F.broadcast(zones.select(F.col("location_id").alias("do_location_id"),
                                  F.col("borough").alias("do_borough")))
    enriched = trips.join(pu, "pu_location_id", "left").join(do, "do_location_id", "left")

    return {
        "daily_kpis": flag_volume_anomalies(trips.groupBy("pickup_date").agg(
            *_trip_metrics(),
            F.round(100 * F.avg(F.col("is_flex_fare").cast("int")), 2).alias("flex_fare_pct"),
            F.round(100 * F.avg((F.col("payment_type") == 2).cast("int")), 2).alias("cash_pct"),
        )),
        "zone_hourly_demand": trips.groupBy(
            F.col("pu_location_id").alias("location_id"), "pickup_date", "pickup_hour"
        ).agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare")),
        "zone_summary": enriched.groupBy(
            F.col("pu_location_id").alias("location_id"), "pu_borough", "pu_zone"
        ).agg(*_trip_metrics()),
        "borough_flows": enriched.groupBy("pu_borough", "do_borough").agg(*_trip_metrics()),
        "hour_weekday_profile": trips.groupBy("pickup_dow", "pickup_hour").agg(
            F.count("*").alias("trips"),
            F.round(F.avg(F.when(F.col("trip_minutes") > 0,
                                 F.col("trip_distance") / (F.col("trip_minutes") / 60))), 2).alias("avg_mph"),
        ),
        "airport_monthly": trips.where(
            F.col("pu_location_id").isin(*AIRPORT_ZONES) | F.col("do_location_id").isin(*AIRPORT_ZONES)
        ).withColumn(
            "airport", F.coalesce(_airport_name("pu_location_id"), _airport_name("do_location_id"))
        ).withColumn(
            "direction", F.when(F.col("pu_location_id").isin(*AIRPORT_ZONES), "from_airport").otherwise("to_airport")
        ).groupBy("source_month", "airport", "direction").agg(*_trip_metrics()),
    }


def _export(df: DataFrame, name: str, order_by: list[str]) -> int:
    rows = [r.asDict() for r in df.orderBy(*order_by).collect()]
    SITE_DATA.mkdir(parents=True, exist_ok=True)
    (SITE_DATA / f"{name}.json").write_text(json.dumps(rows, default=str, separators=(",", ":")))
    return len(rows)


def run(spark: SparkSession) -> dict:
    p = config.paths()
    tables = build(spark)
    counts = {}
    for name, df in tables.items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(p.gold(name))
        counts[name] = spark.read.format("delta").load(p.gold(name)).count()

    load = lambda n: spark.read.format("delta").load(p.gold(n))  # noqa: E731
    exported = {
        "daily_kpis": _export(load("daily_kpis"), "daily_kpis", ["pickup_date"]),
        "zone_summary": _export(load("zone_summary"), "zone_summary", ["location_id"]),
        "borough_flows": _export(load("borough_flows"), "borough_flows", ["pu_borough", "do_borough"]),
        "hour_weekday_profile": _export(load("hour_weekday_profile"), "hour_weekday_profile",
                                        ["pickup_dow", "pickup_hour"]),
        "airport_monthly": _export(load("airport_monthly"), "airport_monthly", ["source_month", "airport", "direction"]),
    }
    anomalies = [str(r.pickup_date) for r in load("daily_kpis").where("volume_anomaly").orderBy("pickup_date").collect()]
    return {"table_rows": counts, "volume_anomaly_days": anomalies, "dashboard_extracts": exported}

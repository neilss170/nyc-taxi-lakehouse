"""Day-ahead hourly pickup forecast per zone with Spark MLlib, scored against naive baselines.

Every feature is at least 24 hours old at prediction time, the hour grid is dense (an hour
with no pickups is a 0, not a missing row) and the split is by time, never random.
"""

from __future__ import annotations

import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import config, gold

# US federal holidays observed in NYC, 2023-2024
HOLIDAYS = [
    "2023-01-01", "2023-01-02", "2023-01-16", "2023-02-20", "2023-05-29", "2023-06-19", "2023-07-04",
    "2023-09-04", "2023-10-09", "2023-11-10", "2023-11-23", "2023-12-25",
    "2024-01-01", "2024-01-15", "2024-02-19", "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02",
    "2024-10-14", "2024-11-11", "2024-11-28", "2024-12-25",
]
LAGS = [24, 48, 168, 336]
FEATURES = ["hour", "dow", "is_weekend", "is_holiday", "month", "zone_mean"] + [f"lag_{h}" for h in LAGS] + [
    "same_hour_mean_7d"]


def dense_hourly(spark: SparkSession, demand: DataFrame, zones: list[int]) -> DataFrame:
    """Zone x hour grid over the whole period, zero-filled."""
    hourly = demand.where(F.col("location_id").isin(zones)).select(
        "location_id",
        (F.col("pickup_date").cast("timestamp") + F.make_interval(hours=F.col("pickup_hour"))).alias("ts"),
        "trips",
    )
    bounds = hourly.agg(F.min("ts").alias("lo"), F.max("ts").alias("hi")).first()
    grid = spark.createDataFrame([(z,) for z in zones], "location_id int").crossJoin(
        spark.sql(f"SELECT explode(sequence(timestamp'{bounds.lo}', timestamp'{bounds.hi}', interval 1 hour)) AS ts")
    )
    return grid.join(hourly, ["location_id", "ts"], "left").fillna(0, ["trips"])


def add_features(hourly: DataFrame, train_end: str, exclude_dates: list[str] | None = None) -> DataFrame:
    by_zone = Window.partitionBy("location_id").orderBy("ts")
    df = hourly
    for h in LAGS:
        df = df.withColumn(f"lag_{h}", F.lag("trips", h).over(by_zone))
    df = df.withColumn("same_hour_mean_7d", sum(F.lag("trips", 24 * d).over(by_zone) for d in range(1, 8)) / 7)
    # zone level learned from training rows only, so the test period cannot leak into it
    zone_mean = df.where(F.col("ts") < train_end).groupBy("location_id").agg(F.avg("trips").alias("zone_mean"))
    return (
        df.join(F.broadcast(zone_mean), "location_id")
        .withColumn("hour", F.hour("ts"))
        .withColumn("dow", F.dayofweek("ts"))
        .withColumn("is_weekend", F.col("dow").isin(1, 7).cast("int"))
        .withColumn("is_holiday", F.to_date("ts").cast("string").isin(HOLIDAYS).cast("int"))
        .withColumn("month", F.month("ts"))
        .withColumn("label", F.col("trips").cast("double"))
        .dropna(subset=[f"lag_{max(LAGS)}"])
        .where(~_touches(exclude_dates or []))
    )


def _touches(dates: list[str]):
    """True when the target hour, or any hour a feature looks back to, falls on one of `dates`."""
    if not dates:
        return F.lit(False)
    offsets = sorted({0, *LAGS, *(24 * d for d in range(1, 8))})
    return F.greatest(*[
        F.date_format(F.col("ts") - F.make_interval(hours=F.lit(h)), "yyyy-MM-dd").isin(dates) for h in offsets])


def score(df: DataFrame, prediction: str) -> dict:
    err = F.col(prediction) - F.col("label")
    m = df.agg(
        F.sqrt(F.avg(err * err)).alias("rmse"),
        F.avg(F.abs(err)).alias("mae"),
        (F.sum(F.abs(err)) / F.sum("label")).alias("wape"),
    ).first()
    return {"rmse": round(m.rmse, 3), "mae": round(m.mae, 3), "wape_pct": round(100 * m.wape, 2)}


def run(spark: SparkSession, train_end: str | None = None, min_trips_per_hour: float = 1.0) -> dict:
    train_end = train_end or os.getenv("LAKEHOUSE_TRAIN_END", "2024-10-01")
    p = config.paths()
    demand = spark.read.format("delta").load(p.gold("zone_hourly_demand"))

    # forecast zones busy enough for an hourly model: >= min_trips_per_hour on average before the split
    train_hours = demand.where(F.col("pickup_date") < train_end).select("pickup_date", "pickup_hour") \
        .distinct().count()
    volume = demand.where(F.col("pickup_date") < train_end).groupBy("location_id").agg(F.sum("trips").alias("n"))
    zones = sorted(r.location_id for r in volume.where(F.col("n") >= min_trips_per_hour * train_hours).collect())
    total_trips = demand.agg(F.sum("trips")).first()[0]
    covered_trips = demand.where(F.col("location_id").isin(zones)).agg(F.sum("trips")).first()[0]

    # days with a known gap in the published source would teach the model fake collapses
    gap_days = [str(r.pickup_date) for r in
                spark.read.format("delta").load(p.gold("daily_kpis")).where("volume_anomaly").collect()]
    data = add_features(dense_hourly(spark, demand, zones), train_end, gap_days).cache()
    train, test = data.where(F.col("ts") < train_end), data.where(F.col("ts") >= train_end)

    gbt = GBTRegressor(featuresCol="features", labelCol="label", maxIter=60, maxDepth=6, stepSize=0.1,
                       subsamplingRate=0.8, seed=42)
    model = Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"), gbt]).fit(train)
    scored = model.transform(test).withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0)))

    results = {
        "naive_same_hour_yesterday": score(scored, "lag_24"),
        "naive_same_hour_last_week": score(scored, "lag_168"),
        "naive_7day_same_hour_mean": score(scored, "same_hour_mean_7d"),
        "gbt": score(scored, "prediction"),
    }
    best_baseline = min((k for k in results if k != "gbt"), key=lambda k: results[k]["wape_pct"])
    importances = model.stages[-1].featureImportances.toArray()
    report = {
        "train_period": f"< {train_end}",
        "test_period": f">= {train_end}",
        "zones_modelled": len(zones),
        "source_gap_days_excluded": sorted(gap_days),
        "share_of_all_trips_pct": round(100 * covered_trips / total_trips, 2),
        "train_rows": train.count(),
        "test_rows": test.count(),
        "metrics": results,
        "best_baseline": best_baseline,
        "gbt_wape_improvement_vs_best_baseline_pct": round(
            100 * (1 - results["gbt"]["wape_pct"] / results[best_baseline]["wape_pct"]), 1),
        "feature_importance": dict(sorted(
            ((f, round(float(i), 4)) for f, i in zip(FEATURES, importances)), key=lambda kv: -kv[1])),
    }
    daily = scored.groupBy(F.to_date("ts").alias("date")).agg(
        F.sum("label").alias("actual"), F.round(F.sum("prediction")).alias("gbt"), F.sum("lag_168").alias("last_week"))
    gold.SITE_DATA.mkdir(parents=True, exist_ok=True)
    (gold.SITE_DATA / "forecast_daily.json").write_text(json.dumps(
        [r.asDict() for r in daily.orderBy("date").collect()], default=str, separators=(",", ":")))
    data.unpersist()
    return report

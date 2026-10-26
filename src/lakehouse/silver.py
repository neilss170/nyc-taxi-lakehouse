"""Silver: deduplicated, rule-checked trips with derived fields; failing rows go to quarantine."""

from __future__ import annotations

from collections import Counter

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import config, quality
from lakehouse.bronze import CANONICAL_SCHEMA

BUSINESS_COLUMNS = list(CANONICAL_SCHEMA)


def _derive(df: DataFrame) -> DataFrame:
    duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60
    return (
        df.withColumn("trip_minutes", F.round(duration_min, 2))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("pickup_dow", F.dayofweek("pickup_ts"))  # 1 = Sunday
        .withColumn("is_flex_fare", F.col("payment_type") == 0)
        .withColumn("tip_pct", F.when(F.col("fare_amount") > 0,
                                      F.round(100 * F.col("tip_amount") / F.col("fare_amount"), 2)))
    )


def _replace_month(df: DataFrame, path: str, month: str) -> None:
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"source_month = '{month}'")
     .partitionBy("source_month").save(path))


def process_month(spark: SparkSession, month: str) -> dict:
    """Dedupe, tag and split one month. One month at a time keeps memory flat as history grows."""
    p = config.paths()
    bronze = spark.read.format("delta").load(p.bronze).where(F.col("source_month") == month)

    # exact duplicates on every business column are re-sent records; keep one
    tagged = quality.tag(bronze.dropDuplicates(BUSINESS_COLUMNS + ["source_month"])).cache()
    try:
        _replace_month(_derive(tagged.where(F.size("dq_errors") == 0)).drop("dq_errors"), p.silver, month)
        _replace_month(tagged.where(F.size("dq_errors") > 0), p.quarantine, month)
        errors = tagged.select(F.explode("dq_errors").alias("rule")).groupBy("rule").count().collect()
        warnings = tagged.where(F.size("dq_errors") == 0) \
            .select(F.explode("dq_warnings").alias("rule")).groupBy("rule").count().collect()
        return {
            "rows_in": bronze.count(),
            "rows_deduped": tagged.count(),
            "errors": {r["rule"]: r["count"] for r in errors},
            "warnings": {r["rule"]: r["count"] for r in warnings},
        }
    finally:
        tagged.unpersist()


def _sum(dicts: list[dict]) -> dict:
    total: Counter = Counter()
    for d in dicts:
        total.update(d)
    return dict(total.most_common())


def run(spark: SparkSession, months: list[str] | None = None) -> dict:
    p = config.paths()
    months = months or config.months()
    stats = [process_month(spark, m) for m in months]

    rows_in = sum(s["rows_in"] for s in stats)
    duplicates = rows_in - sum(s["rows_deduped"] for s in stats)
    silver_rows = spark.read.format("delta").load(p.silver).where(F.col("source_month").isin(months)).count()
    quarantined = spark.read.format("delta").load(p.quarantine).where(F.col("source_month").isin(months)).count()
    if rows_in != silver_rows + quarantined + duplicates:
        raise RuntimeError(f"row conservation failed: {rows_in} != {silver_rows} + {quarantined} + {duplicates}")

    return {
        "rows_in": rows_in,
        "duplicates_removed": duplicates,
        "rows_quarantined": quarantined,
        "rows_silver": silver_rows,
        "pass_rate_pct": round(100 * silver_rows / rows_in, 3),
        "violations_by_rule": _sum([s["errors"] for s in stats]),
        "warnings_by_rule": _sum([s["warnings"] for s in stats]),
        "rules": quality.rule_catalog(),
        "conserved": True,
    }

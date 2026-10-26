"""Declarative data-quality rules for trips.

`error` rules send a row to quarantine; `warning` rules keep it in silver but tag it.
Every threshold below comes from profiling the 2023-2024 bronze table (see README).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

VALID_VENDORS = (1, 2, 6, 7)  # TLC data dictionary (2024): 6 = Myle, 7 = Helix


@dataclass(frozen=True)
class Rule:
    name: str
    severity: str  # "error" | "warning"
    violated: Column  # true when the row breaks the rule
    description: str


def rules() -> list[Rule]:
    """Built on call: PySpark Columns need an active session."""
    duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60
    components = (
        F.col("fare_amount") + F.col("extra") + F.col("mta_tax") + F.col("tip_amount") + F.col("tolls_amount")
        + F.col("improvement_surcharge") + F.coalesce("congestion_surcharge", F.lit(0.0))
        + F.coalesce("airport_fee", F.lit(0.0))
    )
    return [
        Rule("missing_timestamp", "error", F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(),
             "pickup or dropoff time is null"),
        Rule("pickup_outside_file_month", "error", F.date_format("pickup_ts", "yyyy-MM") != F.col("source_month"),
             "pickup time falls outside the month the file covers (meter clock errors, 2001-2026 seen)"),
        Rule("non_positive_duration", "error", F.col("dropoff_ts") <= F.col("pickup_ts"),
             "dropoff is at or before pickup"),
        Rule("duration_over_6h", "error", duration_min > 360, "trip longer than 6 hours"),
        Rule("negative_amount", "error", (F.col("fare_amount") < 0) | (F.col("total_amount") < 0),
             "negative fare or total (refund / void records)"),
        Rule("fare_over_500", "error", F.col("fare_amount") > 500, "fare above $500"),
        Rule("distance_over_100mi", "error", F.col("trip_distance") > 100, "distance above 100 miles"),
        Rule("speed_over_80mph", "error",
             (duration_min > 0) & (F.col("trip_distance") / (duration_min / 60) > 80),
             "average speed above 80 mph"),
        Rule("unknown_vendor", "error", ~F.col("vendor_id").isin(*VALID_VENDORS), "vendor id not in data dictionary"),
        Rule("location_out_of_range", "error",
             ~F.col("pu_location_id").between(1, 265) | ~F.col("do_location_id").between(1, 265),
             "location id outside the 265 TLC zones"),
        Rule("zero_distance", "warning", F.col("trip_distance") == 0, "zero recorded distance"),
        Rule("unknown_zone", "warning",
             F.col("pu_location_id").isin(264, 265) | F.col("do_location_id").isin(264, 265),
             "pickup or dropoff zone is Unknown / Outside NYC"),
        Rule("missing_passenger_count", "warning", F.col("passenger_count").isNull(),
             "passenger count missing (all such rows are payment_type 0, Flex Fare)"),
        Rule("zero_passengers", "warning", F.col("passenger_count") == 0, "passenger count recorded as 0"),
        Rule("total_mismatch", "warning", F.abs(F.col("total_amount") - components) > 0.01,
             "fare components do not sum to total; mostly vendor 1 folding congestion/airport fees into extra"),
    ]


def _tags(severity: str) -> Column:
    return F.array_compact(F.array(*[F.when(r.violated, F.lit(r.name)) for r in rules() if r.severity == severity]))


def tag(df: DataFrame) -> DataFrame:
    """Add dq_errors / dq_warnings arrays naming every rule each row breaks."""
    return df.withColumn("dq_errors", _tags("error")).withColumn("dq_warnings", _tags("warning"))


def rule_catalog() -> list[dict]:
    return [{"name": r.name, "severity": r.severity, "description": r.description} for r in rules()]

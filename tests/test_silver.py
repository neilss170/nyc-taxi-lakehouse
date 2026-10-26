from datetime import datetime

from pyspark.sql import functions as F

from lakehouse import bronze, config, silver
from tests_support import trip, write_month

T = datetime(2024, 3, 10, 8, 0)


def _load(spark, path):
    return spark.read.format("delta").load(path)


def test_rows_are_split_between_silver_and_quarantine_and_conserved(spark, lake):
    rows = [
        trip(T),                                                    # clean
        trip(T),                                                    # exact duplicate of the first
        trip(T, minutes=-5),                                        # dropoff before pickup
        trip(T, fare_amount=-12.8, total_amount=-20.8),             # refund
        trip(datetime(2009, 1, 1, 0, 5)),                           # clock error, outside file month
        trip(T, minutes=1, trip_distance=40.0),                     # 2400 mph
        trip(T, VendorID=9),                                        # unknown vendor
        trip(T, passenger_count=None, RatecodeID=None, payment_type=0),  # flex fare: kept, warned
    ]
    write_month(lake, "2024-03", rows)
    bronze.run(spark, ["2024-03"])

    report = silver.run(spark, ["2024-03"])

    assert report["rows_in"] == 8
    assert report["duplicates_removed"] == 1
    assert report["rows_quarantined"] == 5
    assert report["rows_silver"] == 2
    assert report["violations_by_rule"] == {
        "non_positive_duration": 1, "negative_amount": 1, "pickup_outside_file_month": 1,
        "speed_over_80mph": 1, "unknown_vendor": 1,
    }
    assert report["warnings_by_rule"]["missing_passenger_count"] == 1


def test_quarantined_rows_keep_their_reasons(spark, lake):
    write_month(lake, "2024-03", [trip(T, minutes=-5, fare_amount=-1.0, total_amount=-1.0)])
    bronze.run(spark, ["2024-03"])
    silver.run(spark, ["2024-03"])

    reasons = _load(spark, config.paths().quarantine).select("dq_errors").first().dq_errors
    assert sorted(reasons) == ["negative_amount", "non_positive_duration"]


def test_vendor_fee_convention_is_a_warning_not_a_reject(spark, lake):
    # vendor 1 style: congestion surcharge folded into extra, so components overshoot total by 2.50
    write_month(lake, "2024-03", [trip(T, VendorID=1, extra=3.5, total_amount=20.8)])
    bronze.run(spark, ["2024-03"])
    report = silver.run(spark, ["2024-03"])

    assert report["rows_silver"] == 1
    assert report["warnings_by_rule"] == {"total_mismatch": 1}


def test_derived_fields(spark, lake):
    write_month(lake, "2024-03", [trip(datetime(2024, 3, 10, 17, 45), minutes=30, fare_amount=20.0, tip_amount=5.0)])
    bronze.run(spark, ["2024-03"])
    silver.run(spark, ["2024-03"])

    row = _load(spark, config.paths().silver).first()
    assert (row.trip_minutes, row.pickup_hour, row.pickup_dow, row.tip_pct) == (30.0, 17, 1, 25.0)
    assert row.is_flex_fare is False
    assert "dq_errors" not in _load(spark, config.paths().silver).columns


def test_rerun_is_idempotent(spark, lake):
    write_month(lake, "2024-03", [trip(T), trip(T, minutes=-5)])
    bronze.run(spark, ["2024-03"])
    silver.run(spark, ["2024-03"])
    silver.run(spark, ["2024-03"])

    assert _load(spark, config.paths().silver).count() == 1
    assert _load(spark, config.paths().quarantine).count() == 1
    assert _load(spark, config.paths().silver).where(F.col("source_month") == "2024-03").count() == 1

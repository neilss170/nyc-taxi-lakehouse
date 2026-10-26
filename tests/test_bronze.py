from datetime import datetime

from pyspark.sql import functions as F

from lakehouse import bronze, config
from tests_support import trip, write_month


def test_schema_drift_lands_in_one_canonical_schema(spark, lake):
    write_month(lake, "2023-01", [trip(datetime(2023, 1, 5, 8), Airport_fee=1.75)], legacy_schema=True)
    write_month(lake, "2023-02", [trip(datetime(2023, 2, 5, 8), Airport_fee=1.75)] * 2)

    report = bronze.run(spark, ["2023-01", "2023-02"])

    df = spark.read.format("delta").load(config.paths().bronze)
    assert report["rows_by_month"] == {"2023-01": 1, "2023-02": 2}
    assert report["reconciled_with_source"]
    assert dict(df.dtypes)["vendor_id"] == "int"
    assert dict(df.dtypes)["passenger_count"] == "int"
    # the legacy lowercase airport_fee column is not lost
    assert df.where(F.col("airport_fee") == 1.75).count() == 3


def test_rerunning_a_month_replaces_it_instead_of_appending(spark, lake):
    write_month(lake, "2024-03", [trip(datetime(2024, 3, 1, 9))] * 3)
    write_month(lake, "2024-04", [trip(datetime(2024, 4, 1, 9))])
    bronze.run(spark, ["2024-03", "2024-04"])

    write_month(lake, "2024-03", [trip(datetime(2024, 3, 1, 9))] * 5)  # TLC re-published the month
    bronze.run(spark, ["2024-03"])

    counts = {r["source_month"]: r["count"] for r in
              spark.read.format("delta").load(config.paths().bronze).groupBy("source_month").count().collect()}
    assert counts == {"2024-03": 5, "2024-04": 1}


def test_timestamps_keep_nyc_wall_clock_values(spark, lake):
    write_month(lake, "2024-05", [trip(datetime(2024, 5, 1, 23, 30))])
    bronze.run(spark, ["2024-05"])
    row = spark.read.format("delta").load(config.paths().bronze).select("pickup_ts").first()
    assert row.pickup_ts == datetime(2024, 5, 1, 23, 30)


def test_zone_lookup_is_typed(spark, lake):
    write_month(lake, "2024-06", [trip(datetime(2024, 6, 1, 9))])
    report = bronze.run(spark, ["2024-06"])
    zones = spark.read.format("delta").load(config.paths().zones)
    assert report["zones"] == 5
    assert dict(zones.dtypes)["location_id"] == "int"

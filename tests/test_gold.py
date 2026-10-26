import json
from datetime import datetime

import pytest

from lakehouse import bronze, config, gold, silver
from tests_support import trip, write_month


@pytest.fixture
def built(spark, lake, monkeypatch):
    monkeypatch.setattr(gold, "SITE_DATA", lake / "site")
    write_month(lake, "2024-03", [
        trip(datetime(2024, 3, 4, 8), fare_amount=10.0, total_amount=15.0),
        trip(datetime(2024, 3, 4, 8, 30), fare_amount=20.0, total_amount=25.0, payment_type=2, tip_amount=0.0),
        trip(datetime(2024, 3, 5, 17), PULocationID=132, DOLocationID=161, fare_amount=70.0, total_amount=90.0),
        trip(datetime(2024, 3, 5, 18), minutes=-1),  # quarantined, must not reach gold
    ])
    bronze.run(spark, ["2024-03"])
    silver.run(spark, ["2024-03"])
    report = gold.run(spark)
    return report, lambda name: spark.read.format("delta").load(config.paths().gold(name))


def test_daily_kpis(built):
    _, load = built
    rows = {str(r.pickup_date): r for r in load("daily_kpis").collect()}
    assert rows["2024-03-04"].trips == 2
    assert rows["2024-03-04"].revenue == 40.0
    assert rows["2024-03-04"].cash_pct == 50.0
    assert rows["2024-03-05"].trips == 1


def test_zone_summary_is_enriched_with_zone_names(built):
    _, load = built
    jfk = load("zone_summary").where("location_id = 132").first()
    assert (jfk.pu_borough, jfk.pu_zone, jfk.trips) == ("Queens", "JFK Airport", 1)


def test_airport_trips_are_labelled_by_airport_and_direction(built):
    _, load = built
    rows = [(r.airport, r.direction, r.trips) for r in load("airport_monthly").collect()]
    assert rows == [("JFK", "from_airport", 1)]


def test_zone_hourly_demand_grain(built):
    _, load = built
    midtown_8am = load("zone_hourly_demand").where("location_id = 161 AND pickup_hour = 8").first()
    assert midtown_8am.trips == 2


def test_dashboard_extracts_are_written(built, lake):
    report, _ = built
    daily = json.loads((lake / "site" / "daily_kpis.json").read_text())
    assert [d["pickup_date"] for d in daily] == ["2024-03-04", "2024-03-05"]
    assert report["dashboard_extracts"]["borough_flows"] == 2


def test_volume_anomaly_flags_a_collapsed_day(spark):
    from datetime import date
    from lakehouse.gold import flag_volume_anomalies
    daily = spark.createDataFrame(
        [(date(2024, 1, d), 5 if d == 10 else 100) for d in range(1, 21)], "pickup_date date, trips long")
    flagged = [str(r.pickup_date) for r in flag_volume_anomalies(daily).where("volume_anomaly").collect()]
    assert flagged == ["2024-01-10"]


def test_card_tip_rate_is_a_ratio_of_sums_not_a_mean_of_ratios(spark, lake, monkeypatch):
    monkeypatch.setattr(gold, "SITE_DATA", lake / "site")
    write_month(lake, "2024-03", [
        trip(datetime(2024, 3, 4, 8), fare_amount=0.01, tip_amount=5.0, total_amount=5.01),
        trip(datetime(2024, 3, 4, 9), fare_amount=100.0, tip_amount=10.0, total_amount=110.0),
    ])
    bronze.run(spark, ["2024-03"])
    silver.run(spark, ["2024-03"])
    gold.run(spark)
    row = spark.read.format("delta").load(config.paths().gold("daily_kpis")).first()
    assert row.avg_card_tip_pct == 15.0  # 15 / 100.01, where a mean of per-trip ratios would give ~25,005%

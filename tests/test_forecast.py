from datetime import date, datetime, timedelta

from pyspark.sql import functions as F

from lakehouse import forecast


def _demand(spark, rows):
    return spark.createDataFrame(rows, "location_id int, pickup_date date, pickup_hour int, trips long")


def test_dense_grid_zero_fills_missing_hours(spark):
    demand = _demand(spark, [(1, date(2024, 1, 1), 0, 5), (1, date(2024, 1, 1), 3, 2)])
    grid = forecast.dense_hourly(spark, demand, [1])
    assert [r.trips for r in grid.orderBy("ts").collect()] == [5, 0, 0, 2]


def test_features_only_look_back_at_least_a_day(spark):
    start = date(2024, 1, 1)
    # trips = running hour index, so a lag of h hours must equal index - h exactly
    rows = [(7, start + timedelta(days=i // 24), i % 24, i) for i in range(24 * 20)]
    feats = forecast.add_features(forecast.dense_hourly(spark, _demand(spark, rows), [7]), "2024-01-15")

    r = feats.orderBy(F.desc("ts")).first()
    assert r.lag_24 == r.trips - 24
    assert r.lag_168 == r.trips - 168
    assert r.same_hour_mean_7d == r.trips - 24 * 4  # mean of lags 24..168 step 24
    assert feats.agg(F.min("ts")).first()[0] == datetime(2024, 1, 15, 0)  # rows without a 336h history dropped


def test_zone_mean_ignores_the_test_period(spark):
    start = date(2024, 1, 1)
    rows = [(7, start + timedelta(days=d), h, 1 if d < 16 else 100) for d in range(20) for h in range(24)]
    feats = forecast.add_features(forecast.dense_hourly(spark, _demand(spark, rows), [7]), "2024-01-17")
    assert {r.zone_mean for r in feats.select("zone_mean").distinct().collect()} == {1.0}


def test_score(spark):
    df = spark.createDataFrame([(10.0, 8.0), (0.0, 2.0)], "label double, p double")
    assert forecast.score(df, "p") == {"rmse": 2.0, "mae": 2.0, "wape_pct": 40.0}


def test_rows_touching_a_source_gap_are_excluded(spark):
    start = date(2024, 1, 1)
    rows = [(7, start + timedelta(days=d), h, 10) for d in range(30) for h in range(24)]
    hourly = forecast.dense_hourly(spark, _demand(spark, rows), [7])
    feats = forecast.add_features(hourly, "2024-01-25", exclude_dates=["2024-01-16"])

    days = {r.d for r in feats.select(F.to_date("ts").alias("d")).distinct().collect()}
    # 01-16 itself, the 7 days whose same-hour lookback reaches it, and 01-30 via lag_336
    excluded = {date(2024, 1, d) for d in [*range(16, 24), 30]}
    assert days.isdisjoint(excluded)
    assert {date(2024, 1, 15), date(2024, 1, 24), date(2024, 1, 29)} <= days

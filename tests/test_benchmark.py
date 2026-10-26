from lakehouse import benchmark


def test_conf_override_is_restored(spark):
    before = spark.conf.get("spark.sql.adaptive.enabled")
    with benchmark._conf(spark, spark__sql__adaptive__enabled="false"):
        assert spark.conf.get("spark.sql.adaptive.enabled") == "false"
    assert spark.conf.get("spark.sql.adaptive.enabled") == before


def test_compare_reports_speedup():
    r = benchmark._compare("x", "q", ("slow", 9.0), ("fast", 3.0))
    assert r["speedup_x"] == 3.0
    assert r["baseline"] == {"setting": "slow", "seconds": 9.0}


def test_active_files_and_skipping_follow_the_delta_log(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.createDataFrame([(i,) for i in range(1, 11)], "zone int").repartition(1) \
        .write.format("delta").save(path)
    spark.createDataFrame([(i,) for i in range(100, 111)], "zone int").repartition(1) \
        .write.format("delta").mode("append").save(path)

    files = benchmark.active_files(path)
    assert len(files) == 2
    assert benchmark.files_possibly_matching(files, "zone", 5) == 1
    assert benchmark.files_possibly_matching(files, "zone", 50) == 0

    spark.createDataFrame([(7,)], "zone int").write.format("delta").mode("overwrite").save(path)
    assert len(benchmark.active_files(path)) == 1  # removed files drop out

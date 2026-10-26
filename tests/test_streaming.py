from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from lakehouse import bronze, config, silver, streaming
from tests_support import trip, write_month


def _seed(spark, lake):
    write_month(lake, "2024-03", [
        trip(datetime(2024, 3, 1, 8, 0)),
        trip(datetime(2024, 3, 1, 8, 5)),
        trip(datetime(2024, 3, 1, 8, 20)),               # next 15-minute window
        trip(datetime(2024, 3, 2, 9, 0), PULocationID=132),
        trip(datetime(2024, 3, 3, 23, 50)),
    ])
    bronze.run(spark, ["2024-03"])
    silver.run(spark, ["2024-03"])


def test_replay_matches_batch_aggregate(spark, lake):
    _seed(spark, lake)
    report = streaming.replay(spark, "2024-03")

    assert report["files_replayed"] == 3
    assert report["micro_batches"] == 3
    assert report["rows_streamed"] == 5
    assert report["windows_mismatched_vs_batch"] == 0
    assert report["rows_dropped_by_watermark"] == 0
    rows = {(str(r.window_start), r.location_id): r.trips
            for r in spark.read.format("delta").load(config.paths().gold("stream_zone_15min")).collect()}
    assert rows[("2024-03-01 08:00:00", 161)] == 2
    assert rows[("2024-03-01 08:15:00", 161)] == 1


def test_late_file_behind_the_watermark_is_dropped(spark, lake):
    _seed(spark, lake)
    streaming.replay(spark, "2024-03")
    target = config.paths().gold("stream_zone_15min")

    # a straggler file for 1 March arrives after the stream has moved past 3 March
    late = pa.table({"pickup_ts": pa.array([datetime(2024, 3, 1, 8, 1)], pa.timestamp("us")),
                     "pu_location_id": pa.array([161], pa.int32()), "total_amount": [20.8]})
    pq.write_table(late, lake / "stream" / "landing" / "trips_late.parquet")
    progress = streaming.run_stream(spark, target)

    assert streaming.dropped_by_watermark(progress) == 1
    window = spark.read.format("delta").load(target).where(
        "location_id = 161 AND window_start = '2024-03-01 08:00:00'").first()
    assert window.trips == 2  # unchanged


def test_rerun_after_restart_does_not_double_count(spark, lake):
    _seed(spark, lake)
    streaming.replay(spark, "2024-03")
    target = config.paths().gold("stream_zone_15min")
    progress = streaming.run_stream(spark, target)  # nothing new in landing: checkpoint skips it all

    assert progress == []
    assert sum(r.trips for r in spark.read.format("delta").load(target).collect()) == 5

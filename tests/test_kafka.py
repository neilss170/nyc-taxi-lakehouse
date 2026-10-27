import socket
import uuid
from datetime import datetime

import pytest

from lakehouse import config, kafka_stream
from test_streaming import _seed


def _broker_up() -> bool:
    host, port = kafka_stream.bootstrap().rsplit(":", 1)
    try:
        socket.create_connection((host, int(port)), timeout=2).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _broker_up(), reason="no Kafka broker at KAFKA_BOOTSTRAP")


def test_replay_in_two_halves_matches_batch(spark, lake):
    _seed(spark, lake)  # 1, 2 and 3 March: split on day 2 puts one day in the first half
    report = kafka_stream.replay(spark, "2024-03", split_day=2)

    assert [r["messages_produced"] for r in report["consumer_runs"]] == [3, 2]
    assert [r["rows_consumed"] for r in report["consumer_runs"]] == [3, 2]  # the restart resumed, not re-read
    assert report["offsets_consumed"] == report["messages_produced"] == 5
    assert report["topic_partitions"] == 3
    assert report["windows_mismatched_vs_batch"] == 0
    assert report["rows_dropped_by_watermark"] == 0
    rows = {(str(r.window_start), r.location_id): r.trips
            for r in spark.read.format("delta").load(config.paths().gold("kafka_zone_15min")).collect()}
    assert rows[("2024-03-01 08:00:00", 161)] == 2
    assert rows[("2024-03-02 09:00:00", 132)] == 1


def test_late_message_behind_the_watermark_is_dropped(spark, lake):
    _seed(spark, lake)
    trips = spark.read.format("delta").load(config.paths().silver)
    topic, target = f"test-{uuid.uuid4().hex[:8]}", str(lake / "kafka_target")
    checkpoint = str(lake / "kafka_checkpoint")
    kafka_stream.windowed_counts(trips.select(*kafka_stream.STREAM_COLUMNS).limit(0)).write.format("delta").save(target)
    kafka_stream.produce(trips, topic)
    kafka_stream.consume(spark, topic, target, checkpoint)

    # a straggler for 1 March arrives after the stream has moved past 3 March
    late = spark.createDataFrame([(datetime(2024, 3, 1, 8, 1), 161, 20.8)], kafka_stream.VALUE_SCHEMA)
    kafka_stream.produce(late, topic)
    progress = kafka_stream.consume(spark, topic, target, checkpoint)

    assert kafka_stream.dropped_by_watermark(progress) == 1
    window = spark.read.format("delta").load(target).where(
        "location_id = 161 AND window_start = '2024-03-01 08:00:00'").first()
    assert window.trips == 2  # unchanged


def test_rerun_with_no_new_messages_reads_nothing(spark, lake):
    _seed(spark, lake)
    trips = spark.read.format("delta").load(config.paths().silver)
    topic, target = f"test-{uuid.uuid4().hex[:8]}", str(lake / "kafka_target")
    checkpoint = str(lake / "kafka_checkpoint")
    kafka_stream.windowed_counts(trips.select(*kafka_stream.STREAM_COLUMNS).limit(0)).write.format("delta").save(target)
    kafka_stream.produce(trips, topic)
    kafka_stream.consume(spark, topic, target, checkpoint)

    assert kafka_stream.consume(spark, topic, target, checkpoint) == []
    assert sum(r.trips for r in spark.read.format("delta").load(target).collect()) == 5

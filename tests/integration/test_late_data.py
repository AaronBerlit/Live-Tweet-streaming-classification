"""§12.4 / C17: a record older than the watermark is dropped AND counted.

Silently dropping late data with no counter is the failure mode the PRD names.
This file pins down both halves, on real streaming queries.

It also records a finding. In Spark 3.5 a watermark on a deduplication
operator only evicts old state; it does not reject late input. The first test
below characterises that, which is why the pipeline drops late rows itself
(`LateDataFilter`). If a future Spark changes this, that test fails and says so.

Three files feed one micro-batch each. The watermark is computed at the end of
a batch and applies to the next, so order matters:

  batch 0: an on-time record at T
  batch 1: a record at T+10min      -> watermark advances to T+8min
  batch 2: a record at T again      -> 8 minutes behind the watermark: late
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from pipeline import preprocess
from pipeline.stream_job import (
    LateDataFilter,
    next_batch_watermark,
    to_utc,
    watermark_from_progress,
)

pytestmark = [pytest.mark.spark, pytest.mark.stream]

SCHEMA = "text string, user string, event_time timestamp"
T = datetime(2026, 9, 22, 14, 0, 0, tzinfo=timezone.utc)
WATERMARK = "2 minutes"


def write_batch(directory, name, records, order):
    path = directory / name
    path.write_text(
        "\n".join(json.dumps({**r, "event_time": r["event_time"].isoformat()}) for r in records),
        encoding="utf-8",
    )
    # The file source reads files in modification-time order; make it explicit.
    stamp = time.time() - 1000 + order
    os.utime(path, (stamp, stamp))


@pytest.fixture
def records_dir(tmp_path):
    directory = tmp_path / "late"
    directory.mkdir()
    write_batch(directory, "a.json", [{"text": "on time", "user": "early", "event_time": T}], 0)
    write_batch(
        directory, "b.json",
        [{"text": "moves the watermark", "user": "ahead", "event_time": T + timedelta(minutes=10)}], 1,
    )
    write_batch(directory, "c.json", [{"text": "arrives late", "user": "late", "event_time": T}], 2)
    return directory


def deduplicated_stream(spark, directory):
    """Exactly the stream job's steps 5, 8 and 9."""
    stream = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(directory))
    prepared = preprocess.prepare(stream, emoji_strategy="strip", drop_retweets=False)
    return prepared.withWatermark("event_time", WATERMARK).dropDuplicatesWithinWatermark(
        [preprocess.DEDUP_KEY]
    )


def test_spark_dedup_alone_lets_the_late_record_through(spark, records_dir):
    """Characterisation: the reason LateDataFilter exists."""
    query = (
        deduplicated_stream(spark, records_dir)
        .writeStream.format("memory").queryName("late_spark_only")
        .outputMode("append").trigger(availableNow=True).start()
    )
    query.awaitTermination(timeout=180)

    users = {row["user"] for row in spark.sql("select user from late_spark_only").collect()}
    progress = query.recentProgress  # plain dicts in PySpark 3.5
    watermarks = [watermark_from_progress(p) for p in progress]
    spark_dropped = sum(
        operator.get("numRowsDroppedByWatermark", 0)
        for p in progress for operator in p.get("stateOperators", [])
    )

    assert len(progress) >= 3, "each file must be its own micro-batch"
    assert watermarks[-1] == T + timedelta(minutes=8), watermarks
    assert "late" in users and spark_dropped == 0, (
        "Spark's dedup now drops late rows itself -- LateDataFilter may be redundant"
    )


def test_pipeline_drops_and_counts_the_late_record(spark, records_dir):
    """The fix, on a real stream: the late row is excluded and counted once."""
    late = LateDataFilter()
    kept_users: list[str] = []

    def write(batch, _batch_id):
        rows = [row.asDict() for row in batch.collect()]
        for row in rows:
            row["event_time"] = to_utc(row["event_time"])
        kept_users.extend(row["user"] for row in late.apply(rows))

    query = (
        deduplicated_stream(spark, records_dir)
        .writeStream.foreachBatch(write).trigger(availableNow=True).start()
    )
    late.watermark_source = lambda: next_batch_watermark(query.lastProgress, 2)
    query.awaitTermination(timeout=180)

    assert sorted(kept_users) == ["ahead", "early"], kept_users
    assert late.dropped == 1


# --- the filter's boundaries -------------------------------------------------


def rows_at(*minutes):
    return [{"event_time": T + timedelta(minutes=m), "i": i} for i, m in enumerate(minutes)]


def test_no_watermark_yet_keeps_everything():
    late = LateDataFilter()
    assert len(late.apply(rows_at(0, 5))) == 2 and late.dropped == 0


def test_a_row_exactly_at_the_watermark_is_kept():
    late = LateDataFilter()
    late.watermark_source = lambda: T + timedelta(minutes=5)
    kept = late.apply(rows_at(4, 5, 6))
    assert [r["i"] for r in kept] == [1, 2]
    assert late.dropped == 1


def test_count_is_cumulative_across_batches():
    late = LateDataFilter()
    late.watermark_source = lambda: T + timedelta(minutes=5)
    late.apply(rows_at(1, 2))
    late.apply(rows_at(3, 9))
    assert late.dropped == 3


def test_a_failing_watermark_source_never_drops_data():
    def broken():
        raise RuntimeError("query gone")

    late = LateDataFilter()
    late.watermark_source = broken
    assert len(late.apply(rows_at(0))) == 1 and late.dropped == 0


def test_epoch_watermark_means_no_watermark():
    assert watermark_from_progress({"eventTime": {"watermark": "1970-01-01T00:00:00.000Z"}}) is None
    assert watermark_from_progress(None) is None
    assert watermark_from_progress({"eventTime": {"watermark": "2026-09-22T14:08:00.000Z"}}) == (
        T + timedelta(minutes=8)
    )


# --- the count reaches the dashboard -----------------------------------------


def test_progress_listener_reports_the_pipeline_count(spark, records_dir):
    """End to end: the drop reaches `heartbeat.late_records_dropped`."""
    from pymongo import MongoClient

    from pipeline.config import COLL_HEARTBEAT, DB_NAME, HEARTBEAT_ID, settings
    from pipeline.stream_job import ProgressListener

    assert DB_NAME == "sentiment_stream_test"
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception:  # noqa: BLE001
        pytest.skip("no MongoDB reachable")
    client[DB_NAME].drop_collection(COLL_HEARTBEAT)

    late = LateDataFilter()

    def write(batch, _batch_id):
        rows = [row.asDict() for row in batch.collect()]
        for row in rows:
            row["event_time"] = to_utc(row["event_time"])
        late.apply(rows)

    listener = ProgressListener("run_late_test", "socket", late=late)
    spark.streams.addListener(listener)
    try:
        query = (
            deduplicated_stream(spark, records_dir)
            .writeStream.foreachBatch(write).trigger(availableNow=True).start()
        )
        late.watermark_source = lambda: next_batch_watermark(query.lastProgress, 2)
        query.awaitTermination(timeout=180)
        # Listener callbacks are asynchronous; give the last one a moment.
        deadline = time.monotonic() + 20
        heartbeat = None
        while time.monotonic() < deadline:
            heartbeat = client[DB_NAME][COLL_HEARTBEAT].find_one({"_id": HEARTBEAT_ID})
            if heartbeat and heartbeat.get("late_records_dropped"):
                break
            time.sleep(0.5)
    finally:
        spark.streams.removeListener(listener)
        client.drop_database(DB_NAME)
        client.close()

    assert heartbeat is not None, "the listener never wrote a heartbeat"
    assert heartbeat["late_records_dropped"] == 1


def test_next_batch_watermark_follows_sparks_rule():
    """max(previous watermark, newest event time - delay), never backwards."""
    def progress(watermark, newest):
        return {"eventTime": {"watermark": watermark, "max": newest}}

    # First data seen: the bound is newest - delay.
    assert next_batch_watermark(
        progress("1970-01-01T00:00:00.000Z", "2026-09-22T14:10:00.000Z"), 2
    ) == T + timedelta(minutes=8)
    # An older batch never moves the watermark backwards.
    assert next_batch_watermark(
        progress("2026-09-22T14:08:00.000Z", "2026-09-22T14:01:00.000Z"), 2
    ) == T + timedelta(minutes=8)
    assert next_batch_watermark(None, 2) is None
    assert next_batch_watermark({"eventTime": {}}, 2) is None

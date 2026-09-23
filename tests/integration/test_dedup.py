"""C18 / §12.4: windowed deduplication, late data, and the helpers around them.

The deduplication test drives a real streaming DataFrame through
`dropDuplicatesWithinWatermark` — the same operator `stream_job.py` uses. A
test that deduplicated a batch DataFrame would pass while the stream happily
double-counted.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from pipeline import preprocess

pytestmark = [pytest.mark.spark, pytest.mark.stream]

SCHEMA = "text string, user string, event_time timestamp"
BASE = datetime(2026, 9, 22, 14, 30, 0, tzinfo=timezone.utc)


def write_records(directory, records, name="part-000.json"):
    payload = "\n".join(
        json.dumps({**record, "event_time": record["event_time"].isoformat()})
        for record in records
    )
    (directory / name).write_text(payload, encoding="utf-8")


def run_stream(spark, directory, table, *, watermark="2 minutes"):
    """Preprocess → watermark → dropDuplicatesWithinWatermark, as the job does."""
    stream = (
        spark.readStream.schema(SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(directory))
    )
    assert stream.isStreaming

    prepared = preprocess.prepare(stream, emoji_strategy="strip", drop_retweets=False)
    deduplicated = prepared.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(
        [preprocess.DEDUP_KEY]
    )

    query = (
        deduplicated.writeStream.format("memory")
        .queryName(table)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(timeout=180)
    return spark.sql(f"select * from {table}")


@pytest.fixture
def records_dir(tmp_path):
    directory = tmp_path / "records"
    directory.mkdir()
    return directory


def test_identical_record_injected_three_times_is_counted_once(spark, records_dir):
    """C18. The same record, three times, inside the watermark."""
    record = {"text": "the metro is delayed again", "user": "alice"}
    write_records(
        records_dir,
        [
            {**record, "event_time": BASE},
            {**record, "event_time": BASE + timedelta(seconds=10)},
            {**record, "event_time": BASE + timedelta(seconds=20)},
        ],
    )

    result = run_stream(spark, records_dir, "dedup_three")
    assert result.count() == 1, "the same record was counted more than once"


def test_different_users_posting_identical_text_are_two_records(spark, records_dir):
    """Deduplication must not collapse two people saying the same thing."""
    write_records(
        records_dir,
        [
            {"text": "power cut again", "user": "alice", "event_time": BASE},
            {"text": "power cut again", "user": "bob", "event_time": BASE},
        ],
    )

    result = run_stream(spark, records_dir, "dedup_users")
    assert result.count() == 2


def test_cosmetically_different_duplicates_still_collapse(spark, records_dir):
    """Case and URL noise must not smuggle a duplicate past the dedup key."""
    write_records(
        records_dir,
        [
            {"text": "Power Cut Again", "user": "alice", "event_time": BASE},
            {
                "text": "power   cut again http://t.co/abc",
                "user": "alice",
                "event_time": BASE + timedelta(seconds=5),
            },
        ],
    )

    result = run_stream(spark, records_dir, "dedup_cosmetic")
    assert result.count() == 1


def test_distinct_records_all_survive(spark, records_dir):
    """The guard against a dedup key so coarse it eats real data."""
    write_records(
        records_dir,
        [
            {"text": f"distinct message {index}", "user": "alice", "event_time": BASE}
            for index in range(25)
        ],
    )

    result = run_stream(spark, records_dir, "dedup_distinct")
    assert result.count() == 25


# --- the helpers the streaming job relies on --------------------------------


def test_window_flooring_matches_spark_windows(spark):
    """`BatchWriter._floor` must agree with Spark's own `window()` boundaries.

    If these disagree, records land in one window while their aggregate is
    written under another, and every count on the dashboard is subtly wrong.
    """
    from pyspark.sql import functions as F

    from pipeline.stream_job import BatchWriter

    moments = [
        BASE,
        BASE + timedelta(seconds=59),
        BASE + timedelta(minutes=1),
        BASE + timedelta(minutes=3, seconds=17),
    ]
    frame = spark.createDataFrame([(moment,) for moment in moments], "event_time timestamp")
    spark_starts = [
        row["w"]["start"]
        for row in frame.select(F.window("event_time", "1 minute").alias("w")).collect()
    ]

    writer = BatchWriter.__new__(BatchWriter)
    writer.window_minutes = 1

    for moment, spark_start in zip(moments, spark_starts):
        # Spark returns a naive local-time datetime, exactly as collect() does
        # everywhere else, so it is normalised the same way.
        from pipeline.stream_job import to_utc

        assert writer._floor(moment) == to_utc(spark_start)


def test_to_utc_tags_naive_datetimes_rather_than_shifting_them():
    """PySpark hands back naive *local* datetimes; they must convert, not be
    relabelled, or every Spark timestamp lands hours away from the seeded ones."""
    from pipeline.stream_job import to_utc

    aware = datetime(2026, 9, 22, 9, 0, tzinfo=timezone.utc)
    assert to_utc(aware) == aware
    assert to_utc(None) is None

    naive_local = aware.astimezone().replace(tzinfo=None)
    assert to_utc(naive_local) == aware

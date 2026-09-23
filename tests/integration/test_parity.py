"""Training/serving parity -- the Rule A guard (PRD 12.3, non-negotiable).

Feed identical records through the batch cleaning path and the streaming
cleaning path and assert byte-identical `text_clean`.

This is not a formality. Training/serving skew is the most common silent
defect in this class of system: the model sees differently-cleaned text at
inference than at training, accuracy quietly drops, and the cause is invisible
because both paths look correct in isolation.

The streaming side here is a genuine streaming DataFrame driven by Spark's
file source, not a batch DataFrame in disguise. A test that compared batch
against batch would pass forever while the real stream diverged.
"""

from __future__ import annotations

import json

import pytest

from pipeline import preprocess

pytestmark = [pytest.mark.spark, pytest.mark.stream]

# Every adversarial shape from the Phase 2 gate, plus the ordinary cases.
# If batch and stream can disagree anywhere, it will be on one of these.
RECORDS = [
    {"text": "Hello WORLD", "user": "alice"},
    {"text": "check http://t.co/abc123 now", "user": "bob"},
    {"text": "thanks @alice and @bob_42", "user": "carol"},
    {"text": "great #Election results", "user": "dave"},
    {"text": "too    many\t\tspaces   here", "user": "erin"},
    {"text": "", "user": "frank"},
    {"text": "   \t  ", "user": "grace"},
    {"text": "http://t.co/onlyalink", "user": "heidi"},
    {"text": "line one\nline two", "user": "ivan"},
    {"text": "spam " * 200, "user": "judy"},
    {"text": "مرحبا بالعالم", "user": "karl"},
    {"text": "great day \U0001F600 awful \U0001F622", "user": "lena"},
    {"text": "look \U0001F6F8 unlisted emoji", "user": "mike"},
    {"text": "!!! ???", "user": "nina"},
    {"text": "mixed #Tags and @mentions and http://x.co/1 \U0001F44D", "user": "omar"},
]

SCHEMA = "text string, user string"


@pytest.fixture
def records_dir(tmp_path):
    """The same records on disk as newline-delimited JSON, for the file source."""
    directory = tmp_path / "records"
    directory.mkdir()
    payload = "\n".join(json.dumps(record) for record in RECORDS)
    (directory / "part-000.json").write_text(payload, encoding="utf-8")
    return directory


def _batch_clean(spark, emoji_strategy):
    """Exactly what pipeline/clean_batch.py does."""
    df = spark.createDataFrame(RECORDS, SCHEMA)
    return preprocess.prepare(df, emoji_strategy=emoji_strategy, drop_retweets=False)


def _stream_clean(spark, records_dir, emoji_strategy, table):
    """Exactly what pipeline/stream_job.py does, on a real streaming DataFrame."""
    stream = (
        spark.readStream.schema(SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(records_dir))
    )
    assert stream.isStreaming, "the parity test must exercise a real stream"

    cleaned = preprocess.prepare(
        stream, emoji_strategy=emoji_strategy, drop_retweets=False
    )
    query = (
        cleaned.writeStream.format("memory")
        .queryName(table)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(timeout=120)
    return spark.sql(f"select * from {table}")


@pytest.mark.parametrize("emoji_strategy", ["strip", "keep", "map"])
def test_batch_and_stream_produce_identical_clean_text(spark, records_dir, emoji_strategy):
    batch = _batch_clean(spark, emoji_strategy)
    stream = _stream_clean(
        spark, records_dir, emoji_strategy, f"parity_{emoji_strategy}"
    )

    def by_user(df):
        return {
            row["user"]: row[preprocess.TEXT_CLEAN]
            for row in df.select("user", preprocess.TEXT_CLEAN).collect()
        }

    batch_rows, stream_rows = by_user(batch), by_user(stream)

    assert set(batch_rows) == set(stream_rows), "the two paths saw different records"
    for user in sorted(batch_rows):
        assert batch_rows[user] == stream_rows[user], (
            f"training/serving skew for {user!r} under emoji={emoji_strategy}:\n"
            f"  batch  {batch_rows[user]!r}\n"
            f"  stream {stream_rows[user]!r}"
        )


def test_batch_and_stream_produce_identical_hashtags(spark, records_dir):
    batch = _batch_clean(spark, "strip")
    stream = _stream_clean(spark, records_dir, "strip", "parity_hashtags")

    def by_user(df):
        return {
            row["user"]: list(row[preprocess.HASHTAGS])
            for row in df.select("user", preprocess.HASHTAGS).collect()
        }

    assert by_user(batch) == by_user(stream)


def test_batch_and_stream_produce_identical_dedup_keys(spark, records_dir):
    """If these diverge, windowed dedup silently stops working in the stream."""
    batch = _batch_clean(spark, "strip")
    stream = _stream_clean(spark, records_dir, "strip", "parity_dedup")

    def by_user(df):
        return {
            row["user"]: row[preprocess.DEDUP_KEY]
            for row in df.select("user", preprocess.DEDUP_KEY).collect()
        }

    assert by_user(batch) == by_user(stream)

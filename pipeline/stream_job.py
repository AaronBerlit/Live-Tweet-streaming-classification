"""C12/C14/C16/C17/C18/C22/C23: the Spark Structured Streaming job.

Order of operations follows PRD 8:

  1. assert_versions()
  2. load the persisted PipelineModel ONCE, logging the load duration
  3. create a `runs` document
  4. readStream from socket or kafka
  5. preprocess -- the same module the batch job used, never a copy
  6. model.transform() -> prediction + probability; confidence = max(probability)
  7. apply the neutral policy if the model was trained with one
  8. .withWatermark("event_time", N minutes)
  9. .dropDuplicatesWithinWatermark(["dedup_key"])
 10. window the records into one-minute event-time buckets
 11. foreachBatch, in one function, writing windows / scored / hashtags
 12. checkpoint to /sentiment/checkpoints/<run_id>

Two design decisions worth stating
----------------------------------
**Row-level foreachBatch, windows recomputed in Mongo.** A streaming query can
emit aggregates or rows, not both. The PRD asks for one `foreachBatch` writing
all the collections, which rules out a stateful `groupBy(window, prediction)`
query -- its batches contain only aggregates, and the `scored` rows would be
gone. So the stream stays row-level, and each batch recomputes the affected
windows' counts **from the `scored` collection**. Recomputation rather than
`$inc` is what makes the writes idempotent (Rule B): replaying a batch after a
restart recomputes the same counts instead of adding them again.

**Measurements come from Spark, not from a stopwatch around the sink.** The
heartbeat is written by `ProgressListener` using Spark's own query progress.
Timing the `foreachBatch` body would measure only the MongoDB write and
exclude the read, preprocessing, scoring and deduplication -- and then publish
that as end-to-end latency, which would make C22 and C23 flattering fictions.

**Late records are dropped here, explicitly.** In Spark 3.5 a watermark on a
deduplication operator evicts old state but does not reject late input, so
`dropDuplicatesWithinWatermark` alone lets late rows through (measured, see
`LateDataFilter` and tests/integration/test_late_data.py). Each batch drops
rows older than the watermark Spark reported for the previous batch, and counts
them into `heartbeat.late_records_dropped`.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from api.models import Run, SparkConfig, Window
from pipeline import preprocess, versions
from pipeline.hashtags import upserts_for_window as hashtag_upserts
from pipeline.config import (
    COLL_BATCHES,
    COLL_HASHTAGS,
    COLL_HEARTBEAT,
    COLL_RUNS,
    COLL_SCORED,
    COLL_WINDOWS,
    DB_NAME,
    DEFAULT_REPLAY_RATE,
    DEFAULT_TRIGGER_SECONDS,
    DEFAULT_WATERMARK_MINUTES,
    DEFAULT_WINDOW_MINUTES,
    HEARTBEAT_ID,
    KAFKA_TOPIC,
    SOCKET_HOST,
    SOCKET_PORT,
    settings,
)
from storage import hdfs
from trainer.evaluate import load_manifest
from trainer.label_policy import NeutralPolicy, apply_neutral, index_to_label

#: The producer's newline-delimited JSON payload.
PAYLOAD_SCHEMA = StructType(
    [
        StructField("text", StringType()),
        StructField("user", StringType()),
        StructField("label", StringType()),
        StructField("event_time", TimestampType()),
        StructField("dedup_key", StringType()),
    ]
)

#: Epoch used to anchor window flooring, matching Spark's own window origin.
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def to_utc(moment: datetime | None) -> datetime | None:
    """Return a timezone-aware UTC datetime.

    `DataFrame.collect()` hands back **naive local-time** datetimes for
    timestamp columns, regardless of `spark.sql.session.timeZone`, because
    PySpark builds them with `datetime.fromtimestamp()`. pymongo then stores a
    naive datetime as if it were UTC, which silently shifts every event time,
    window boundary and window `_id` by the machine's UTC offset -- 5h30m here.
    Seeded documents (written timezone-aware) and Spark documents would then
    land on disjoint parts of the same chart.
    """
    if moment is None:
        return None
    if moment.tzinfo is None:
        # A naive datetime is interpreted as local time, which is what PySpark
        # produced, and converted to UTC.
        return moment.astimezone(timezone.utc)
    return moment.astimezone(timezone.utc)


def _progress_time(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        moment = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    # Spark reports the epoch before it has seen any event time; that means
    # "no watermark yet", not a 56-year lag.
    return None if moment.year <= 1970 else moment


def watermark_from_progress(progress: dict | None) -> datetime | None:
    """The watermark that was in force FOR the batch this progress describes."""
    if not progress:
        return None
    return _progress_time((progress.get("eventTime") or {}).get("watermark"))


def next_batch_watermark(progress: dict | None, delay_minutes: float) -> datetime | None:
    """The watermark in force for the batch AFTER the one `progress` describes.

    A progress report carries the watermark that applied to its own batch,
    which is one step behind: Spark advances the watermark at the end of each
    batch, to (newest event time seen) - (delay), and never moves it backwards.
    Reading only `eventTime.watermark` would therefore judge every batch
    against the previous batch's bound and let one batch's worth of late rows
    through -- measured in tests/integration/test_late_data.py.
    """
    if not progress:
        return None
    times = progress.get("eventTime") or {}
    previous = _progress_time(times.get("watermark"))
    newest = _progress_time(times.get("max"))
    candidate = newest - timedelta(minutes=delay_minutes) if newest else None
    known = [moment for moment in (previous, candidate) if moment is not None]
    return max(known) if known else None


class LateDataFilter:
    """Drops rows older than the watermark in force for this batch, and counts them.

    Why this exists (C17): in Spark 3.5 a watermark on a deduplication operator
    only *evicts old state*; it does not reject late *input*. Only stateful
    aggregations and joins do that. Measured: a record eight minutes behind the
    watermark passed through both `dropDuplicatesWithinWatermark` and
    `dropDuplicates`, each reporting `numRowsDroppedByWatermark = 0`. Without
    this filter, late rows silently landed in old windows and the "late
    records dropped" counter was always 0 -- a counter that measured nothing.

    The watermark used is the one Spark establishes at the end of the previous
    batch -- max(previous watermark, newest event time - delay), derived from
    `query.lastProgress` by `next_batch_watermark` -- which is exactly the bound
    Spark would apply to this batch in a stateful aggregation. `watermark_source` is
    wired after the query starts, because the query object does not exist when
    the writer is built.
    """

    def __init__(self) -> None:
        self.watermark_source = None  # set to a zero-argument callable
        self.dropped = 0

    def current_watermark(self) -> datetime | None:
        if self.watermark_source is None:
            return None
        try:
            return self.watermark_source()
        except Exception:  # noqa: BLE001 - never let the filter kill a batch
            return None

    def apply(self, rows: list[dict]) -> list[dict]:
        watermark = self.current_watermark()
        if watermark is None:
            return rows
        kept = [row for row in rows if row["event_time"] >= watermark]
        self.dropped += len(rows) - len(kept)
        return kept


class BatchWriter:
    """Writes one micro-batch's rows, windows and hashtags to MongoDB.

    Deliberately does NOT write the heartbeat: the honest timing lives in
    `ProgressListener`, which sees Spark's own end-to-end numbers.
    """

    def __init__(self, run_id: str, window_minutes: int) -> None:
        self.run_id = run_id
        self.window_minutes = window_minutes
        self.late = LateDataFilter()
        self._client = MongoClient(settings.mongo_uri)
        self._db = self._client[DB_NAME]

    def __call__(self, batch, batch_id: int) -> None:
        started = time.monotonic()
        now = datetime.now(timezone.utc)

        rows = [self._normalise(row.asDict()) for row in batch.collect()]
        # Late rows are dropped before anything is written, so they can never
        # reach `scored` or be folded into a window that was already reported.
        rows = self.late.apply(rows)
        if not rows:
            return

        self._write_scored(rows)
        touched = sorted({self._floor(row["event_time"]) for row in rows})
        self._recompute_windows(touched, now)
        self._recompute_hashtags(touched)

        elapsed_ms = int((time.monotonic() - started) * 1000)
        print(
            f"batch {batch_id:>5} | {len(rows):>6,} rows | "
            f"{len(touched)} window(s) | sink {elapsed_ms:,} ms",
            flush=True,
        )

    @staticmethod
    def _normalise(row: dict) -> dict:
        """Make every timestamp timezone-aware UTC before it reaches Mongo."""
        row["event_time"] = to_utc(row["event_time"])
        row["ingest_time"] = to_utc(row["ingest_time"])
        return row

    # --- scored --------------------------------------------------------------

    def _write_scored(self, rows) -> None:
        """Upsert on `(dedup_key, event_time)`.

        The frozen 7.2 contract gives `scored` an ObjectId `_id`, so the
        idempotency key cannot be the `_id`. Upserting on the indexed
        `dedup_key` *plus* the event time achieves the same thing using only
        contract fields.

        Why the event time is part of the key: `dedup_key` is
        `sha1(text_clean|user)` and carries no time. Keyed on it alone, a
        record re-emitted by `producer --loop` -- or any user posting the same
        text twice -- would overwrite the earlier document and drag its
        `event_time` into a different window, leaving the older window's count
        permanently disagreeing with `scored`. That is precisely the 12.5
        invariant this design exists to guarantee. Within a single window,
        genuine duplicates still collapse, and Spark's
        `dropDuplicatesWithinWatermark` remains the primary deduplication.
        """
        operations = [
            UpdateOne(
                {"dedup_key": row["dedup_key"], "event_time": row["event_time"]},
                {
                    "$set": {
                        "text_raw": row["text"],
                        "text_clean": row[preprocess.TEXT_CLEAN],
                        "prediction": row["prediction_label"],
                        "confidence": float(row["confidence"]),
                        "hashtags": list(row[preprocess.HASHTAGS] or []),
                        "event_time": row["event_time"],
                        "ingest_time": row["ingest_time"],
                        "dedup_key": row["dedup_key"],
                        "run_id": self.run_id,
                        "source": "spark",
                    }
                },
                upsert=True,
            )
            for row in rows
        ]
        self._db[COLL_SCORED].bulk_write(operations, ordered=False)

    # --- windows (C16) -------------------------------------------------------

    def _floor(self, moment: datetime) -> datetime:
        """Floor an event time to its window start, anchored at the epoch.

        Anchoring at the epoch rather than flooring the minute field keeps
        these boundaries identical to Spark's `window()` for any window size,
        not only sizes that divide 60.
        """
        size = timedelta(minutes=self.window_minutes)
        elapsed = moment - EPOCH
        return EPOCH + (elapsed // size) * size

    def _recompute_windows(self, starts: list[datetime], now: datetime) -> None:
        """Recompute each touched window's counts from `scored` and $set them."""
        operations = []
        for start in starts:
            end = start + timedelta(minutes=self.window_minutes)
            grouped = self._db[COLL_SCORED].aggregate(
                [
                    {"$match": {"event_time": {"$gte": start, "$lt": end}}},
                    {
                        "$group": {
                            "_id": "$prediction",
                            "count": {"$sum": 1},
                            "avg_confidence": {"$avg": "$confidence"},
                        }
                    },
                ]
            )
            for group in grouped:
                # A window is never written with count 0: absence and zero are
                # different, and 12.5 depends on the distinction.
                if not group["count"]:
                    continue
                operations.append(
                    UpdateOne(
                        {"_id": Window.make_id(start, group["_id"])},
                        {
                            "$set": {
                                "window_start": start,
                                "window_end": end,
                                "prediction": group["_id"],
                                "count": int(group["count"]),
                                "avg_confidence": round(float(group["avg_confidence"]), 4),
                                "run_id": self.run_id,
                                "source": "spark",
                                "updated_at": now,
                            }
                        },
                        upsert=True,
                    )
                )
        if operations:
            self._db[COLL_WINDOWS].bulk_write(operations, ordered=False)

    # --- hashtags (C21) ------------------------------------------------------

    def _recompute_hashtags(self, starts: list[datetime]) -> None:
        """Recompute hashtag counts for touched windows (C21).

        The aggregation itself lives in `pipeline/hashtags.py` so the evidence
        scripts and any future backfill share one implementation.
        """
        operations = []
        for start in starts:
            operations.extend(
                hashtag_upserts(
                    self._db[COLL_SCORED], start, self.window_minutes, self.run_id
                )
            )
        if operations:
            self._db[COLL_HASHTAGS].bulk_write(operations, ordered=False)


def _parse_progress_time(raw: str | None) -> datetime | None:
    """Parse an ISO timestamp from Spark's query progress, or None."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


class ProgressListener(StreamingQueryListener):
    """Writes the heartbeat from Spark's own progress metrics (C17, C22, C23).

    Everything published here is measured by Spark across the whole trigger --
    source read, preprocessing, model inference, watermarking, deduplication
    and the sink write -- which is what "end-to-end latency per micro-batch"
    has to mean for C22 to be a true claim.
    """

    def __init__(
        self, run_id: str, input_source: str, late: LateDataFilter | None = None
    ) -> None:
        self.run_id = run_id
        self.input_source = input_source
        # The pipeline's own late-data filter, whose count is the real one;
        # Spark's dedup operator never reports a watermark drop (see
        # LateDataFilter).
        self.late = late
        self._client = MongoClient(settings.mongo_uri)
        self._db = self._client[DB_NAME]
        self.spark_dropped = 0

    @property
    def late_records_dropped(self) -> int:
        """Rows dropped for arriving after the watermark, cumulative per run."""
        return self.spark_dropped + (self.late.dropped if self.late else 0)

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark's interface
        print(f"query started: {event.id}", flush=True)

    def onQueryProgress(self, event) -> None:  # noqa: N802
        progress = event.progress
        now = datetime.now(timezone.utc)

        duration_ms = int(progress.durationMs.get("triggerExecution", 0))
        rows_in_batch = int(progress.numInputRows)
        rows_per_sec = float(progress.processedRowsPerSecond or 0.0)

        # Spark also reports progress for empty triggers while the query idles.
        # Writing those to the heartbeat would keep the badge LIVE after the
        # producer died -- "the stream is alive" is not "data is flowing", and
        # DoD check 6 requires a dead producer to read STALLED within 15s.
        if rows_in_batch == 0:
            return

        # Rows any Spark stateful operator discarded as late. With the current
        # plan this stays 0 (see LateDataFilter), but it is kept so a future
        # stateful aggregation's drops are not silently lost.
        for operator in progress.stateOperators or []:
            self.spark_dropped += int(getattr(operator, "numRowsDroppedByWatermark", 0) or 0)

        event_times = progress.eventTime or {}
        watermark = _parse_progress_time(event_times.get("watermark"))
        # Before any event time has been seen Spark reports the epoch as the
        # watermark. That is "no watermark yet", not a 56-year lag.
        if watermark is not None and watermark.year <= 1970:
            watermark = None
        max_event_time = _parse_progress_time(event_times.get("max"))

        document = {
            "last_batch_id": int(progress.batchId),
            "last_batch_at": now,
            "rows_in_batch": rows_in_batch,
            "batch_duration_ms": duration_ms,
            "rows_per_sec": round(rows_per_sec, 1),
            "input_source": self.input_source,
            "watermark": watermark,
            "late_records_dropped": self.late_records_dropped,
            "run_id": self.run_id,
        }

        self._db[COLL_HEARTBEAT].update_one(
            {"_id": HEARTBEAT_ID}, {"$set": document}, upsert=True
        )

        # Append-only history: the heartbeat is current state, this is the
        # series the Stream Monitor charts and `make evidence-velocity` reads.
        # Keyed on (run_id, batch_id) so a replayed batch overwrites its own
        # sample rather than adding a second one.
        self._db[COLL_BATCHES].update_one(
            {"_id": f"{self.run_id}|{progress.batchId}"},
            {
                "$set": {
                    "run_id": self.run_id,
                    "batch_id": int(progress.batchId),
                    "batch_at": now,
                    "rows_in_batch": rows_in_batch,
                    "batch_duration_ms": duration_ms,
                    "rows_per_sec": round(rows_per_sec, 1),
                    "input_source": self.input_source,
                    "watermark": watermark,
                    "max_event_time": max_event_time,
                    "late_records_dropped": self.late_records_dropped,
                }
            },
            upsert=True,
        )

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        print(f"query terminated: {event.id}", flush=True)


def read_source(spark, args):
    """Step 4: readStream from socket or Kafka, yielding the payload columns."""
    if args.source == "socket":
        raw = (
            spark.readStream.format("socket")
            .option("host", args.host)
            .option("port", args.port)
            .option("includeTimestamp", "true")
            .load()
        )
    else:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            # Backpressure: bound each micro-batch, so a backlog (after a
            # restart, or a slow start) drains over many small batches
            # instead of one enormous one that blows the trigger interval.
            .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
            .load()
        )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), PAYLOAD_SCHEMA).alias("payload"),
        F.col("timestamp").alias("ingest_time"),
    )

    return parsed.select(
        F.col("payload.text").alias("text"),
        F.col("payload.user").alias("user"),
        F.col("payload.label").alias("label"),
        # The producer injects event_time. Falling back to ingestion time would
        # silently turn every window into a measure of the replay rate rather
        # than of the data.
        F.coalesce(F.col("payload.event_time"), F.col("ingest_time")).alias("event_time"),
        F.col("ingest_time"),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["socket", "kafka"], default="socket")
    parser.add_argument("--model", default="nb")
    parser.add_argument("--trigger", type=int, default=DEFAULT_TRIGGER_SECONDS)
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW_MINUTES)
    parser.add_argument("--watermark", type=int, default=DEFAULT_WATERMARK_MINUTES)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--host", default=SOCKET_HOST)
    parser.add_argument("--port", type=int, default=SOCKET_PORT)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--starting-offsets", default="latest",
                        help="kafka only; 'earliest' replays the retained log (C13)")
    parser.add_argument("--rate", type=int, default=DEFAULT_REPLAY_RATE,
                        help="the producer's configured rate, recorded on the run")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=2000,
                        help="kafka only; upper bound on records per micro-batch")
    args = parser.parse_args()

    # Step 1.
    try:
        versions.assert_versions()
    except versions.VersionMismatch as exc:
        print(str(exc), file=sys.stderr)
        return 1

    run_id = args.run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    if not hdfs.exists(hdfs.MODELS / args.model):
        print(f"no persisted model at {hdfs.model_uri(args.model)}", file=sys.stderr)
        print(f"run: python -m trainer.train --model {args.model}", file=sys.stderr)
        return 1

    from scripts.spark_session import build

    spark = build(f"stream-{args.source}", kafka=(args.source == "kafka"), mongo=False)

    # Step 2: load the model ONCE. Loading per batch would dominate the
    # latency measurement and make C22 meaningless.
    model_uri = hdfs.model_uri(args.model)
    print(f"loading PipelineModel from {model_uri}")
    load_started = time.monotonic()
    model = PipelineModel.load(model_uri)
    load_duration = time.monotonic() - load_started
    print(f"  model loaded once at startup in {load_duration:.2f}s")

    manifest = load_manifest(spark, model_uri)
    policy = NeutralPolicy(
        strategy=manifest["neutral_strategy"], threshold=manifest["neutral_threshold"]
    )
    print(f"  neutral strategy: {policy.strategy}")
    print(f"  emoji strategy:   {manifest['emoji_strategy']}")

    # Step 3: the run document.
    client = MongoClient(settings.mongo_uri)
    run = Run(
        _id=run_id,
        started_at=datetime.now(timezone.utc),
        ended_at=None,
        model_id=None,
        input_source=args.source,
        replay_rate_per_sec=args.rate,
        spark_config=SparkConfig(
            master=settings.spark_master,
            trigger=f"{args.trigger} seconds",
            window=f"{args.window} minute",
            watermark=f"{args.watermark} minutes",
        ),
        storage_backend=hdfs.backend(),
    )
    client[DB_NAME][COLL_RUNS].replace_one(
        {"_id": run_id}, run.model_dump(by_alias=True), upsert=True
    )
    print(f"  run {run_id} recorded")

    # Step 4.
    records = read_source(spark, args)

    # Step 5: the same preprocessing module the batch job used.
    records = preprocess.prepare(records, emoji_strategy=manifest["emoji_strategy"])

    # Step 6. `probability` is an MLlib Vector, whose Catalyst representation
    # is a struct -- array functions cannot read it directly, so it is
    # converted to an array first.
    scored = model.transform(records)
    scored = scored.withColumn("prediction_label", index_to_label(F.col("prediction")))
    scored = scored.withColumn(
        "confidence", F.array_max(vector_to_array(F.col("probability")))
    )

    # Step 7.
    scored = apply_neutral(scored, policy)

    # Steps 8 and 9: the watermark bounds the deduplication state, so a long
    # run cannot grow it without limit, and records arriving after it are
    # dropped by Spark and counted via numRowsDroppedByWatermark.
    scored = scored.withWatermark("event_time", f"{args.watermark} minutes")
    scored = scored.dropDuplicatesWithinWatermark([preprocess.DEDUP_KEY])

    scored = scored.select(
        "text",
        preprocess.TEXT_CLEAN,
        preprocess.HASHTAGS,
        preprocess.DEDUP_KEY,
        "prediction_label",
        "confidence",
        "event_time",
        "ingest_time",
    )

    # Steps 10 and 11.
    writer = BatchWriter(run_id, args.window)
    listener = ProgressListener(run_id, args.source, late=writer.late)
    spark.streams.addListener(listener)

    checkpoint = hdfs.checkpoint_uri(run_id)
    print(f"\ncheckpoint: {checkpoint}")
    print(f"window {args.window}min | watermark {args.watermark}min | "
          f"trigger {args.trigger}s\n")

    query = (
        scored.writeStream.outputMode("append")
        .foreachBatch(writer)
        .option("checkpointLocation", checkpoint)  # Step 12.
        .trigger(processingTime=f"{args.trigger} seconds")
        .start()
    )
    # The late filter reads the watermark Spark reported at the end of the
    # previous batch -- the one in force for the batch now being written.
    writer.late.watermark_source = lambda: next_batch_watermark(
        query.lastProgress, args.watermark
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nstopping stream...")
        query.stop()
    finally:
        client[DB_NAME][COLL_RUNS].update_one(
            {"_id": run_id}, {"$set": {"ended_at": datetime.now(timezone.utc)}}
        )
        spark.streams.removeListener(listener)
        spark.stop()

    print(f"run {run_id} ended")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

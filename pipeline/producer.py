"""C13/C14/C15: replay the cleaned corpus as a controlled stream.

Reads `/sentiment/clean` and emits newline-delimited JSON to either a TCP
socket on 9999 or a Kafka topic.

Three properties matter here:

*Deterministic* (C15) -- `--seed` fixes the record order, so two runs emit the
same records in the same order and `make evidence-reproducible` can compare
checksums. Reproducibility is a claim in the report, so it has to be a
property of the code rather than a hope.

*Rate-limited by token bucket*, not `sleep()` per record. Sleeping per record
caps throughput at roughly 1/resolution of the system clock -- around 60/s on
Windows -- which would make the configured rate a fiction above that.

*Honest about the achieved rate* -- the actual rate is logged every 10s, so an
operator can see when the machine, not the setting, is the bottleneck.
"""

from __future__ import annotations

import argparse
import json
import random
import socket
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from pipeline.config import DEFAULT_REPLAY_RATE, DEFAULT_SEED, KAFKA_TOPIC, SOCKET_PORT, settings
from storage import hdfs

REPORT_INTERVAL_SECONDS = 10


class TokenBucket:
    """Classic token bucket: capacity of one second's worth of records.

    Bursts up to one second, then settles to the configured rate. Unlike a
    per-record sleep, it stays accurate at thousands of records per second.
    """

    def __init__(self, rate: float, capacity: float | None = None) -> None:
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        self._tokens = self.capacity
        self._last = time.monotonic()

    def take(self, tokens: float = 1.0) -> None:
        """Block until `tokens` are available."""
        while True:
            now = time.monotonic()
            self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
            self._last = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return
            time.sleep(max((tokens - self._tokens) / self.rate, 0.001))


def load_records(limit: int | None, seed: int) -> list[dict]:
    """Read the cleaned corpus into a deterministically ordered list.

    Collecting to the driver is acceptable and intended here: the producer is
    a replay harness, not a distributed job, and the default cap keeps the
    footprint small on a 16 GB machine.
    """
    from scripts.spark_session import build

    if not hdfs.exists(hdfs.CLEAN):
        raise FileNotFoundError(
            f"{hdfs.CLEAN} does not exist -- run `make clean-batch` first"
        )

    spark = build("producer-load", mongo=False)
    frame = spark.read.parquet(hdfs.uri(hdfs.CLEAN)).select(
        "text", "text_clean", "hashtags", "dedup_key", "user", "label", "date"
    )
    # Order by dedup_key before sampling: Parquet file order is not guaranteed
    # stable across runs, and C15 needs it to be.
    frame = frame.orderBy("dedup_key")
    if limit:
        frame = frame.limit(limit)
    rows = [row.asDict() for row in frame.collect()]
    spark.stop()

    random.Random(seed).shuffle(rows)
    return rows


def to_payload(record: dict, event_time: datetime) -> str:
    """One newline-delimited JSON message.

    The producer injects `event_time` so the stream has a real event-time
    column to window on. The socket source gives only an ingestion timestamp,
    which would make windowing a measure of the replay rather than of the data.
    """
    return json.dumps(
        {
            "text": record.get("text") or "",
            "user": record.get("user") or "",
            "label": record.get("label"),
            "event_time": event_time.isoformat(),
            "dedup_key": record.get("dedup_key"),
        },
        ensure_ascii=False,
    )


class RateReporter:
    """Logs the achieved rate so the configured rate is never taken on trust."""

    def __init__(self, target_rate: int) -> None:
        self.target_rate = target_rate
        self.started = time.monotonic()
        self._last_report = self.started
        self._last_count = 0

    def tick(self, total_sent: int) -> None:
        now = time.monotonic()
        if now - self._last_report < REPORT_INTERVAL_SECONDS:
            return
        window = now - self._last_report
        achieved = (total_sent - self._last_count) / window
        drift = achieved / self.target_rate if self.target_rate else 1.0
        note = "" if drift > 0.9 else "   <- machine is the bottleneck"
        print(
            f"  {total_sent:>9,} sent | {achieved:8.1f} rec/s "
            f"(target {self.target_rate}){note}",
            flush=True,
        )
        self._last_report = now
        self._last_count = total_sent


def run_socket(records: list[dict], args) -> int:
    """Serve one connection on 9999 and stream into it (C14).

    Spark's socket source connects as a client, so the producer listens. The
    replay starts only once Spark attaches -- otherwise the first thousands of
    records would be emitted into a socket nobody is reading.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", args.port))
    server.listen(1)
    print(f"listening on port {args.port}; waiting for the Spark job to connect...")

    connection, address = server.accept()
    print(f"connected from {address[0]}:{address[1]} -- starting replay\n")

    bucket = TokenBucket(args.rate)
    reporter = RateReporter(args.rate)
    sent = 0
    try:
        with connection:
            for record, event_time in _iterate(records, args):
                bucket.take()
                connection.sendall((to_payload(record, event_time) + "\n").encode("utf-8"))
                sent += 1
                reporter.tick(sent)
    except (BrokenPipeError, ConnectionResetError):
        print(f"\nconsumer disconnected after {sent:,} records")
    except KeyboardInterrupt:
        print(f"\ninterrupted after {sent:,} records")
    finally:
        server.close()

    _summarise(sent, reporter)
    return 0


def run_kafka(records: list[dict], args) -> int:
    """Publish continuously to the Kafka topic (C13)."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        print(
            "kafka-python is not installed. Add it with:\n"
            "  .venv/Scripts/pip.exe install kafka-python-ng==2.2.3",
            file=sys.stderr,
        )
        return 1

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda value: value.encode("utf-8"),
        linger_ms=50,
        acks=1,
    )
    print(f"publishing to {args.topic} at {settings.kafka_bootstrap}\n")

    bucket = TokenBucket(args.rate)
    reporter = RateReporter(args.rate)
    sent = 0
    try:
        for record, event_time in _iterate(records, args):
            bucket.take()
            # The message timestamp is what the Spark Kafka source exposes, so
            # the injected event_time travels in the value as well.
            producer.send(
                args.topic,
                value=to_payload(record, event_time),
                timestamp_ms=int(event_time.timestamp() * 1000),
            )
            sent += 1
            reporter.tick(sent)
    except KeyboardInterrupt:
        print(f"\ninterrupted after {sent:,} records")
    finally:
        producer.flush()
        producer.close()

    _summarise(sent, reporter)
    return 0


def _iterate(records: list[dict], args):
    """Yield (record, event_time) pairs, looping if asked.

    Event time is wall-clock now, optionally compressed: `--time-compression
    60` advances event time 60x faster than real time, so a one-minute window
    fills in one second and the demo shows several windows within a viva.
    """
    emitted = 0
    start_wall = datetime.now(timezone.utc)
    start_monotonic = time.monotonic()

    while True:
        for record in records:
            if args.limit and emitted >= args.limit:
                return
            elapsed = time.monotonic() - start_monotonic
            event_time = start_wall + timedelta(seconds=elapsed * args.time_compression)
            emitted += 1
            yield record, event_time
        if not args.loop:
            return
        print(f"  corpus exhausted at {emitted:,} records -- looping", flush=True)


def _summarise(sent: int, reporter: RateReporter) -> None:
    elapsed = time.monotonic() - reporter.started
    rate = sent / elapsed if elapsed else 0.0
    print(f"\nsent {sent:,} records in {elapsed:.1f}s ({rate:.1f} rec/s average)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sink", choices=["socket", "kafka"], default="socket")
    parser.add_argument("--rate", type=int, default=DEFAULT_REPLAY_RATE,
                        help="records per second")
    parser.add_argument("--limit", type=int, default=None, help="stop after N records")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help="deterministic ordering (C15)")
    parser.add_argument("--loop", action="store_true",
                        help="restart from the beginning when exhausted")
    parser.add_argument(
        "--time-compression", type=float, default=1.0,
        help="event time advances this many times faster than wall clock. "
             "Default 1: event time is the moment of emission. Values above 1 "
             "push event time into the future, and a restarted replay then "
             "writes into windows an earlier run already filled.",
    )
    parser.add_argument("--port", type=int, default=SOCKET_PORT)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--pool", type=int, default=50_000,
                        help="records to load into memory for replay")
    args = parser.parse_args()

    print(f"loading up to {args.pool:,} records (seed {args.seed})...")
    try:
        records = load_records(args.pool, args.seed)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if not records:
        print("the cleaned corpus is empty", file=sys.stderr)
        return 1

    print(f"loaded {len(records):,} records")
    print(f"  rate              {args.rate}/s")
    print(f"  time compression  {args.time_compression}x")
    print(f"  loop              {args.loop}")

    if args.sink == "socket":
        return run_socket(records, args)
    return run_kafka(records, args)


if __name__ == "__main__":
    raise SystemExit(main())

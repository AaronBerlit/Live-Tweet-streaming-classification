"""`make demo-socket` / `make demo-kafka` -- run the producer and the stream
together, and shut both down cleanly on Ctrl-C.

Startup order matters on the socket path. Spark's socket source connects as a
*client*, so the producer must already be listening; starting them the other
way round gives "connection refused" and an empty stream that looks like a
Spark problem.

On the Kafka path the order is irrelevant -- the broker decouples them -- but
the topic is created first so the consumer does not sit on an unknown topic.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time

from pipeline.config import (
    DEFAULT_REPLAY_RATE,
    DEFAULT_TRIGGER_SECONDS,
    DEFAULT_WATERMARK_MINUTES,
    DEFAULT_WINDOW_MINUTES,
    KAFKA_TOPIC,
    REPO_ROOT,
    SOCKET_PORT,
)
from storage.hdfs import docker_binary

PYTHON = sys.executable


def _spawn(
    module: str, arguments: list[str], label: str, *, capture: bool = False
) -> subprocess.Popen:
    command = [PYTHON, "-u", "-m", module, *arguments]
    print(f"  starting {label}: {' '.join(command[3:])}", flush=True)
    creationflags = 0
    if os.name == "nt":
        # Give each child its own process group so Ctrl-C in this console does
        # not race both of them into a half-stopped state.
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    environment = dict(os.environ)
    if module == "pipeline.producer":
        # The producer's Spark session only collects its replay pool. On the
        # Kafka path it overlaps with the stream job's session, and two 4 GB
        # drivers do not fit in WSL's memory; 1 GB is ample for the producer.
        environment["SPARK_DRIVER_MEMORY"] = "1g"
    return subprocess.Popen(
        command,
        cwd=str(REPO_ROOT),
        env=environment,
        creationflags=creationflags,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        text=capture,
        bufsize=1 if capture else -1,
    )


def _wait_until_listening(producer: subprocess.Popen, timeout: float = 300) -> bool:
    """Block until the producer reports it is listening, echoing its output.

    The producer loads its replay pool through Spark before it opens the
    socket, which takes tens of seconds. Spark's socket source fails at once
    if nobody is listening, and the port cannot be probed either: the producer
    accepts exactly one connection, so a probe would steal the stream's slot.
    Its own "listening on port" line is the only safe signal.
    """
    deadline = time.monotonic() + timeout
    assert producer.stdout is not None
    for line in producer.stdout:
        print(f"  [producer] {line.rstrip()}", flush=True)
        if "listening on port" in line:
            # Keep echoing the rest of its output in the background.
            threading.Thread(
                target=lambda: [print(f"  [producer] {rest.rstrip()}", flush=True)
                                for rest in producer.stdout],  # type: ignore[union-attr]
                daemon=True,
            ).start()
            return True
        if time.monotonic() > deadline:
            return False
    return False


def _terminate(process: subprocess.Popen, label: str) -> None:
    if process.poll() is not None:
        return
    print(f"  stopping {label}...", flush=True)
    # SIGINT first: the stream job handles KeyboardInterrupt by stopping the
    # query and stamping `runs.ended_at`. SIGTERM would kill Python before that
    # cleanup ran, leaving the run looking like it was still going.
    try:
        if os.name != "nt":
            process.send_signal(signal.SIGINT)
            process.wait(timeout=30)
            return
    except subprocess.TimeoutExpired:
        pass
    try:
        process.terminate()
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()


def _ensure_topic(topic: str) -> None:
    """Create the Kafka topic if the broker does not have it yet."""
    command = [
        docker_binary(), "exec", "sentiment-kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "localhost:9092",
        "--create", "--if-not-exists",
        "--topic", topic,
        "--partitions", "3",
        "--replication-factor", "1",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  topic {topic} ready")
    else:
        print(f"  could not pre-create {topic}: {result.stderr.strip()}")
        print("  continuing -- Kafka may auto-create it")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["socket", "kafka"], default="socket")
    parser.add_argument("--model", default="nb")
    parser.add_argument("--rate", type=int, default=DEFAULT_REPLAY_RATE)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--loop", action="store_true", default=True)
    parser.add_argument("--no-loop", dest="loop", action="store_false")
    parser.add_argument("--trigger", type=int, default=DEFAULT_TRIGGER_SECONDS)
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW_MINUTES)
    parser.add_argument("--watermark", type=int, default=DEFAULT_WATERMARK_MINUTES)
    parser.add_argument(
        "--time-compression",
        type=float,
        default=1.0,
        help="event-time speed-up; 1 = event time is the moment of emission "
             "(see pipeline/producer.py for why larger values are unsafe)",
    )
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--port", type=int, default=SOCKET_PORT)
    args = parser.parse_args()

    producer_args = [
        "--sink", args.source,
        "--rate", str(args.rate),
        "--time-compression", str(args.time_compression),
    ]
    if args.loop:
        producer_args.append("--loop")
    if args.limit:
        producer_args += ["--limit", str(args.limit)]
    if args.source == "socket":
        producer_args += ["--port", str(args.port)]
    else:
        producer_args += ["--topic", args.topic]

    stream_args = [
        "--source", args.source,
        "--model", args.model,
        "--trigger", str(args.trigger),
        "--window", str(args.window),
        "--watermark", str(args.watermark),
        "--rate", str(args.rate),
    ]
    if args.source == "socket":
        stream_args += ["--port", str(args.port)]
    else:
        stream_args += ["--topic", args.topic]

    print(f"\n=== demo: {args.source} ===")
    if args.source == "kafka":
        _ensure_topic(args.topic)

    producer = None
    stream = None
    try:
        if args.source == "socket":
            # The producer listens; Spark connects to it. Start it first.
            producer = _spawn("pipeline.producer", producer_args, "producer", capture=True)
            if not _wait_until_listening(producer):
                print("producer never started listening", file=sys.stderr)
                return 1
            stream = _spawn("pipeline.stream_job", stream_args, "stream job")
        else:
            stream = _spawn("pipeline.stream_job", stream_args, "stream job")
            # Let the consumer establish its subscription before publishing,
            # so a `latest` offset does not skip the first records.
            time.sleep(25)
            producer = _spawn("pipeline.producer", producer_args, "producer")

        print("\nboth running. Ctrl-C to stop.")
        print("open the dashboard: http://localhost:5173 (make web)\n")

        while True:
            if producer and producer.poll() is not None:
                print(f"\nproducer exited with code {producer.returncode}")
                break
            if stream and stream.poll() is not None:
                print(f"\nstream job exited with code {stream.returncode}")
                break
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\ninterrupted")
    finally:
        if stream:
            _terminate(stream, "stream job")
        if producer:
            _terminate(producer, "producer")

    print("demo stopped")
    return 0


if __name__ == "__main__":
    # Ignore Ctrl-C in the child process groups; this process orchestrates the
    # shutdown so both children stop in the right order.
    if os.name != "nt":
        signal.signal(signal.SIGINT, signal.default_int_handler)
    raise SystemExit(main())

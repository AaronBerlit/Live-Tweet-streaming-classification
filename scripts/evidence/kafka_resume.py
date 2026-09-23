"""C13 / §12.4: stop the Kafka consumer mid-run and restart it from its
checkpoint. Nothing is lost and nothing is counted twice.

The test, end to end, against the real broker and the real streaming job:

  1. recreate the topic, so offsets start from zero and every count is exact;
  2. start the streaming job on a fixed run id (so it has one checkpoint);
  3. publish exactly N records;
  4. SIGKILL the streaming job mid-run -- a crash, not a clean stop, so the
     batch in flight is left uncommitted;
  5. let the producer finish, so a backlog accumulates in the retained log;
  6. restart the job on the same run id: Spark resumes from the checkpointed
     offsets and replays the uncommitted batch;
  7. wait until it drains the backlog.

The proof is arithmetic. The rows Spark read, summed over distinct batch ids,
must equal N exactly: fewer means the restart skipped offsets, more means it
re-read committed ones. And `windows` must still reconcile with `scored`, which
is what shows that replaying the uncommitted batch wrote nothing twice
(Rule B).
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

from pymongo import MongoClient

from pipeline.config import COLL_BATCHES, DB_NAME, KAFKA_TOPIC, REPO_ROOT, settings
from scripts.evidence._common import Evidence, repository_or_fail
from storage.hdfs import docker_binary

RECORDS = 20_000
RATE = 200
KILL_AFTER_ROWS = 6_000
LOG_DIR = REPO_ROOT / "data" / "logs"


def kafka_topics(*arguments: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [docker_binary(), "exec", "sentiment-kafka", "/opt/kafka/bin/kafka-topics.sh",
         "--bootstrap-server", "localhost:9092", *arguments],
        capture_output=True, text=True,
    )


def topic_end_offset(topic: str) -> int:
    """Total messages retained in the topic, summed over partitions."""
    result = subprocess.run(
        [docker_binary(), "exec", "sentiment-kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "localhost:9092", "--topic", topic, "--time", "-1"],
        capture_output=True, text=True,
    )
    return sum(int(line.rsplit(":", 1)[1]) for line in result.stdout.split() if line.count(":") == 2)


def start_stream(run_id: str, log_name: str) -> subprocess.Popen:
    log = open(LOG_DIR / log_name, "w", encoding="utf-8")
    return subprocess.Popen(
        [sys.executable, "-u", "-m", "pipeline.stream_job", "--source", "kafka",
         "--run-id", run_id, "--starting-offsets", "earliest", "--rate", str(RATE)],
        cwd=str(REPO_ROOT), stdout=log, stderr=subprocess.STDOUT,
    )


def rows_consumed(batches, run_id: str) -> tuple[int, int]:
    """(rows read, batches) over DISTINCT batch ids -- a replayed batch
    overwrites its own sample, so it cannot be summed twice here."""
    docs = list(batches.find({"run_id": run_id}, {"rows_in_batch": 1}))
    return sum(d["rows_in_batch"] for d in docs), len(docs)


def wait_for(predicate, timeout: float, interval: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def main() -> int:
    evidence = Evidence(
        "kafka_resume",
        "Restarting the Kafka consumer resumes from its retained offset: "
        "nothing lost, nothing counted twice",
        "KAFKA RESUME FROM CHECKPOINT",
    )
    if repository_or_fail(evidence) is None:
        return evidence.fail("MongoDB unreachable", "docker compose up -d mongo")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    batches = MongoClient(settings.mongo_uri)[DB_NAME][COLL_BATCHES]
    run_id = f"kafka_resume_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
    evidence.line(f"run id: {run_id}  |  records: {RECORDS:,} at {RATE}/s")

    # 1. A fresh topic, so offsets and counts are exact.
    kafka_topics("--delete", "--if-exists", "--topic", KAFKA_TOPIC)
    time.sleep(3)
    created = kafka_topics("--create", "--topic", KAFKA_TOPIC, "--partitions", "3",
                           "--replication-factor", "1")
    if created.returncode != 0:
        return evidence.fail(f"could not create topic: {created.stderr.strip()}")
    evidence.line(f"topic {KAFKA_TOPIC} recreated (3 partitions)")

    # 2 + 3. Stream first, then the producer -- but only once the query has
    # actually started. A fixed sleep once let the producer publish the whole
    # run before Spark (still fetching connector jars) had begun, so batch 0
    # swallowed everything and the crash landed on an idle stream.
    first_log = LOG_DIR / f"{run_id}-first.log"
    stream = start_stream(run_id, first_log.name)
    if not wait_for(lambda: "query started" in first_log.read_text(encoding="utf-8", errors="ignore"), 600):
        stream.kill()
        return evidence.fail("the streaming query never started", f"see {first_log}")
    producer_env = dict(os.environ, SPARK_DRIVER_MEMORY="1g")
    producer = subprocess.Popen(
        [sys.executable, "-u", "-m", "pipeline.producer", "--sink", "kafka",
         "--rate", str(RATE), "--limit", str(RECORDS), "--pool", str(RECORDS)],
        cwd=str(REPO_ROOT), env=producer_env,
        stdout=open(LOG_DIR / f"{run_id}-producer.log", "w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )

    # 4. Crash the stream mid-run.
    if not wait_for(lambda: rows_consumed(batches, run_id)[0] >= KILL_AFTER_ROWS, 600):
        stream.kill(); producer.kill()
        return evidence.fail("the stream never reached the kill point")
    before_rows, before_batches = rows_consumed(batches, run_id)
    stream.send_signal(signal.SIGKILL)
    stream.wait()
    evidence.line(f"SIGKILL after {before_rows:,} rows in {before_batches} batches")

    # 5. Let the backlog build in the retained log.
    producer.wait(timeout=900)
    published = topic_end_offset(KAFKA_TOPIC)
    evidence.line(f"producer finished; topic holds {published:,} messages")

    # 6 + 7. Restart on the same checkpoint and drain.
    restarted = start_stream(run_id, f"{run_id}-restart.log")
    wait_for(lambda: rows_consumed(batches, run_id)[0] >= published, 900)
    time.sleep(12)  # one more trigger, to catch any over-read
    total_rows, total_batches = rows_consumed(batches, run_id)
    restarted.send_signal(signal.SIGINT)
    try:
        restarted.wait(timeout=60)
    except subprocess.TimeoutExpired:
        restarted.kill()

    from api.repository import Repository

    reconciliation = Repository().reconciliation()

    evidence.line("")
    evidence.table(
        ["check", "value"],
        [
            ["messages published to the topic", f"{published:,}"],
            ["rows read by Spark, over distinct batch ids", f"{total_rows:,}"],
            ["batches", str(total_batches)],
            ["rows read before the crash", f"{before_rows:,}"],
            ["rows read after the restart", f"{total_rows - before_rows:,}"],
            ["windows sum == scored (before watermark)",
             f"{reconciliation['windows_total']:,} == {reconciliation['scored_total']:,}"],
        ],
    )
    evidence.line("")

    lost = published - total_rows
    after_restart = total_rows - before_rows
    if before_rows >= published or after_restart <= 0:
        # The crash must interrupt a stream with work left, or the restart
        # proves nothing about resuming. Do not report a pass that wasn't earned.
        evidence.line(
            "VERDICT: INCONCLUSIVE -- the crash did not leave a backlog, so the "
            "resume path was not exercised. Re-run."
        )
        evidence.write()
        return 1
    if lost == 0 and reconciliation["consistent"]:
        evidence.line(
            "VERDICT: every published message was read exactly once across the "
            "crash and restart, and the replayed batch wrote nothing twice. C13 holds."
        )
        return evidence.done()
    if lost > 0:
        evidence.line(f"VERDICT: {lost:,} messages were never read -- the restart skipped offsets.")
    elif lost < 0:
        evidence.line(f"VERDICT: {-lost:,} messages were read twice -- committed offsets were re-read.")
    if not reconciliation["consistent"]:
        evidence.line("VERDICT: windows and scored disagree after the restart.")
    evidence.write()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

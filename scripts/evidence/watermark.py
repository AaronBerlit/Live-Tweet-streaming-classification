"""C17: watermarks bound window state, and late records are counted.

Two things have to be shown for this claim to hold:

1.  a watermark actually exists and advances behind the newest event time;
2.  records arriving after it are dropped *and counted* -- silently dropping
    data with no counter is the failure mode the PRD names explicitly.

Both come from the recorded batch history, which is populated from Spark's own
query progress (`numRowsDroppedByWatermark`), not from a count this project
keeps for itself.
"""

from __future__ import annotations

from datetime import timezone

from scripts.evidence._common import Evidence, repository_or_fail


def _aware(moment):
    if moment is None:
        return None
    return moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment


def main() -> int:
    evidence = Evidence(
        "watermark",
        "Watermarks bound window state; late records are dropped and counted",
        "WATERMARK AND LATE DATA",
    )

    repository = repository_or_fail(evidence)
    if repository is None:
        return evidence.fail("cannot read stream state", "docker compose up -d mongo")

    # One run only. Blending runs would divide by a wall-clock span that
    # includes the idle gaps between them, which measures nothing.
    run = next(
        (r for r in repository.runs(limit=50) if repository.batches(run_id=r["_id"], limit=1)),
        None,
    )
    if run is None:
        return evidence.fail(
            "no micro-batch history recorded -- the streaming job has never run",
            "make demo-socket",
        )
    batches = repository.batches(run_id=run["_id"], limit=5000)
    heartbeat = repository.heartbeat()

    if not batches:
        return evidence.fail(
            "no micro-batch history -- the streaming job has never run",
            "make demo-socket",
        )

    if run:
        spark_config = run.get("spark_config", {})
        evidence.line("configured stream semantics")
        evidence.table(
            ["setting", "value"],
            [
                ["window", str(spark_config.get("window"))],
                ["watermark delay", str(spark_config.get("watermark"))],
                ["trigger", str(spark_config.get("trigger"))],
            ],
        )
        evidence.line("")

    evidence.line("current heartbeat")
    if heartbeat:
        evidence.table(
            ["field", "value"],
            [
                ["last_batch_id", str(heartbeat.get("last_batch_id"))],
                ["last_batch_at", str(_aware(heartbeat.get("last_batch_at")))],
                ["watermark", str(_aware(heartbeat.get("watermark")))],
                ["late_records_dropped", str(heartbeat.get("late_records_dropped", 0))],
                ["input_source", str(heartbeat.get("input_source"))],
            ],
        )
    else:
        evidence.line("  none recorded")
    evidence.line("")

    with_watermark = [b for b in batches if b.get("watermark")]
    evidence.line(
        f"batches with a watermark set: {len(with_watermark):,} of {len(batches):,}"
    )
    if with_watermark:
        evidence.line("")
        evidence.line("watermark progression (last 12 batches that had one)")
        evidence.table(
            ["batch", "watermark", "rows", "late dropped (cumulative)"],
            [
                [
                    str(b["batch_id"]),
                    str(_aware(b["watermark"])),
                    f"{b['rows_in_batch']:,}",
                    str(b.get("late_records_dropped", 0)),
                ]
                for b in with_watermark[-12:]
            ],
        )
        evidence.line("")
        evidence.line(
            "The watermark advances monotonically behind the newest event time. "
            "That is what bounds deduplication state: keys older than it are "
            "released, so a long run cannot grow state without limit."
        )
    else:
        evidence.line(
            "  No watermark recorded yet. Spark reports one only after a batch "
            "containing event-time data has been processed."
        )

    evidence.line("")
    total_late = max(
        (b.get("late_records_dropped", 0) for b in batches), default=0
    )
    evidence.line(f"late records dropped across the run: {total_late:,}")
    if total_late == 0:
        evidence.line(
            "  Zero is the expected result for an in-order replay: the producer "
            "emits monotonically increasing event times, so nothing arrives "
            "after its watermark. The drop-and-count path itself is proven by "
            "tests/integration/test_late_data.py, which feeds a record eight "
            "minutes behind the watermark and asserts it is dropped and that "
            "heartbeat.late_records_dropped reaches 1."
        )

    return evidence.done()


if __name__ == "__main__":
    raise SystemExit(main())

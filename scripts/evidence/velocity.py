"""Three V's -- Velocity, and C22/C23.

Prints observed rows/sec and micro-batch durations from the recorded batch
history. Every figure is a measurement Spark reported during a real run; if no
run has happened, this script says so and exits non-zero rather than printing
a plausible-looking number.
"""

from __future__ import annotations

from datetime import timezone

from scripts.evidence._common import Evidence, repository_or_fail


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile. Small samples make interpolation misleading."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round(fraction * len(ordered))) - 1))
    return ordered[index]


def main() -> int:
    evidence = Evidence(
        "velocity",
        "The corpus is replayed at a controlled rate and processed in "
        "micro-batches; throughput and latency are measured",
        "VELOCITY",
    )

    repository = repository_or_fail(evidence)
    if repository is None:
        return evidence.fail("cannot read batch history", "docker compose up -d mongo")

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

    if run:
        evidence.line("most recent run")
        evidence.table(
            ["setting", "value"],
            [
                ["run_id", str(run.get("_id"))],
                ["input source", str(run.get("input_source"))],
                ["configured replay rate", f"{run.get('replay_rate_per_sec')}/s"],
                ["spark master", str(run.get("spark_config", {}).get("master"))],
                ["trigger", str(run.get("spark_config", {}).get("trigger"))],
                ["window", str(run.get("spark_config", {}).get("window"))],
                ["watermark", str(run.get("spark_config", {}).get("watermark"))],
                ["storage backend", str(run.get("storage_backend"))],
            ],
        )
        evidence.line("")

    durations = [float(b["batch_duration_ms"]) for b in batches]
    rates = [float(b["rows_per_sec"]) for b in batches if b["rows_in_batch"]]
    rows = [int(b["rows_in_batch"]) for b in batches]
    total_rows = sum(rows)

    first_at = batches[0]["batch_at"]
    last_at = batches[-1]["batch_at"]
    if first_at.tzinfo is None:
        first_at = first_at.replace(tzinfo=timezone.utc)
    if last_at.tzinfo is None:
        last_at = last_at.replace(tzinfo=timezone.utc)
    span_seconds = max((last_at - first_at).total_seconds(), 1e-9)

    evidence.line(f"micro-batches recorded: {len(batches):,}")
    evidence.line(f"records processed:      {total_rows:,}")
    evidence.line(f"wall-clock span:        {span_seconds:,.1f}s")
    evidence.line("")

    evidence.line("throughput (records/second, as reported by Spark)")
    evidence.table(
        ["statistic", "value"],
        [
            ["mean of per-batch rates", f"{(sum(rates) / len(rates)) if rates else 0:,.1f}"],
            ["p50", f"{percentile(rates, 0.50):,.1f}"],
            ["p95", f"{percentile(rates, 0.95):,.1f}"],
            ["max", f"{max(rates) if rates else 0:,.1f}"],
            ["overall (rows / wall-clock span)", f"{total_rows / span_seconds:,.1f}"],
        ],
    )
    evidence.line("")

    evidence.line("micro-batch duration (ms, Spark triggerExecution)")
    evidence.table(
        ["statistic", "value"],
        [
            ["mean", f"{sum(durations) / len(durations):,.1f}"],
            ["p50", f"{percentile(durations, 0.50):,.1f}"],
            ["p95", f"{percentile(durations, 0.95):,.1f}"],
            ["max", f"{max(durations):,.1f}"],
        ],
    )
    evidence.line("")

    evidence.line("last 10 batches")
    evidence.table(
        ["batch", "rows", "duration ms", "rows/sec", "late dropped"],
        [
            [
                str(b["batch_id"]),
                f"{b['rows_in_batch']:,}",
                f"{b['batch_duration_ms']:,}",
                f"{b['rows_per_sec']:,.1f}",
                str(b.get("late_records_dropped", 0)),
            ]
            for b in batches[-10:]
        ],
    )
    evidence.line("")
    evidence.line(
        "These are single-machine numbers: Spark on local[2] sharing a laptop "
        "with the producer, MongoDB, Kafka and HDFS. They are honest "
        "measurements of this deployment, not a claim about cluster capacity. "
        "See docs/LIMITATIONS.md."
    )

    return evidence.done()


if __name__ == "__main__":
    raise SystemExit(main())

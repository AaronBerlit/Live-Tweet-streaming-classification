"""C5: prove Spark 3.x is installed and working, with a word-count job.

This is the PRD's named acceptance command for Phase 0. It does not touch
Mongo, Kafka or HDFS -- if this fails, nothing else can work, and the failure
should be unambiguous about which layer broke.
"""

from __future__ import annotations

import sys
import time

from pipeline import versions


def main() -> int:
    print("--- pinned stack ---")
    for key, value in versions.version_report().items():
        print(f"  {key:24} {value}")

    try:
        versions.assert_versions()
    except versions.VersionMismatch as exc:
        print(str(exc), file=sys.stderr)
        return 1

    from scripts.spark_session import build

    print("\n--- starting SparkSession ---")
    started = time.monotonic()
    # No connectors: this check must not depend on a Maven download.
    spark = build("verify-spark", kafka=False, mongo=False)
    print(f"  session up in {time.monotonic() - started:.1f}s")
    print(f"  spark.version   {spark.version}")
    print(f"  master          {spark.sparkContext.master}")

    print("\n--- word count ---")
    text = [
        "the pipeline is the point not the classifier",
        "windows watermarks and idempotent writes",
        "the pipeline replays the corpus",
    ]
    counts = (
        spark.sparkContext.parallelize(text)
        .flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: (-kv[1], kv[0]))
        .take(5)
    )
    for word, count in counts:
        print(f"  {count:3d}  {word}")

    # Compare against the same count done in plain Python, so the check
    # verifies Spark rather than asserting a number someone typed by hand.
    from collections import Counter

    # Sort every word before truncating: taking the top 5 first would break
    # ties by insertion order and disagree with Spark's (-count, word) sort.
    tally = Counter(word for line in text for word in line.split())
    expected = sorted(tally.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    if counts != expected:
        print(
            f"\nword count disagrees with the local computation:\n"
            f"  spark  {counts}\n"
            f"  python {expected}",
            file=sys.stderr,
        )
        spark.stop()
        return 1

    spark.stop()
    print("\nC5 verified: Spark is installed and executing jobs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

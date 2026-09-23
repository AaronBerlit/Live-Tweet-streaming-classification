"""C15: replay is reproducible -- same seed, same records, same order.

Runs the producer's record-selection twice with the same seed and compares
SHA-256 checksums of the emitted payloads, then runs it once with a different
seed to show the checksum actually depends on the seed. A test that only
checked equality would pass even if the seed were ignored entirely.

This exercises the real selection path (`producer.load_records`), not a
reimplementation of it.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from pipeline import producer
from scripts.evidence._common import Evidence
from storage import hdfs

SAMPLE = 5_000
FIXED_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def checksum(records: list[dict]) -> str:
    """SHA-256 over the payloads the producer would emit, in order.

    Event time is pinned to a constant so the digest reflects record identity
    and ordering only -- otherwise wall-clock would make every run differ and
    the check would be meaningless.
    """
    digest = hashlib.sha256()
    for record in records:
        digest.update(producer.to_payload(record, FIXED_TIME).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def main() -> int:
    evidence = Evidence(
        "reproducible",
        "Replay is reproducible -- same records, same order, for a given seed",
        "REPRODUCIBLE REPLAY",
    )

    if not hdfs.exists(hdfs.CLEAN):
        return evidence.fail(f"{hdfs.CLEAN} does not exist", "make clean-batch")

    evidence.line(f"sample size: {SAMPLE:,} records")
    evidence.line("")

    evidence.line("run 1 (seed 42)")
    first = producer.load_records(SAMPLE, 42)
    first_sum = checksum(first)
    evidence.line(f"  records: {len(first):,}")
    evidence.line(f"  sha256:  {first_sum}")

    evidence.line("")
    evidence.line("run 2 (seed 42, same parameters)")
    second = producer.load_records(SAMPLE, 42)
    second_sum = checksum(second)
    evidence.line(f"  records: {len(second):,}")
    evidence.line(f"  sha256:  {second_sum}")

    evidence.line("")
    evidence.line("run 3 (seed 1337, control)")
    third = producer.load_records(SAMPLE, 1337)
    third_sum = checksum(third)
    evidence.line(f"  records: {len(third):,}")
    evidence.line(f"  sha256:  {third_sum}")

    evidence.line("")
    identical = first_sum == second_sum
    seed_matters = first_sum != third_sum

    evidence.table(
        ["check", "result"],
        [
            ["run 1 == run 2 (same seed)", "PASS" if identical else "FAIL"],
            ["run 1 != run 3 (different seed)", "PASS" if seed_matters else "FAIL"],
        ],
    )
    evidence.line("")

    if identical and seed_matters:
        evidence.line(
            "VERDICT: replay is deterministic for a given seed, and the seed "
            "genuinely determines the ordering. C15 holds."
        )
        return evidence.done()

    if not identical:
        evidence.line(
            "VERDICT: two runs with the same seed produced different output. "
            "C15 does NOT hold -- the ordering depends on something other than "
            "the seed (Parquet file order is the usual culprit)."
        )
    else:
        evidence.line(
            "VERDICT: changing the seed did not change the output, so the seed "
            "is not actually driving the shuffle. C15 is not demonstrated."
        )
    evidence.write()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

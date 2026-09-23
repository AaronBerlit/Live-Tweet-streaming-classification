"""PRD 12.5: data correctness of the serving layer.

The streaming job derives `windows` from `scored`, so the two must agree
exactly: the sum of window counts equals the number of scored records, no
window is stored with a count of zero, and every scored record's run exists.
A disagreement means a write was lost or doubled, and this script exits
non-zero so it cannot pass quietly.
"""

from __future__ import annotations

from scripts.evidence._common import Evidence, repository_or_fail


def main() -> int:
    evidence = Evidence(
        "reconciliation",
        "windows.count sums to the scored record count; no zero windows; no orphan runs",
        "DATA RECONCILIATION",
    )
    repository = repository_or_fail(evidence)
    if repository is None:
        return evidence.fail("cannot read the serving layer", "docker compose up -d mongo")

    check = repository.reconciliation()
    if check["windows_total"] == 0 and check["scored_total"] == 0:
        return evidence.fail("no Spark-written data yet", "make demo-socket")

    evidence.table(
        ["check", "value"],
        [
            [f"sum of windows.count (last {check['range_hours']}h)", f"{check['windows_total']:,}"],
            [f"scored documents (last {check['range_hours']}h)", f"{check['scored_total']:,}"],
            ["difference", f"{check['difference']:+,}"],
            ["windows stored with count <= 0", str(check["zero_count_windows"])],
            ["scored run_ids missing from runs", ", ".join(check["orphan_run_ids"]) or "none"],
        ],
    )
    evidence.line("")
    if check["consistent"]:
        evidence.line("VERDICT: consistent. Every window count is backed by scored records.")
        return evidence.done()
    evidence.line("VERDICT: INCONSISTENT. Writes were lost or doubled.")
    evidence.write()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

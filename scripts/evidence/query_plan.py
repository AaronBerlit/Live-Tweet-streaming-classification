"""C20: the dashboard reads small aggregated documents through an index.

Runs `explain("executionStats")` on the exact query `/api/windows` issues and
prints the winning plan. An IXSCAN with `docsExamined` close to `nReturned` is
the evidence; a COLLSCAN would disprove the claim, and this script says so
rather than quietly printing the plan either way.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from scripts.evidence._common import Evidence, repository_or_fail


def _stage_names(stage: dict) -> list[str]:
    """Flatten a plan tree into the list of stage names it contains."""
    names = []
    while isinstance(stage, dict):
        if "stage" in stage:
            names.append(stage["stage"])
        stage = stage.get("inputStage") or (stage.get("inputStages") or [None])[0]
    return names


def main() -> int:
    evidence = Evidence(
        "query_plan",
        "The dashboard reads small aggregated documents via an index",
        "QUERY PLAN",
    )

    repository = repository_or_fail(evidence)
    if repository is None:
        return evidence.fail("cannot run explain()", "docker compose up -d mongo")

    start = datetime.now(timezone.utc) - timedelta(hours=6)
    evidence.line("query under test (the one /api/windows issues)")
    evidence.line(f"  collection: windows")
    evidence.line(f"  filter:     {{window_start: {{$gte: {start.isoformat()}}}}}")
    evidence.line("  sort:       {window_start: -1, prediction: 1}")
    evidence.line("")

    plan = repository.windows_explain(start=start)
    query_planner = plan.get("queryPlanner", {})
    winning = query_planner.get("winningPlan", {})
    execution = plan.get("executionStats", {})

    stages = _stage_names(winning)
    evidence.line("winning plan stages")
    for name in stages:
        evidence.line(f"  {name}")
    evidence.line("")

    evidence.line("execution statistics")
    evidence.table(
        ["metric", "value"],
        [
            ["documents returned", f"{execution.get('nReturned', 0):,}"],
            ["documents examined", f"{execution.get('totalDocsExamined', 0):,}"],
            ["index keys examined", f"{execution.get('totalKeysExamined', 0):,}"],
            ["execution time (ms)", str(execution.get("executionTimeMillis", "?"))],
        ],
    )
    evidence.line("")

    evidence.line("indexes present on `windows`")
    try:
        indexes = repository._db["windows"].index_information()  # noqa: SLF001
        evidence.table(
            ["index", "key"],
            [[name, str(spec.get("key"))] for name, spec in indexes.items()],
        )
    except Exception as exc:  # noqa: BLE001
        evidence.line(f"  could not read index information: {exc}")
    evidence.line("")

    uses_index = "IXSCAN" in stages
    examined = execution.get("totalDocsExamined", 0)
    returned = execution.get("nReturned", 0)

    if uses_index:
        evidence.line("VERDICT: the query uses an index scan (IXSCAN). C20 holds.")
        if returned and examined > returned * 2:
            evidence.line(
                f"  note: {examined:,} documents examined for {returned:,} returned -- "
                f"the index narrows the scan but is not fully covering."
            )
    elif "COLLSCAN" in stages:
        evidence.line(
            "VERDICT: the query is a COLLECTION SCAN. C20 does NOT hold as written."
        )
        evidence.line("  fix: run scripts/init_mongo.py to create the indexes")
        evidence.write()
        return 1
    else:
        evidence.line(
            "VERDICT: inconclusive -- no IXSCAN or COLLSCAN stage in the plan. "
            "The collection may be empty; seed or run the pipeline first."
        )

    return evidence.done()


if __name__ == "__main__":
    raise SystemExit(main())

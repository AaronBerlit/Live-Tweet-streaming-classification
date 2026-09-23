"""C19: MongoDB stores aggregates, not the raw corpus.

Puts the lake's size next to MongoDB's collection sizes. The claim is only
credible if the numbers are shown together -- a serving layer holding a few
megabytes of aggregates beside a lake holding the whole corpus is the point.
"""

from __future__ import annotations

from pipeline.config import SCORED_TTL_SECONDS
from scripts.evidence._common import Evidence, format_bytes, repository_or_fail
from storage import hdfs


def main() -> int:
    evidence = Evidence(
        "storage_split",
        "MongoDB stores aggregates, not the raw corpus; `scored` is TTL-bounded",
        "STORAGE SPLIT",
    )

    repository = repository_or_fail(evidence)
    if repository is None:
        return evidence.fail("cannot read collection sizes", "docker compose up -d mongo")

    stats = repository.collection_stats()
    evidence.line("MongoDB -- serving layer")
    evidence.table(
        ["collection", "documents", "data size", "storage", "indexes", "index size"],
        [
            [
                row["collection"],
                f"{row['documents']:,}",
                format_bytes(row["size_bytes"]),
                format_bytes(row["storage_bytes"]),
                str(row["indexes"]),
                format_bytes(row["index_bytes"]),
            ]
            for row in stats
        ],
    )
    mongo_total = sum(row["size_bytes"] for row in stats)
    evidence.line("")
    evidence.line(f"MongoDB total data size: {format_bytes(mongo_total)}")
    evidence.line("")

    try:
        lake_entries = [entry for entry in hdfs.listing(hdfs.ROOT) if not entry.is_dir]
        lake_total = sum(entry.size_bytes for entry in lake_entries)
        evidence.line(f"data lake ({hdfs.backend()}) -- {len(lake_entries):,} files")
        evidence.line(f"data lake total size:    {format_bytes(lake_total)}")
        if mongo_total:
            evidence.line("")
            evidence.line(
                f"the lake holds {lake_total / mongo_total:,.1f}x "
                f"more bytes than the serving layer"
            )
    except hdfs.StorageError as exc:
        evidence.line(f"lake unavailable: {exc}")

    evidence.line("")
    evidence.line("why the split is shaped this way")
    evidence.line(
        "  windows/hashtags  small, aggregated, durable -- what the dashboard reads"
    )
    evidence.line(
        f"  scored            raw classified records, TTL "
        f"{SCORED_TTL_SECONDS // 3600}h on ingest_time"
    )
    evidence.line(
        "  the corpus itself never enters MongoDB; it stays in the lake as Parquet"
    )

    # Prove the TTL exists rather than asserting it.
    evidence.line("")
    evidence.line("TTL index on `scored` (the mitigation for uncontrolled growth)")
    try:
        indexes = repository._db["scored"].index_information()  # noqa: SLF001
        ttl_rows = [
            [name, str(spec.get("key")), f"{spec['expireAfterSeconds']}s"]
            for name, spec in indexes.items()
            if "expireAfterSeconds" in spec
        ]
        if ttl_rows:
            evidence.table(["index", "key", "expires after"], ttl_rows)
        else:
            evidence.line("  NONE FOUND -- run scripts/init_mongo.py")
    except Exception as exc:  # noqa: BLE001
        evidence.line(f"  could not read index information: {exc}")

    return evidence.done()


if __name__ == "__main__":
    raise SystemExit(main())

"""Three V's -- Volume (PRD §1.1).

Prints the lake listing and the row count actually stored, so the report can
cite a measured corpus size rather than the number on the dataset's Kaggle
page.
"""

from __future__ import annotations

from scripts.evidence._common import Evidence, format_bytes
from storage import hdfs


def main() -> int:
    evidence = Evidence(
        "volume",
        "Sentiment140 (~1.6M labelled tweets) is the dataset, stored in HDFS as Parquet",
        "VOLUME",
    )
    evidence.line(f"storage backend: {hdfs.backend()}")
    evidence.line(f"lake root:       {hdfs.describe()['root']}")
    evidence.line("")

    try:
        entries = hdfs.listing(hdfs.ROOT)
    except hdfs.StorageError as exc:
        return evidence.fail(str(exc), "make setup")

    if not entries:
        return evidence.fail(
            f"nothing stored under {hdfs.ROOT}",
            "make ingest",
        )

    rows = []
    for path in (hdfs.RAW, hdfs.CLEAN, hdfs.TRAIN, hdfs.TEST):
        files = [
            entry
            for entry in entries
            if entry.path.startswith(str(path)) and not entry.is_dir
        ]
        total = sum(entry.size_bytes for entry in files)
        rows.append([str(path), f"{len(files)}", format_bytes(total)])
    evidence.line("stored datasets")
    evidence.table(["path", "files", "size on disk"], rows)
    evidence.line("")

    # Row counts come from Spark reading the Parquet, not from a file listing.
    from scripts.spark_session import build

    spark = build("evidence-volume", mongo=False)
    try:
        counts = []
        for path in (hdfs.RAW, hdfs.CLEAN, hdfs.TRAIN, hdfs.TEST):
            if not hdfs.exists(path):
                counts.append([str(path), "not present"])
                continue
            try:
                count = spark.read.parquet(hdfs.uri(path)).count()
                counts.append([str(path), f"{count:,}"])
            except Exception as exc:  # noqa: BLE001 - report, do not crash
                counts.append([str(path), f"unreadable: {type(exc).__name__}"])
        evidence.line("row counts (read through Spark)")
        evidence.table(["path", "rows"], counts)

        if hdfs.exists(hdfs.RAW):
            evidence.line("")
            evidence.line("label distribution in /sentiment/raw")
            label_rows = [
                [str(row["label"]), f"{row['count']:,}"]
                for row in spark.read.parquet(hdfs.uri(hdfs.RAW))
                .groupBy("label")
                .count()
                .orderBy("label")
                .collect()
            ]
            evidence.table(["label", "rows"], label_rows)
    finally:
        spark.stop()

    evidence.line("")
    evidence.line(
        "Parquet is columnar and compressed; the size above is the compressed "
        "on-disk footprint, not the size of the source CSV."
    )
    return evidence.done()


if __name__ == "__main__":
    raise SystemExit(main())

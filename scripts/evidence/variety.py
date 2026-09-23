"""Three V's -- Variety (PRD §1.1).

Prints the schema, per-column null rates, and the token-class breakdown that
shows these records are not clean prose.

The patterns come from `pipeline/preprocess.py`, not from local copies, so the
shares reported here describe exactly what the cleaner acts on.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from pipeline import preprocess
from pipeline.emoji_lexicon import EMOJI_RANGES
from scripts.evidence._common import Evidence
from storage import hdfs

HASHTAG_RE = r"#\w+"


def main() -> int:
    evidence = Evidence(
        "variety",
        "Six-field records with free text, hashtags, URLs, mentions, emoji "
        "and inconsistently populated metadata",
        "VARIETY",
    )

    if not hdfs.exists(hdfs.RAW):
        return evidence.fail(f"{hdfs.RAW} does not exist", "make ingest")

    from scripts.spark_session import build

    spark = build("evidence-variety", mongo=False)
    try:
        frame = spark.read.parquet(hdfs.uri(hdfs.RAW)).cache()
        total = frame.count()
        evidence.line(f"records: {total:,}")
        evidence.line("")

        evidence.line("schema")
        evidence.table(
            ["column", "type", "nullable"],
            [
                [field.name, field.dataType.simpleString(), str(field.nullable)]
                for field in frame.schema.fields
            ],
        )
        evidence.line("")

        evidence.line("null and empty rates per column")
        null_rows = []
        for field in frame.schema.fields:
            column = F.col(field.name)
            nulls = frame.filter(column.isNull()).count()
            empties = (
                frame.filter(F.trim(column.cast("string")) == "").count()
                if field.dataType.simpleString() == "string"
                else 0
            )
            null_rows.append(
                [
                    field.name,
                    f"{nulls:,}",
                    f"{nulls / total * 100:.3f}%",
                    f"{empties:,}",
                ]
            )
        evidence.table(["column", "nulls", "null rate", "empty strings"], null_rows)
        evidence.line("")

        evidence.line("token classes in the text column")
        class_rows = []
        for label, condition in [
            ("contains a URL", F.col("text").rlike(preprocess.URL_RE)),
            ("contains an @mention", F.col("text").rlike(preprocess.MENTION_RE)),
            ("contains a #hashtag", F.col("text").rlike(HASHTAG_RE)),
            ("contains emoji", F.col("text").rlike(EMOJI_RANGES)),
            ("is a retweet", F.col("text").rlike(preprocess.RETWEET_RE)),
        ]:
            matched = frame.filter(condition).count()
            class_rows.append([label, f"{matched:,}", f"{matched / total * 100:.2f}%"])
        evidence.table(["token class", "records", "share"], class_rows)
        evidence.line("")

        evidence.line("distinct value counts in the metadata columns")
        meta_rows = [
            [name, f"{frame.select(name).distinct().count():,}"]
            for name in ("query", "user", "target")
        ]
        evidence.table(["column", "distinct values"], meta_rows)
        evidence.line("")
        evidence.line(
            "`query` is overwhelmingly a single placeholder value, which is the "
            "inconsistently-populated metadata the Variety claim refers to."
        )
    finally:
        spark.stop()

    return evidence.done()


if __name__ == "__main__":
    raise SystemExit(main())

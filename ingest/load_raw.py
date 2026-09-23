"""C1/C4: load the Sentiment140 CSV into the lake as Parquet.

The schema is declared explicitly. `inferSchema` on 1.6M rows costs a full
extra pass for a result we already know (PRD §5.1), and it would guess `date`
wrong anyway.

Output: `/sentiment/raw`, Parquet, partitioned by label.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from ingest.download import CSV_NAME, RAW_DIR
from storage import hdfs

#: The six columns the corpus actually has, in order (C2).
#: Sentiment140 ships without a header row.
RAW_SCHEMA = StructType(
    [
        StructField("target", IntegerType(), nullable=False),
        StructField("id", LongType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        StructField("query", StringType(), nullable=True),
        StructField("user", StringType(), nullable=True),
        StructField("text", StringType(), nullable=True),
    ]
)

#: Sentiment140 is ISO-8859-1, not UTF-8. Reading it as UTF-8 mangles every
#: non-ASCII character, which would silently corrupt the emoji experiments.
SOURCE_ENCODING = "ISO-8859-1"

#: 0 -> negative, 4 -> positive. There is no 2 (neutral) in this corpus (C3).
LABEL_MAP = {0: "negative", 4: "positive"}

#: e.g. "Mon Apr 06 22:19:45 PDT 2009". The weekday and the zone abbreviation
#: are both stripped before parsing. Spark 3's parser rejects day-of-week
#: ('EEE') in parse patterns, and its handling of three-letter zone names is
#: version- and locale-dependent; both are redundant with the rest of the
#: string. Timestamps are therefore treated as wall-clock UTC -- see
#: docs/dataset_profile.md.
DATE_PATTERN = "MMM dd HH:mm:ss yyyy"
WEEKDAY_RE = r"^[A-Za-z]{3}\s+"
ZONE_RE = r"\s+(PDT|PST|UTC|GMT|EDT|EST|CDT|CST|MDT|MST)\s+"


def parse_dates(df: DataFrame) -> DataFrame:
    stripped = F.regexp_replace(F.col("date"), WEEKDAY_RE, "")
    stripped = F.regexp_replace(stripped, ZONE_RE, " ")
    return df.withColumn("date", F.to_timestamp(stripped, DATE_PATTERN))


def add_labels(df: DataFrame) -> DataFrame:
    mapping = F.create_map(
        *[item for pair in LABEL_MAP.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    return df.withColumn("label", mapping[F.col("target")])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=RAW_DIR / CSV_NAME)
    parser.add_argument(
        "--rows", type=int, default=None, help="cap rows (for a fast smoke run)"
    )
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"corpus not found at {args.csv}", file=sys.stderr)
        print("run: python -m ingest.download", file=sys.stderr)
        return 1

    from scripts.spark_session import build

    spark = build("load-raw", mongo=False)
    started = time.monotonic()

    df = (
        spark.read.option("header", "false")
        .option("encoding", SOURCE_ENCODING)
        .option("multiLine", "false")
        .option("escape", '"')
        .schema(RAW_SCHEMA)
        .csv(args.csv.resolve().as_uri())  # explicit file://: fs.defaultFS may be HDFS
    )
    if args.rows:
        df = df.limit(args.rows)

    df = add_labels(parse_dates(df))

    unmapped = df.filter(F.col("label").isNull()).count()
    if unmapped:
        # C3 depends on the label set being exactly {0, 4}. If that is not true
        # of the file we have, the report must not claim it is.
        print(
            f"WARNING: {unmapped:,} rows have a target outside {sorted(LABEL_MAP)}",
            file=sys.stderr,
        )

    unparsed = df.filter(F.col("date").isNull()).count()
    if unparsed:
        # A silently-null date would corrupt every event-time window built
        # from this corpus. Report it rather than writing it.
        print(f"WARNING: {unparsed:,} rows have a date that did not parse", file=sys.stderr)

    destination = hdfs.uri(hdfs.RAW)
    print(f"writing Parquet -> {destination}")
    df.write.mode("overwrite").partitionBy("label").parquet(destination)

    written = spark.read.parquet(destination)
    count = written.count()
    elapsed = time.monotonic() - started

    print(f"\nwrote {count:,} rows in {elapsed:.1f}s")
    print("partitions by label:")
    written.groupBy("label").count().orderBy("label").show(truncate=False)

    spark.stop()
    print(f"C1/C4 satisfied: raw corpus in the lake at {hdfs.RAW} "
          f"(backend={hdfs.backend()})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

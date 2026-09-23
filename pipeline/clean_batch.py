"""C8: Spark batch job -- raw -> clean, plus the seeded train/test split.

Reads `/sentiment/raw`, applies the shared preprocessing module, writes
`/sentiment/clean`, then splits 80/20 on a fixed seed into `/sentiment/train`
and `/sentiment/test`.

The split seed is fixed so the trainer and every later evaluation see the same
partition. A fresh random split per run would make model comparisons
meaningless -- C10's NB-vs-LR table has to compare like with like.
"""

from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import functions as F

from pipeline import preprocess
from pipeline.config import DEFAULT_SEED, EMOJI_STRATEGIES
from storage import hdfs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--emoji",
        choices=EMOJI_STRATEGIES,
        default="strip",
        help="emoji strategy (PRD §5.3); recorded in the metrics document",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--rows", type=int, default=None, help="cap input rows for a fast run"
    )
    parser.add_argument(
        "--keep-retweets",
        action="store_true",
        help="skip the C24 retweet filter (for measuring its effect)",
    )
    args = parser.parse_args()

    if not hdfs.exists(hdfs.RAW):
        print(f"{hdfs.RAW} does not exist -- run `make ingest` first", file=sys.stderr)
        return 1

    from scripts.spark_session import build

    spark = build("clean-batch", mongo=False)
    started = time.monotonic()

    raw = spark.read.parquet(hdfs.uri(hdfs.RAW))
    if args.rows:
        raw = raw.limit(args.rows)
    raw_count = raw.count()
    print(f"read {raw_count:,} rows from {hdfs.RAW}")

    clean = preprocess.prepare(
        raw, emoji_strategy=args.emoji, drop_retweets=not args.keep_retweets
    )

    # Empty cleaned text carries no signal and would train the model on noise.
    clean = clean.filter(F.length(F.col(preprocess.TEXT_CLEAN)) > 0)

    clean_count = clean.cache().count()
    dropped = raw_count - clean_count
    print(
        f"cleaned {clean_count:,} rows "
        f"({dropped:,} dropped: retweets + empty-after-cleaning)"
    )

    clean_uri = hdfs.uri(hdfs.CLEAN)
    print(f"writing -> {clean_uri}")
    clean.write.mode("overwrite").parquet(clean_uri)

    train, test = clean.randomSplit([0.8, 0.2], seed=args.seed)
    train_uri, test_uri = hdfs.uri(hdfs.TRAIN), hdfs.uri(hdfs.TEST)
    print(f"writing -> {train_uri}")
    train.write.mode("overwrite").parquet(train_uri)
    print(f"writing -> {test_uri}")
    test.write.mode("overwrite").parquet(test_uri)

    train_count = spark.read.parquet(train_uri).count()
    test_count = spark.read.parquet(test_uri).count()
    elapsed = time.monotonic() - started

    print(f"\ndone in {elapsed:.1f}s")
    print(f"  clean  {clean_count:,}")
    print(f"  train  {train_count:,}  ({train_count / clean_count * 100:.1f}%)")
    print(f"  test   {test_count:,}  ({test_count / clean_count * 100:.1f}%)")
    print(f"  seed   {args.seed}   emoji strategy: {args.emoji}")

    print("\nlabel balance in train:")
    spark.read.parquet(train_uri).groupBy("label").count().orderBy("label").show(
        truncate=False
    )

    spark.stop()
    print(f"C8 satisfied: cleaned corpus and split written (backend={hdfs.backend()})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

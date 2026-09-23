"""C9/C10/C12: train a sentiment classifier and persist the whole pipeline.

    Tokenizer -> StopWordsRemover -> HashingTF -> IDF -> (NaiveBayes | LogisticRegression)

The persisted artifact is the entire `PipelineModel`, not the bare classifier.
That is what guarantees the streaming job applies identical feature
transformation: it loads this object and calls `.transform()`. The alternative
-- reimplementing featurisation inside the stream job -- is how training/serving
skew gets in, and it is subtle enough that nobody notices for weeks.

HashingTF rather than CountVectorizer is deliberate (PRD 4.3): CountVectorizer
holds the vocabulary on the driver, which is exactly the memory pressure a
16 GB machine cannot absorb at corpus scale.
"""

from __future__ import annotations

import argparse
import sys
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql import functions as F

from pipeline import preprocess
from pipeline.config import (
    DEFAULT_NUM_FEATURES,
    DEFAULT_SEED,
    DEFAULT_TRAIN_ROWS,
    EMOJI_STRATEGIES,
    MODEL_NAMES,
)
from storage import hdfs
from trainer.label_policy import (
    DEFAULT_NEUTRAL_THRESHOLD,
    LABEL_ORDER,
    NeutralPolicy,
    label_to_index,
)

#: Below this share for any class, a sample is treated as degenerate.
MIN_CLASS_SHARE = 0.10

RAW_FEATURES = "raw_features"
FEATURES = "features"


def build_pipeline(
    model_key: str, *, num_features: int, use_idf: bool, seed: int
) -> Pipeline:
    """The single Pipeline definition. Both models share every feature stage.

    C10 compares NB against LR "on identical features". Sharing the stage
    construction here is what makes that claim true rather than aspirational.
    """
    stages = list(preprocess.tokenizer_stages())
    stages.append(
        HashingTF(
            inputCol=preprocess.FILTERED_TOKENS,
            outputCol=RAW_FEATURES,
            numFeatures=num_features,
        )
    )
    if use_idf:
        stages.append(IDF(inputCol=RAW_FEATURES, outputCol=FEATURES, minDocFreq=2))
        features_col = FEATURES
    else:
        features_col = RAW_FEATURES

    if model_key == "nb":
        # Multinomial NB requires non-negative features, which TF-IDF satisfies.
        classifier = NaiveBayes(
            featuresCol=features_col,
            labelCol="label_index",
            modelType="multinomial",
            smoothing=1.0,
        )
    elif model_key == "lr":
        classifier = LogisticRegression(
            featuresCol=features_col,
            labelCol="label_index",
            maxIter=20,
            regParam=0.01,
            family="binomial",
        )
    else:
        raise ValueError(f"unknown model {model_key!r}; expected one of {sorted(MODEL_NAMES)}")

    stages.append(classifier)
    return Pipeline(stages=stages)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=sorted(MODEL_NAMES), default="nb")
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_TRAIN_ROWS,
        help=f"row cap (default {DEFAULT_TRAIN_ROWS:,}; sized for this machine)",
    )
    parser.add_argument(
        "--full", action="store_true", help="use the complete corpus instead of --rows"
    )
    parser.add_argument("--emoji", choices=EMOJI_STRATEGIES, default="strip")
    parser.add_argument("--features", type=int, default=DEFAULT_NUM_FEATURES)
    parser.add_argument("--idf", dest="idf", action="store_true", default=True)
    parser.add_argument("--no-idf", dest="idf", action="store_false")
    parser.add_argument(
        "--neutral", choices=["none", "threshold"], default="none",
        help="label policy recorded with the metrics (PRD 6.3)",
    )
    parser.add_argument(
        "--neutral-threshold", type=float, default=DEFAULT_NEUTRAL_THRESHOLD
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--suffix", default="", help="suffix the model directory (for experiments)"
    )
    args = parser.parse_args()

    # Validate the policy before spending minutes on a fit.
    NeutralPolicy(
        strategy=args.neutral,
        threshold=args.neutral_threshold if args.neutral == "threshold" else None,
    )

    if not hdfs.exists(hdfs.TRAIN):
        print(f"{hdfs.TRAIN} does not exist -- run `make clean-batch` first", file=sys.stderr)
        return 1

    from scripts.spark_session import build

    spark = build(f"train-{args.model}", mongo=False)

    train = spark.read.parquet(hdfs.uri(hdfs.TRAIN))
    if not args.full:
        # A seeded random sample, NOT `limit`: the lake is partitioned by
        # label, so the first N rows all come from one partition -- a bare
        # `limit` once produced a 200k-row training set that was 100%
        # negative. Ordering by a seeded rand() is uniform and reproducible.
        train = train.orderBy(F.rand(args.seed)).limit(args.rows)

    # The corpus in /sentiment/clean was cleaned with the batch job's emoji
    # strategy. Re-cleaning here lets a single clean run serve every emoji
    # experiment without re-running the whole batch job three times.
    train = preprocess.clean_text(train, emoji_strategy=args.emoji)
    train = label_to_index(train).select("label", "label_index", preprocess.TEXT_CLEAN)
    train = train.cache()

    row_count = train.count()
    print(f"training {MODEL_NAMES[args.model]} on {row_count:,} rows")
    print(f"  features   HashingTF({args.features:,}){' + IDF' if args.idf else ''}")
    print(f"  emoji      {args.emoji}")
    print(f"  seed       {args.seed}")
    print("\nlabel balance:")
    balance = {row["label"]: row["count"] for row in train.groupBy("label").count().collect()}
    for label in LABEL_ORDER:
        print(f"  {label:<10} {balance.get(label, 0):>10,}")

    # Refuse to fit on a degenerate sample. A single-class model scores a
    # meaningless accuracy, and evaluate.py would publish it as a result.
    missing = [label for label in LABEL_ORDER if not balance.get(label)]
    if missing:
        print(f"\nREFUSING TO TRAIN: no {missing} rows in the sample", file=sys.stderr)
        spark.stop()
        return 1
    smallest = min(balance[label] for label in LABEL_ORDER)
    if smallest / row_count < MIN_CLASS_SHARE:
        print(
            f"\nREFUSING TO TRAIN: smallest class is {smallest / row_count:.1%} of "
            f"the sample (minimum {MIN_CLASS_SHARE:.0%})",
            file=sys.stderr,
        )
        spark.stop()
        return 1

    pipeline = build_pipeline(
        args.model, num_features=args.features, use_idf=args.idf, seed=args.seed
    )

    started = time.monotonic()
    model = pipeline.fit(train)
    duration = time.monotonic() - started
    print(f"fit completed in {duration:.1f}s")

    destination = hdfs.model_uri(args.model + args.suffix)
    print(f"persisting PipelineModel -> {destination}")
    model.write().overwrite().save(destination)

    # The trainer records what it did; evaluate.py reads it back so the
    # metrics document describes the model that was actually fitted rather
    # than the flags someone typed later.
    manifest = {
        "model_key": args.model,
        "model_name": MODEL_NAMES[args.model],
        "train_rows": row_count,
        "num_features": args.features,
        "idf": args.idf,
        "emoji_strategy": args.emoji,
        "neutral_strategy": args.neutral,
        "neutral_threshold": (
            args.neutral_threshold if args.neutral == "threshold" else None
        ),
        "seed": args.seed,
        "train_duration_sec": round(duration, 2),
        "full_corpus": args.full,
    }
    _write_manifest(spark, destination, manifest)

    spark.stop()
    print(f"\nC9/C12 satisfied: {MODEL_NAMES[args.model]} persisted at {destination}")
    print(f"next: python -m trainer.evaluate --model {args.model}{args.suffix}")
    return 0


def _write_manifest(spark, destination: str, manifest: dict) -> None:
    """Store the training manifest beside the model, inside the lake.

    Written through Spark rather than a local file handle so it lands in HDFS
    or the local lake according to STORAGE_BACKEND, exactly like the model.
    """
    import json

    payload = [(json.dumps(manifest),)]
    frame = spark.createDataFrame(payload, "manifest string")
    frame.coalesce(1).write.mode("overwrite").text(f"{destination}_manifest")


if __name__ == "__main__":
    raise SystemExit(main())

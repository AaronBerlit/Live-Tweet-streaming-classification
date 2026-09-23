"""C11: evaluate a persisted model and write one real metrics document.

Never overwrites: each run appends, so the Model page can show a history and
an NB-vs-LR comparison (C10).

PRD rule 4 governs this file more than any other. Every number written here
is computed from the test split by the code below. If evaluation cannot run,
nothing is written, the API returns an empty array, and the UI says "not yet
evaluated". There is no default, no fallback, and no placeholder.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from api.models import FeatureConfig, Metrics
from pipeline import preprocess
from pipeline.config import COLL_METRICS, DB_NAME, MODEL_NAMES, settings
from storage import hdfs
from trainer.label_policy import (
    LABEL_ORDER,
    NeutralPolicy,
    apply_neutral,
    index_to_label,
    label_to_index,
    neutral_share,
)


def load_manifest(spark, model_uri: str) -> dict:
    """Read the manifest train.py wrote beside the model.

    The metrics document must describe the model that was actually fitted, not
    the flags someone happens to pass to this script.
    """
    try:
        rows = spark.read.text(f"{model_uri}_manifest").collect()
    except Exception as exc:  # noqa: BLE001 - absence is the expected failure
        raise FileNotFoundError(
            f"no training manifest beside {model_uri}. "
            f"Re-run trainer.train so the feature configuration is recorded."
        ) from exc
    if not rows:
        raise FileNotFoundError(f"empty training manifest beside {model_uri}")
    return json.loads(rows[0]["value"])


def confusion_matrix(scored, classes: list[str]) -> list[list[int]]:
    """Rows are actual classes, columns predicted, both in `classes` order."""
    counts = {
        (row["label"], row["prediction_label"]): row["n"]
        for row in scored.groupBy("label", "prediction_label")
        .agg(F.count("*").alias("n"))
        .collect()
    }
    return [[int(counts.get((actual, predicted), 0)) for predicted in classes] for actual in classes]


def per_class_metrics(matrix: list[list[int]], classes: list[str]) -> dict[str, dict[str, float]]:
    """Precision, recall and F1 per class, computed from the matrix itself.

    Derived from the confusion matrix rather than from a second evaluator pass,
    so the published matrix and the published scores cannot disagree.
    """
    precision, recall, f1 = {}, {}, {}
    for index, name in enumerate(classes):
        true_positive = matrix[index][index]
        predicted = sum(row[index] for row in matrix)
        actual = sum(matrix[index])
        p = true_positive / predicted if predicted else 0.0
        r = true_positive / actual if actual else 0.0
        precision[name] = round(p, 4)
        recall[name] = round(r, 4)
        f1[name] = round(2 * p * r / (p + r), 4) if (p + r) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="nb", help="model directory key, e.g. nb, lr")
    parser.add_argument("--notes", default="", help="free-text note on this run")
    parser.add_argument(
        "--no-write", action="store_true", help="compute and print without writing"
    )
    args = parser.parse_args()

    model_uri = hdfs.model_uri(args.model)
    if not hdfs.exists(hdfs.MODELS / args.model):
        print(f"no persisted model at {model_uri}", file=sys.stderr)
        print(f"run: python -m trainer.train --model {args.model}", file=sys.stderr)
        return 1
    if not hdfs.exists(hdfs.TEST):
        print(f"{hdfs.TEST} does not exist -- run `make clean-batch` first", file=sys.stderr)
        return 1

    from scripts.spark_session import build

    spark = build(f"evaluate-{args.model}", mongo=False)

    try:
        manifest = load_manifest(spark, model_uri)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        spark.stop()
        return 1

    policy = NeutralPolicy(
        strategy=manifest["neutral_strategy"], threshold=manifest["neutral_threshold"]
    )

    print(f"loading PipelineModel from {model_uri}")
    started = time.monotonic()
    model = PipelineModel.load(model_uri)
    print(f"  loaded in {time.monotonic() - started:.1f}s")

    test = spark.read.parquet(hdfs.uri(hdfs.TEST))
    # Featurise the test set exactly as the training set was featurised.
    test = preprocess.clean_text(test, emoji_strategy=manifest["emoji_strategy"])
    test = label_to_index(test)

    scored = model.transform(test)
    scored = scored.withColumn("prediction_label", index_to_label(F.col("prediction")))
    # `probability` is an MLlib Vector; its Catalyst representation is a struct,
    # not an array, so array functions cannot read it until it is converted.
    scored = scored.withColumn(
        "confidence", F.array_max(vector_to_array(F.col("probability")))
    )
    scored = apply_neutral(scored, policy).cache()

    test_rows = scored.count()
    if test_rows == 0:
        print("the test split is empty -- nothing to evaluate", file=sys.stderr)
        spark.stop()
        return 1

    classes = policy.classes
    matrix = confusion_matrix(scored, classes)
    scores = per_class_metrics(matrix, classes)
    correct = sum(matrix[i][i] for i in range(len(classes)))
    accuracy = correct / test_rows

    print(f"\nevaluated {test_rows:,} test rows")
    print(f"  accuracy  {accuracy:.4f}")
    header = "  " + "".join(f"{name:>12}" for name in classes)
    print("\n  confusion matrix (rows actual, columns predicted)")
    print(header)
    for name, row in zip(classes, matrix):
        print(f"  {name:<10}" + "".join(f"{value:>12,}" for value in row))
    print("\n  per class")
    for name in classes:
        print(
            f"  {name:<10} precision {scores['precision'][name]:.4f}"
            f"  recall {scores['recall'][name]:.4f}"
            f"  f1 {scores['f1'][name]:.4f}"
        )

    if policy.strategy == "threshold":
        share = neutral_share(scored)
        print(f"\n  the threshold policy relabelled {share * 100:.2f}% of records neutral")

    document = Metrics(
        model_name=manifest["model_name"],
        trained_at=datetime.now(timezone.utc),
        train_rows=manifest["train_rows"],
        test_rows=test_rows,
        accuracy=round(accuracy, 4),
        precision=scores["precision"],
        recall=scores["recall"],
        f1=scores["f1"],
        confusion_matrix=matrix,
        feature_config=FeatureConfig(
            vectorizer="hashingtf",
            num_features=manifest["num_features"],
            idf=manifest["idf"],
            emoji_strategy=manifest["emoji_strategy"],
        ),
        neutral_strategy=manifest["neutral_strategy"],
        neutral_threshold=manifest["neutral_threshold"],
        train_duration_sec=manifest["train_duration_sec"],
        notes=args.notes
        or (
            "full corpus"
            if manifest.get("full_corpus")
            else f"{manifest['train_rows']:,}-row subset"
        ),
    )

    spark.stop()

    if args.no_write:
        print("\n--no-write: nothing persisted")
        return 0

    payload = document.model_dump(by_alias=True, exclude_none=True)
    payload.pop("_id", None)
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10_000)
    result = client[DB_NAME][COLL_METRICS].insert_one(payload)
    print(f"\nC11 satisfied: metrics document {result.inserted_id} written")
    print("the Model page will now show these figures")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

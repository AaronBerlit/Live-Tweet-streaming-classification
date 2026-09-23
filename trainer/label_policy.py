"""Neutral-class policy (PRD C3, 6.3).

Sentiment140 provides only positive and negative labels. There is no neutral
ground truth in the corpus, so neutral is not free -- it has to be either
declined, inferred at inference time, or imported from elsewhere. Each choice
changes what the report can honestly claim, so the choice is recorded on every
metrics document as `neutral_strategy`.

The UI reads that field and renders accordingly. It never hardcodes three
classes, and under `none` it never renders a permanently-zero neutral series,
which would falsely imply the model looked for neutral records and found none.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F

from pipeline.config import NEUTRAL_STRATEGIES

#: Index -> label. MLlib emits a numeric prediction; this is the only place
#: that mapping is defined, so the batch and streaming paths cannot disagree.
LABEL_ORDER = ["negative", "positive"]

DEFAULT_NEUTRAL_THRESHOLD = 0.6


@dataclass(frozen=True)
class NeutralPolicy:
    """How (and whether) a neutral class is produced."""

    strategy: str
    threshold: float | None = None

    def __post_init__(self) -> None:
        if self.strategy not in NEUTRAL_STRATEGIES:
            raise ValueError(
                f"neutral strategy must be one of {NEUTRAL_STRATEGIES}, "
                f"got {self.strategy!r}"
            )
        if self.strategy == "threshold" and self.threshold is None:
            raise ValueError("the 'threshold' strategy requires a threshold value")
        if self.threshold is not None and not 0.0 < self.threshold < 1.0:
            raise ValueError(
                f"neutral threshold must be strictly between 0 and 1, "
                f"got {self.threshold}"
            )

    @property
    def classes(self) -> list[str]:
        """The classes this policy can actually emit."""
        if self.strategy == "none":
            return list(LABEL_ORDER)
        return [*LABEL_ORDER, "neutral"]

    def describe(self) -> str:
        """The footnote the UI shows under the sentiment split."""
        if self.strategy == "none":
            return (
                "Binary classification. Sentiment140 contains only positive and "
                "negative labels, so no neutral class was trained."
            )
        if self.strategy == "threshold":
            return (
                f"Records whose highest class probability is below "
                f"{self.threshold} are relabelled neutral at inference time. "
                f"The model itself remains binary."
            )
        return "A supplementary labelled neutral set was incorporated."


def label_to_index(df: DataFrame, label_col: str = "label") -> DataFrame:
    """Map the string label to the numeric label MLlib trains on.

    An explicit map rather than a StringIndexer: StringIndexer orders by
    frequency, so a differently-balanced training subset would silently swap
    the class indices and invert every prediction.
    """
    mapping = F.create_map(
        *[
            item
            for index, name in enumerate(LABEL_ORDER)
            for item in (F.lit(name), F.lit(float(index)))
        ]
    )
    return df.withColumn("label_index", mapping[F.col(label_col)])


def index_to_label(column):
    """Map a numeric prediction back to its class name."""
    mapping = F.create_map(
        *[
            item
            for index, name in enumerate(LABEL_ORDER)
            for item in (F.lit(float(index)), F.lit(name))
        ]
    )
    return mapping[column]


def apply_neutral(
    df: DataFrame,
    policy: NeutralPolicy,
    *,
    prediction_col: str = "prediction_label",
    confidence_col: str = "confidence",
) -> DataFrame:
    """Apply the neutral policy to already-scored records.

    Under `none` and `external` this is a no-op on the predictions: `none`
    has no neutral class, and `external` resolved neutral during training.
    """
    if policy.strategy != "threshold":
        return df
    return df.withColumn(
        prediction_col,
        F.when(F.col(confidence_col) < policy.threshold, F.lit("neutral")).otherwise(
            F.col(prediction_col)
        ),
    )


def neutral_share(df: DataFrame, prediction_col: str = "prediction_label") -> float:
    """Fraction of records the threshold policy captured as neutral.

    PRD 6.3 asks for this to be reported: a threshold that relabels 40% of
    the corpus is a different claim from one that relabels 3%.
    """
    total = df.count()
    if total == 0:
        return 0.0
    captured = df.filter(F.col(prediction_col) == "neutral").count()
    return captured / total

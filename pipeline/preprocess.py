"""The single preprocessing implementation (PRD §4.2 Rule A).

Both `pipeline/clean_batch.py` and `pipeline/stream_job.py` import this module.
There is exactly one implementation of every transform, and
`tests/test_parity.py` asserts the batch and stream paths produce
byte-identical `text_clean` for the same input.

Training/serving skew -- the model seeing differently-cleaned text at inference
than at training -- is the classic silent defect in this class of system.
Accuracy drops and nobody can find why. Structurally preventing it is worth
more than any amount of care.

Every function here is a pure DataFrame -> DataFrame transformation with no
side effects, so the same code runs unchanged over a batch DataFrame and a
streaming DataFrame.
"""

from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql import DataFrame, functions as F

from pipeline.config import EMOJI_STRATEGIES, settings
from pipeline.emoji_lexicon import (
    EMOJI_RANGES,
    NEGATIVE_RE,
    NEGATIVE_TOKEN,
    NEUTRAL_TOKEN,
    POSITIVE_RE,
    POSITIVE_TOKEN,
)

# --- column names, so no caller spells them by hand -------------------------

TEXT_RAW = "text"
TEXT_CLEAN = "text_clean"
HASHTAGS = "hashtags"
DEDUP_KEY = "dedup_key"
TOKENS = "tokens"
FILTERED_TOKENS = "filtered_tokens"

# --- patterns ---------------------------------------------------------------

URL_RE = r"(https?://\S+|www\.\S+)"
MENTION_RE = r"@\w+"
RETWEET_RE = r"^\s*RT\s"
WHITESPACE_RE = r"\s+"
HASHTAG_EXTRACT_RE = r"#(\w+)"


def _apply_emoji_strategy(column, strategy: str):
    """Resolve emoji per EMOJI_STRATEGY (PRD §5.3).

    The report flags emoji handling as unresolved; all three options are
    implemented so the choice can be made from measured F1 deltas rather than
    intuition. See docs/experiments.md.
    """
    if strategy not in EMOJI_STRATEGIES:
        raise ValueError(
            f"emoji strategy must be one of {EMOJI_STRATEGIES}, got {strategy!r}"
        )

    if strategy == "keep":
        return column

    if strategy == "strip":
        return F.regexp_replace(column, EMOJI_RANGES, " ")

    # 'map': known emoji become sentiment tokens; the rest become neutral
    # tokens, so an unlisted emoji degrades to "neutral" instead of vanishing.
    mapped = F.regexp_replace(column, POSITIVE_RE, f" {POSITIVE_TOKEN} ")
    mapped = F.regexp_replace(mapped, NEGATIVE_RE, f" {NEGATIVE_TOKEN} ")
    return F.regexp_replace(mapped, EMOJI_RANGES, f" {NEUTRAL_TOKEN} ")


def clean_text(
    df: DataFrame,
    text_col: str = TEXT_RAW,
    *,
    emoji_strategy: str | None = None,
    output_col: str = TEXT_CLEAN,
) -> DataFrame:
    """lowercase -> strip URLs -> strip @mentions -> emoji policy -> collapse ws.

    The hashtag '#' is PRESERVED: hashtag analysis (C21) depends on it, and
    stripping it here would silently break the Hashtag panel.

    Null text becomes an empty string rather than propagating null, so
    downstream transforms never have to special-case it.
    """
    strategy = emoji_strategy or _default_emoji_strategy()

    column = F.lower(F.coalesce(F.col(text_col), F.lit("")))
    column = F.regexp_replace(column, URL_RE, " ")
    column = F.regexp_replace(column, MENTION_RE, " ")
    column = _apply_emoji_strategy(column, strategy)
    column = F.trim(F.regexp_replace(column, WHITESPACE_RE, " "))

    return df.withColumn(output_col, column)


def _default_emoji_strategy() -> str:
    import os

    return os.environ.get("EMOJI_STRATEGY", "strip").lower()


def extract_hashtags(
    df: DataFrame, text_col: str = TEXT_CLEAN, output_col: str = HASHTAGS
) -> DataFrame:
    """Hashtags as array<string>, lowercased, '#' removed (C21).

    Reads the cleaned text, not the raw text, so URL fragments (`...#section`)
    cannot masquerade as hashtags.
    """
    source = F.coalesce(F.col(text_col), F.lit(""))
    # F.lit rather than F.expr with an interpolated pattern: inside a Spark SQL
    # string literal the backslash in '\w' is consumed as an escape, silently
    # turning the pattern into '#(w+)' and matching nothing.
    extracted = F.regexp_extract_all(source, F.lit(HASHTAG_EXTRACT_RE), 1)
    return df.withColumn(output_col, F.when(source == "", F.array()).otherwise(extracted))


def filter_retweets(df: DataFrame, text_col: str = TEXT_RAW) -> DataFrame:
    """Drop retweets (C24).

    Retweets are near-duplicates of an original post. Left in, they let a
    single viral message dominate a window and skew the sentiment split -- the
    skew problem the report raises. Matched against the RAW text, because
    cleaning lowercases the 'RT' marker away.
    """
    return df.filter(~F.coalesce(F.col(text_col), F.lit("")).rlike(RETWEET_RE))


def add_dedup_key(
    df: DataFrame,
    text_col: str = TEXT_CLEAN,
    user_col: str = "user",
    output_col: str = DEDUP_KEY,
) -> DataFrame:
    """sha1 of normalised text + user, for windowed deduplication (C18).

    Deterministic across runs and across the batch/stream boundary: the same
    record always produces the same key, which is what makes
    `dropDuplicatesWithinWatermark` and idempotent writes work.
    """
    user = F.coalesce(F.col(user_col), F.lit("")) if user_col in df.columns else F.lit("")
    return df.withColumn(
        output_col,
        F.sha1(F.concat_ws("|", F.coalesce(F.col(text_col), F.lit("")), user)),
    )


def tokenizer_stages(
    input_col: str = TEXT_CLEAN,
    tokens_col: str = TOKENS,
    output_col: str = FILTERED_TOKENS,
) -> list:
    """The Tokenizer + StopWordsRemover stages (C7).

    Returned as stages rather than applied, so `trainer/train.py` can embed
    exactly these objects in the persisted PipelineModel. The streaming job
    then gets identical featurisation for free -- it never rebuilds them.
    """
    return [
        Tokenizer(inputCol=input_col, outputCol=tokens_col),
        StopWordsRemover(inputCol=tokens_col, outputCol=output_col),
    ]


def tokenize(df: DataFrame, input_col: str = TEXT_CLEAN) -> DataFrame:
    """Apply the tokenizer stages directly. Used by tests and ad-hoc analysis."""
    return Pipeline(stages=tokenizer_stages(input_col=input_col)).fit(df).transform(df)


def prepare(
    df: DataFrame,
    *,
    emoji_strategy: str | None = None,
    drop_retweets: bool = True,
) -> DataFrame:
    """The full preprocessing chain, in the order PRD §8 step 5 specifies.

    This is the function both the batch job and the streaming job call. Neither
    reimplements the ordering, because the order matters: retweets are filtered
    on raw text, hashtags are extracted from cleaned text, and the dedup key is
    computed from the cleaned text.
    """
    if drop_retweets:
        df = filter_retweets(df)
    df = clean_text(df, emoji_strategy=emoji_strategy)
    df = extract_hashtags(df)
    return add_dedup_key(df)

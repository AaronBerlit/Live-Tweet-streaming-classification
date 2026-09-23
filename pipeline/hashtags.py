"""C21: hashtag aggregation.

Extraction itself lives in `preprocess.extract_hashtags`, because it has to be
the same code the batch path uses (Rule A). What lives here is the
*aggregation*: turning scored records into the per-window, per-tag documents
the Hashtag panel reads.

Aggregation is recomputed from the `scored` collection rather than accumulated
with `$inc`, for the same reason window counts are: recomputation is idempotent
under batch replay, so restarting the stream cannot double a tag's count.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from pymongo import UpdateOne

from api.models import Hashtag

#: Every class a tag's split can contain. Fixed so a window with no neutral
#: records still writes `neutral: 0` rather than omitting the key, which would
#: make the frozen §7.3 contract fail validation.
SPLIT_CLASSES = ("positive", "negative", "neutral")


def window_pipeline(start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Aggregation pipeline: tag counts by prediction within one window."""
    return [
        {"$match": {"event_time": {"$gte": start, "$lt": end}}},
        {"$unwind": "$hashtags"},
        {
            "$group": {
                "_id": {"tag": "$hashtags", "prediction": "$prediction"},
                "count": {"$sum": 1},
            }
        },
    ]


def split_from_groups(groups) -> dict[str, dict[str, int]]:
    """Fold `(tag, prediction) -> count` rows into `tag -> sentiment split`."""
    totals: dict[str, dict[str, int]] = {}
    for group in groups:
        tag = group["_id"]["tag"]
        prediction = group["_id"]["prediction"]
        bucket = totals.setdefault(tag, {name: 0 for name in SPLIT_CLASSES})
        if prediction in bucket:
            bucket[prediction] = int(group["count"])
    return totals


def upserts_for_window(
    scored_collection,
    start: datetime,
    window_minutes: int,
    run_id: str,
    *,
    source: str = "spark",
) -> list[UpdateOne]:
    """Idempotent upserts for every tag active in one window.

    Returns operations rather than executing them, so a caller can batch a
    whole micro-batch's windows into one `bulk_write`.
    """
    end = start + timedelta(minutes=window_minutes)
    groups = scored_collection.aggregate(window_pipeline(start, end))
    totals = split_from_groups(groups)

    return [
        UpdateOne(
            {"_id": Hashtag.make_id(start, tag)},
            {
                "$set": {
                    "window_start": start,
                    "tag": tag,
                    "count": sum(split.values()),
                    "sentiment_split": split,
                    "run_id": run_id,
                    "source": source,
                }
            },
            upsert=True,
        )
        for tag, split in totals.items()
        # A tag with no records in the window is not written at all: absence
        # and zero are different, exactly as for window counts.
        if sum(split.values()) > 0
    ]


def top_tags(rows: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    """Rank already-aggregated tag rows. Used by the evidence scripts."""
    return sorted(rows, key=lambda row: (-row["count"], row["tag"]))[:limit]

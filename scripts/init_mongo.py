"""Create every MongoDB collection and index the data contracts require.

Idempotent: safe to run on every `make setup`. The indexes here are not
optional decoration -- C20 (the dashboard reads small aggregated documents via
an index) is proved by `make evidence-query-plan` reading these, and the TTL on
`scored` is the PRD §7.2 mitigation for uncontrolled growth.
"""

from __future__ import annotations

import sys

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import PyMongoError

from pipeline.config import (
    BATCHES_TTL_SECONDS,
    COLL_BATCHES,
    COLL_HASHTAGS,
    COLL_HEARTBEAT,
    COLL_METRICS,
    COLL_RUNS,
    COLL_SCORED,
    COLL_WINDOWS,
    DB_NAME,
    SCORED_TTL_SECONDS,
    settings,
)

#: collection -> [(keys, kwargs)]
INDEXES: dict[str, list[tuple[list[tuple[str, int]], dict]]] = {
    COLL_WINDOWS: [
        ([("window_start", DESCENDING), ("prediction", ASCENDING)], {"name": "window_start_-1_prediction_1"}),
        ([("run_id", ASCENDING)], {"name": "run_id_1"}),
    ],
    COLL_SCORED: [
        ([("event_time", DESCENDING)], {"name": "event_time_-1"}),
        ([("prediction", ASCENDING), ("event_time", DESCENDING)], {"name": "prediction_1_event_time_-1"}),
        ([("dedup_key", ASCENDING)], {"name": "dedup_key_1"}),
        # PRD §7.2: 24h TTL. "Do not omit."
        ([("ingest_time", ASCENDING)], {"name": "ingest_time_ttl", "expireAfterSeconds": SCORED_TTL_SECONDS}),
    ],
    COLL_HASHTAGS: [
        ([("window_start", DESCENDING), ("count", DESCENDING)], {"name": "window_start_-1_count_-1"}),
        ([("tag", ASCENDING)], {"name": "tag_1"}),
    ],
    COLL_METRICS: [
        ([("trained_at", DESCENDING)], {"name": "trained_at_-1"}),
        ([("model_name", ASCENDING), ("trained_at", DESCENDING)], {"name": "model_name_1_trained_at_-1"}),
    ],
    COLL_RUNS: [
        ([("started_at", DESCENDING)], {"name": "started_at_-1"}),
    ],
    COLL_HEARTBEAT: [],  # single document keyed by _id; no secondary index needed
    COLL_BATCHES: [
        ([("batch_at", DESCENDING)], {"name": "batch_at_-1"}),
        ([("run_id", ASCENDING), ("batch_at", DESCENDING)], {"name": "run_id_1_batch_at_-1"}),
        ([("batch_at", ASCENDING)], {"name": "batch_at_ttl", "expireAfterSeconds": BATCHES_TTL_SECONDS}),
    ],
}


def main() -> int:
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10_000)
    try:
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"cannot reach MongoDB at {settings.mongo_uri}: {exc}", file=sys.stderr)
        print("start it with: docker compose up -d mongo", file=sys.stderr)
        return 1

    db = client[DB_NAME]
    existing = set(db.list_collection_names())

    for collection, specs in INDEXES.items():
        if collection not in existing:
            db.create_collection(collection)
            print(f"created collection {collection}")
        for keys, kwargs in specs:
            name = db[collection].create_index(keys, **kwargs)
            print(f"  index {collection}.{name}")

    print(f"\n{DB_NAME}: {len(INDEXES)} collections ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

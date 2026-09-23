"""Copy the pipeline's MongoDB results to a cloud MongoDB (Atlas) for hosting.

The hosted dashboard (Vercel) cannot reach the MongoDB running in Docker on the
team laptop, so it reads a snapshot. This copies every collection, recreates
the indexes the dashboard's queries rely on, and deliberately leaves out ONE:
the 24-hour TTL on `scored.ingest_time`. On a frozen snapshot that index would
delete the tweet drill-down within a day -- possibly before anyone looks at it.

Usage:
    ATLAS_URI in .env (never on the command line, never in git), then
    python scripts/snapshot_to_atlas.py --replace

The connection string is read from the environment and is never printed.
"""

from __future__ import annotations

import argparse
import os
import sys

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import PyMongoError

from pipeline.config import DB_NAME, settings

COLLECTIONS = ["windows", "hashtags", "metrics", "runs", "batches", "heartbeat", "scored"]

#: The indexes the API's queries use -- every one from scripts/init_mongo.py
#: except the TTL.
INDEXES = {
    "windows": [[("window_start", DESCENDING), ("prediction", ASCENDING)], [("run_id", ASCENDING)]],
    "scored": [
        [("event_time", DESCENDING)],
        [("prediction", ASCENDING), ("event_time", DESCENDING)],
        [("dedup_key", ASCENDING)],
    ],
    "hashtags": [[("window_start", DESCENDING), ("count", DESCENDING)], [("tag", ASCENDING)]],
    "metrics": [[("trained_at", DESCENDING)]],
    "runs": [[("started_at", DESCENDING)]],
    "batches": [[("batch_at", DESCENDING)], [("run_id", ASCENDING), ("batch_at", DESCENDING)]],
}

BATCH = 5_000


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replace", action="store_true",
                        help="drop the target database first (required if it has data)")
    args = parser.parse_args()

    target_uri = os.environ.get("ATLAS_URI", "").strip()
    if not target_uri:
        print("ATLAS_URI is not set. Add it to .env (see docs in README), then re-run.",
              file=sys.stderr)
        return 1

    source = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10_000)[DB_NAME]
    target_client = MongoClient(target_uri, serverSelectionTimeoutMS=20_000)
    try:
        target_client.admin.command("ping")
    except PyMongoError as exc:
        print(f"cannot reach the target MongoDB: {type(exc).__name__}", file=sys.stderr)
        print("check the user/password in ATLAS_URI and that Network Access allows "
              "0.0.0.0/0 (Vercel has no fixed IP)", file=sys.stderr)
        return 1
    target = target_client[DB_NAME]

    existing = [name for name in target.list_collection_names() if name in COLLECTIONS]
    if existing and not args.replace:
        print(f"target already has {existing}; re-run with --replace to overwrite",
              file=sys.stderr)
        return 1
    if args.replace:
        target_client.drop_database(DB_NAME)

    for name in COLLECTIONS:
        documents = source[name].find({})
        buffer, copied = [], 0
        for document in documents:
            buffer.append(document)
            if len(buffer) >= BATCH:
                target[name].insert_many(buffer, ordered=False)
                copied += len(buffer)
                buffer = []
        if buffer:
            target[name].insert_many(buffer, ordered=False)
            copied += len(buffer)
        for keys in INDEXES.get(name, []):
            target[name].create_index(keys)
        print(f"  {name:<10} {copied:>9,} documents", flush=True)

    ttl = [
        spec for spec in target["scored"].index_information().values()
        if "expireAfterSeconds" in spec
    ]
    print(f"\nsnapshot complete; TTL indexes on target: {len(ttl)} (expected 0)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

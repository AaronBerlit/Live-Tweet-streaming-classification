"""Drop every MongoDB collection in the serving layer. The lake is untouched.

Used by the §12.6 checklist ("dropping all collections shows the checklist,
not a broken dashboard") and to get back to EMPTY mode between demos.
"""

from __future__ import annotations

import argparse
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from pipeline.config import DB_NAME, settings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yes", action="store_true", help="skip the confirmation prompt"
    )
    args = parser.parse_args()

    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10_000)
    try:
        collections = client[DB_NAME].list_collection_names()
    except PyMongoError as exc:
        print(f"cannot reach MongoDB: {exc}", file=sys.stderr)
        return 1

    if not collections:
        print(f"{DB_NAME} is already empty")
        return 0

    print(f"about to drop from {DB_NAME}: {', '.join(sorted(collections))}")
    if not args.yes:
        if input("type 'drop' to confirm: ").strip() != "drop":
            print("aborted")
            return 1

    for name in collections:
        client[DB_NAME].drop_collection(name)
        print(f"  dropped {name}")

    print("\nre-run 'python scripts/init_mongo.py' to recreate the indexes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

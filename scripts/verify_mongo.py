"""C6: prove MongoDB is running and the Spark connector actually works.

Two separate checks, because they fail for different reasons and the PRD's
claim covers both:

1. pymongo reaches the server        -- is the container up?
2. Spark round-trips a DataFrame     -- is the connector version compatible?

Check 2 is the one that historically broke this team (C25), so it writes real
data through the connector and reads it back rather than merely loading a JAR.
"""

from __future__ import annotations

import sys
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from pipeline import versions
from pipeline.config import DB_NAME, settings

PROBE_COLLECTION = "_connector_probe"


def _check_pymongo() -> bool:
    print("--- pymongo ---")
    print(f"  uri  {settings.mongo_uri}")
    try:
        client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10_000)
        info = client.admin.command("buildInfo")
    except PyMongoError as exc:
        print(f"  FAILED: {exc}", file=sys.stderr)
        print("  fix: docker compose up -d mongo", file=sys.stderr)
        return False
    print(f"  server version  {info['version']}")
    collections = client[DB_NAME].list_collection_names()
    print(f"  {DB_NAME} collections: {sorted(collections) or 'none yet'}")
    return True


def _check_spark_connector() -> bool:
    print("\n--- spark mongo connector ---")
    print(f"  package  {versions.MONGO_SPARK_PACKAGE}")
    from scripts.spark_session import build

    started = time.monotonic()
    spark = build("verify-mongo", kafka=False, mongo=True)
    print(f"  session up in {time.monotonic() - started:.1f}s")

    rows = [("probe-a", 1), ("probe-b", 2)]
    frame = spark.createDataFrame(rows, ["name", "value"])

    try:
        (
            frame.write.format("mongodb")
            .mode("overwrite")
            .option("database", DB_NAME)
            .option("collection", PROBE_COLLECTION)
            .save()
        )
        read_back = (
            spark.read.format("mongodb")
            .option("database", DB_NAME)
            .option("collection", PROBE_COLLECTION)
            .load()
        )
        count = read_back.count()
    except Exception as exc:  # noqa: BLE001 - surface the real connector error
        print(f"  FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        print(
            f"  fix: the connector is pinned to Spark {versions.SPARK_VERSION} / "
            f"Scala {versions.SCALA_VERSION} in pipeline/versions.py",
            file=sys.stderr,
        )
        spark.stop()
        return False

    print(f"  wrote 2 rows, read back {count}")
    read_back.orderBy("name").select("name", "value").show(truncate=False)
    spark.stop()

    # Leave no probe data behind.
    MongoClient(settings.mongo_uri)[DB_NAME].drop_collection(PROBE_COLLECTION)
    print("  probe collection dropped")
    return count == len(rows)


def main() -> int:
    if not _check_pymongo():
        return 1
    if not _check_spark_connector():
        return 1
    print("\nC6 verified: MongoDB reachable and the Spark connector round-trips.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

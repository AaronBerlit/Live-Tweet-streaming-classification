"""API against a real MongoDB (PRD §12.1, §12.2, Phase 4 gate).

Runs against the isolated `sentiment_stream_test` database (see
tests/conftest.py). Every test starts from empty collections, so each state --
empty, seeded, live, degraded, malformed -- is constructed explicitly rather
than inherited from whatever the last run left behind.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient
from pymongo import MongoClient

from api import main
from api.repository import Repository
from pipeline.config import DB_NAME, HEARTBEAT_ID, settings

pytestmark = pytest.mark.mongo

NOW = datetime.now(timezone.utc).replace(microsecond=0)


@pytest.fixture
def db():
    assert DB_NAME == "sentiment_stream_test", "tests must never touch the real database"
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=1500, tz_aware=True)
    try:
        client.admin.command("ping")
    except Exception:  # noqa: BLE001
        pytest.skip("no MongoDB reachable; start it with `docker compose up -d mongo`")
    database = client[DB_NAME]
    for name in database.list_collection_names():
        database.drop_collection(name)
    yield database
    client.drop_database(DB_NAME)
    client.close()


@pytest.fixture
def api(db):
    with TestClient(main.app) as client:
        yield client


def window(start: datetime, prediction: str, count: int, source: str = "seed", **extra):
    doc = {
        "_id": f"{start:%Y-%m-%dT%H:%M:%SZ}|{prediction}",
        "window_start": start,
        "window_end": start + timedelta(minutes=1),
        "prediction": prediction,
        "count": count,
        "avg_confidence": 0.8,
        "run_id": "run_test",
        "source": source,
        "updated_at": start + timedelta(minutes=1),
    }
    doc.update(extra)
    return doc


def scored(event_time: datetime, index: int, source: str = "spark"):
    return {
        "_id": ObjectId(),
        "text_raw": f"record {index}",
        "text_clean": f"record {index}",
        "prediction": "positive" if index % 2 else "negative",
        "confidence": 0.9,
        "hashtags": [],
        "event_time": event_time,
        "ingest_time": event_time,
        "dedup_key": f"{index:040d}",
        "run_id": "run_test",
        "source": source,
    }


# --- mode derivation through the real query path ----------------------------


def test_empty_database_derives_empty(api):
    body = api.get("/api/mode").json()
    assert body["mode"] == "EMPTY"


def test_metrics_is_an_empty_list_not_zeros(api):
    """PRD rule 4: un-evaluated means [] and "not yet evaluated", never 0."""
    response = api.get("/api/metrics")
    assert response.status_code == 200
    assert response.json()["data"] == []


def test_seeded_windows_derive_seed(api, db):
    db.windows.insert_one(window(NOW - timedelta(minutes=5), "positive", 10))
    assert api.get("/api/mode").json()["mode"] == "SEED"


def test_spark_windows_without_fresh_heartbeat_derive_replay(api, db):
    db.windows.insert_one(window(NOW - timedelta(minutes=5), "positive", 10, source="spark"))
    db.heartbeat.insert_one({"_id": HEARTBEAT_ID, "last_batch_at": NOW - timedelta(minutes=5), "last_batch_id": 3})
    assert api.get("/api/mode").json()["mode"] == "REPLAY"


def test_fresh_heartbeat_derives_live(api, db):
    db.windows.insert_one(window(NOW - timedelta(minutes=1), "positive", 10, source="spark"))
    db.heartbeat.insert_one({"_id": HEARTBEAT_ID, "last_batch_at": datetime.now(timezone.utc), "last_batch_id": 9})
    assert api.get("/api/mode").json()["mode"] == "LIVE"


# --- no blending of seeded and real data (G6) --------------------------------


def test_summary_excludes_seed_once_spark_data_exists(api, db):
    db.windows.insert_many([
        window(NOW - timedelta(minutes=3), "positive", 1000, source="seed"),
        window(NOW - timedelta(minutes=2), "positive", 7, source="spark"),
    ])
    data = api.get("/api/summary?hours=1").json()["data"]
    assert data["total_classified"] == 7, "seeded counts leaked into a Spark-mode KPI"


# --- degradation -------------------------------------------------------------


def test_health_is_200_with_mongo_error_when_database_unreachable(api):
    """Phase 4 gate: a dead database yields 200 + status error, never a 5xx."""
    original = main.state["repository"]
    main.state["repository"] = Repository("mongodb://127.0.0.1:1", timeout_ms=300)
    try:
        response = api.get("/api/health")
        assert response.status_code == 200
        components = {c["name"]: c for c in response.json()["data"]}
        assert components["mongo"]["status"] == "error"

        summary = api.get("/api/summary")
        assert summary.status_code == 200
        assert summary.json()["data"] is None
        assert summary.json()["warnings"]
    finally:
        main.state["repository"].close()
        main.state["repository"] = original


def test_malformed_window_does_not_break_the_summary(api, db):
    """A document missing a field must degrade the figure, not 500 the page."""
    bad = window(NOW - timedelta(minutes=2), "negative", 5)
    del bad["avg_confidence"]
    db.windows.insert_many([bad, window(NOW - timedelta(minutes=1), "positive", 5)])
    response = api.get("/api/summary?hours=1")
    assert response.status_code == 200
    assert response.json()["data"]["total_classified"] == 10


def test_timestamps_are_serialised_as_utc(api, db):
    db.windows.insert_one(window(NOW - timedelta(minutes=2), "positive", 5))
    data = api.get("/api/summary?hours=1").json()["data"]
    assert data["window_start"].endswith(("Z", "+00:00")), data["window_start"]


# --- keyset pagination (§12.1) -----------------------------------------------


def test_cursor_pagination_has_no_duplicates_or_gaps(db):
    """Many records share one timestamp -- exactly where naive pagination breaks."""
    times = [NOW - timedelta(seconds=s // 25) for s in range(250)]
    db.scored.insert_many([scored(t, i) for i, t in enumerate(times)])

    repository = Repository()
    seen: list[str] = []
    cursor = None
    while True:
        rows, cursor = repository.scored(cursor=cursor, limit=37)
        seen.extend(str(row["_id"]) for row in rows)
        if cursor is None:
            break
    repository.close()

    assert len(seen) == 250, f"expected 250 rows across pages, got {len(seen)}"
    assert len(set(seen)) == 250, "a record appeared on two pages"


def test_malformed_cursor_is_a_warning_not_a_500(api):
    response = api.get("/api/scored?cursor=not-a-real-cursor")
    assert response.status_code == 200
    assert response.json()["warnings"]


def test_search_text_is_escaped_not_interpreted_as_regex(api, db):
    db.scored.insert_one(scored(NOW, 1, source="seed") | {"text_raw": "price (a+b)*"})
    db.windows.insert_one(window(NOW, "positive", 1))
    response = api.get("/api/scored", params={"q": "(a+b)*"})
    assert response.status_code == 200
    assert len(response.json()["data"]["records"]) == 1


# --- live push (§12.2: within 3s of a direct Mongo write) --------------------


def test_websocket_pushes_within_three_seconds_of_a_heartbeat(api, db):
    with api.websocket_connect("/ws/stream") as socket:
        socket.receive_json()  # the initial mode announcement
        started = time.monotonic()
        db.heartbeat.update_one(
            {"_id": HEARTBEAT_ID},
            {"$set": {"last_batch_at": datetime.now(timezone.utc), "last_batch_id": 1}},
            upsert=True,
        )
        event = socket.receive_json()
        elapsed = time.monotonic() - started
    assert event["type"] in {"heartbeat", "window_update", "mode_change"}
    assert elapsed < 3.0, f"push took {elapsed:.2f}s"


# --- data correctness (§12.5) ------------------------------------------------


def _run(db, run_id="run_test"):
    db.runs.insert_one({"_id": run_id, "started_at": NOW})


def test_reconciliation_consistent_when_windows_match_scored(db):
    _run(db)
    start = NOW - timedelta(minutes=10)
    db.scored.insert_many([scored(start + timedelta(seconds=i), i) for i in range(5)])
    db.windows.insert_one(window(start, "positive", 5, source="spark"))
    check = Repository().reconciliation()
    assert check["consistent"], check


def test_reconciliation_flags_a_doubled_window(db):
    """The failure Rule B exists to prevent: a replayed batch doubling a count."""
    _run(db)
    start = NOW - timedelta(minutes=10)
    db.scored.insert_many([scored(start + timedelta(seconds=i), i) for i in range(5)])
    db.windows.insert_one(window(start, "positive", 10, source="spark"))
    check = Repository().reconciliation()
    assert not check["consistent"]
    assert check["difference"] == 5


def test_reconciliation_flags_zero_count_windows(db):
    _run(db)
    db.windows.insert_one(window(NOW - timedelta(minutes=3), "negative", 0, source="spark"))
    assert Repository().reconciliation()["zero_count_windows"] == 1


def test_reconciliation_flags_orphan_runs(db):
    start = NOW - timedelta(minutes=10)
    db.scored.insert_one(scored(start, 1) | {"run_id": "run_never_recorded"})
    db.windows.insert_one(window(start, "positive", 1, source="spark"))
    check = Repository().reconciliation()
    assert check["orphan_run_ids"] == ["run_never_recorded"]
    assert not check["consistent"]


def test_reconciliation_ignores_seeded_data(db):
    """Seeded scored rows are a documented sample of seeded windows."""
    db.windows.insert_one(window(NOW - timedelta(minutes=5), "positive", 500, source="seed"))
    db.scored.insert_one(scored(NOW - timedelta(minutes=5), 1, source="seed"))
    check = Repository().reconciliation()
    assert check["windows_total"] == 0 and check["scored_total"] == 0


def test_health_reports_reconciliation_failure(api, db):
    _run(db)
    start = NOW - timedelta(minutes=10)
    db.scored.insert_one(scored(start, 1))
    db.windows.insert_one(window(start, "positive", 3, source="spark"))
    body = api.get("/api/health").json()
    components = {c["name"]: c for c in body["data"]}
    assert components["reconciliation"]["status"] == "error"
    assert any("reconciliation" in w for w in body["warnings"])

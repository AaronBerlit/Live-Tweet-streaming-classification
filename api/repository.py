"""Every MongoDB access in the application goes through this file.

PRD 9.1 backend rule: all Mongo access via `repository.py`, never inline in a
route. Routes translate HTTP to calls here and back; they never build a query.

Connection failures raise `RepositoryUnavailable`, which the API turns into a
200 with populated `warnings` and `data: null`. The dashboard degrades rather
than error-boundarying.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import PyMongoError

from pipeline.config import (
    COLL_BATCHES,
    COLL_HASHTAGS,
    COLL_HEARTBEAT,
    COLL_METRICS,
    COLL_RUNS,
    COLL_SCORED,
    COLL_WINDOWS,
    DB_NAME,
    HEARTBEAT_ID,
    settings,
)


#: A heartbeat younger than this means a micro-batch may still be writing.
STALLED_GRACE_SECONDS = 30


class RepositoryUnavailable(RuntimeError):
    """MongoDB could not be reached, or a query failed."""


def _encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, default=str).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_cursor(cursor: str) -> dict[str, Any]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
        return json.loads(raw)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError("malformed cursor") from exc


def _range_query(
    field: str, start: datetime | None, end: datetime | None
) -> dict[str, Any]:
    """Build an inclusive range filter, or an empty filter when unbounded."""
    if not start and not end:
        return {}
    bounds: dict[str, Any] = {}
    if start:
        bounds["$gte"] = start
    if end:
        bounds["$lte"] = end
    return {field: bounds}


class Repository:
    """Read-only access to the serving layer."""

    def __init__(self, uri: str | None = None, *, timeout_ms: int = 3000) -> None:
        self._client = MongoClient(
            uri or settings.mongo_uri,
            serverSelectionTimeoutMS=timeout_ms,
            connectTimeoutMS=timeout_ms,
            # Return every datetime as timezone-aware UTC. Without this,
            # aggregation results (e.g. $min/$max in summary()) come back
            # naive, serialise without a 'Z', and the browser parses them
            # as local time -- a silent 5h30m shift on this machine.
            tz_aware=True,
        )
        self._db = self._client[DB_NAME]

    def close(self) -> None:
        self._client.close()

    # --- liveness ------------------------------------------------------------

    def ping(self) -> str:
        """Server version, or raise. Used by /api/health."""
        try:
            return self._client.admin.command("buildInfo")["version"]
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

    def heartbeat(self) -> dict[str, Any] | None:
        """The single heartbeat document (PRD 7.5), or None if absent."""
        try:
            return self._db[COLL_HEARTBEAT].find_one({"_id": HEARTBEAT_ID})
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

    def batches(self, *, run_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        """Recent micro-batch samples, oldest first for charting.

        The frozen 7.5 `heartbeat` is one upserted document and so has no
        history; this is the series behind the Stream Monitor's latency and
        throughput charts and behind C22/C23.
        """
        query: dict[str, Any] = {}
        if run_id:
            query["run_id"] = run_id
        try:
            rows = list(
                self._db[COLL_BATCHES]
                .find(query)
                .sort([("batch_at", DESCENDING)])
                .limit(limit)
            )
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc
        return list(reversed(rows))

    def latest_window_end(self, *, source: str | None = None) -> datetime | None:
        """End of the newest window, the anchor for ranges outside LIVE mode."""
        query: dict[str, Any] = {"source": source} if source else {}
        try:
            newest = self._db[COLL_WINDOWS].find_one(
                query, {"window_end": 1}, sort=[("window_end", DESCENDING)]
            )
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc
        return newest["window_end"] if newest else None

    def window_presence(self) -> tuple[bool, bool]:
        """`(has_windows, has_spark_windows)` -- the inputs to mode derivation.

        Two cheap existence probes rather than counts: mode is derived on every
        request and must stay far cheaper than the data it labels.
        """
        try:
            collection = self._db[COLL_WINDOWS]
            has_any = collection.find_one({}, {"_id": 1}) is not None
            has_spark = collection.find_one({"source": "spark"}, {"_id": 1}) is not None
            return has_any, has_spark
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

    # --- windows -------------------------------------------------------------

    def windows(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        run_id: str | None = None,
        source: str | None = None,
        limit: int = 2000,
    ) -> list[dict[str, Any]]:
        """Window aggregates in a time range, oldest first for charting."""
        query = _range_query("window_start", start, end)
        if run_id:
            query["run_id"] = run_id
        if source:
            query["source"] = source
        try:
            return list(
                self._db[COLL_WINDOWS]
                .find(query)
                .sort([("window_start", ASCENDING), ("prediction", ASCENDING)])
                .limit(limit)
            )
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

    def windows_explain(
        self, *, start: datetime | None = None, end: datetime | None = None
    ) -> dict[str, Any]:
        """C20: the explain plan proving the range query uses its index."""
        try:
            return self._db.command(
                "explain",
                {
                    "find": COLL_WINDOWS,
                    "filter": _range_query("window_start", start, end),
                    "sort": {"window_start": -1, "prediction": 1},
                },
                verbosity="executionStats",
            )
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

    def summary(
        self, *, since: datetime | None = None, source: str | None = None
    ) -> dict[str, Any]:
        """KPI totals for /api/summary, aggregated server-side.

        Summing `windows` rather than counting `scored` is deliberate: the
        aggregates are the durable record, and `scored` is TTL'd after 24h
        (PRD 7.2), so counting it would under-report older ranges.
        """
        match = _range_query("window_start", since, None)
        # Never blend seeded and Spark-written documents into one figure: the
        # badge would say LIVE while the number silently included seeded rows.
        if source:
            match["source"] = source
        stages: list[dict[str, Any]] = []
        if match:
            stages.append({"$match": match})

        group_stage = {
            "$group": {
                "_id": "$prediction",
                "count": {"$sum": "$count"},
                "weighted_confidence": {
                    "$sum": {"$multiply": ["$avg_confidence", "$count"]}
                },
            }
        }
        span_stage = {
            "$group": {
                "_id": None,
                "first": {"$min": "$window_start"},
                "last": {"$max": "$window_end"},
            }
        }

        try:
            groups = list(self._db[COLL_WINDOWS].aggregate(stages + [group_stage]))
            span = list(self._db[COLL_WINDOWS].aggregate(stages + [span_stage]))
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

        by_prediction = {row["_id"]: row["count"] for row in groups}
        total = sum(by_prediction.values())
        weighted = sum(row["weighted_confidence"] for row in groups)
        bounds = span[0] if span else {}
        return {
            "total_classified": total,
            "by_prediction": by_prediction,
            "avg_confidence": (weighted / total) if total else None,
            "window_start": bounds.get("first"),
            "window_end": bounds.get("last"),
        }

    # --- hashtags ------------------------------------------------------------

    def hashtags(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        source: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Top N tags in a range, with their sentiment split (C21)."""
        match = _range_query("window_start", start, end)
        if source:
            match["source"] = source
        stages: list[dict[str, Any]] = []
        if match:
            stages.append({"$match": match})
        stages += [
            {
                "$group": {
                    "_id": "$tag",
                    "count": {"$sum": "$count"},
                    "positive": {"$sum": "$sentiment_split.positive"},
                    "negative": {"$sum": "$sentiment_split.negative"},
                    "neutral": {"$sum": "$sentiment_split.neutral"},
                }
            },
            {"$sort": {"count": -1, "_id": 1}},
            {"$limit": limit},
        ]
        try:
            rows = list(self._db[COLL_HASHTAGS].aggregate(stages))
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc
        return [
            {
                "tag": row["_id"],
                "count": row["count"],
                "sentiment_split": {
                    "positive": row["positive"],
                    "negative": row["negative"],
                    "neutral": row["neutral"],
                },
            }
            for row in rows
        ]

    # --- scored --------------------------------------------------------------

    def scored(
        self,
        *,
        window_start: datetime | None = None,
        prediction: str | None = None,
        query_text: str | None = None,
        source: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Paginated raw records, newest first.

        Keyset pagination on `(event_time, _id)`. Skip/limit would duplicate
        and drop rows as new records arrive mid-scroll; the compound key keeps
        page boundaries exact even when many records share a timestamp
        (12.1).
        """
        query: dict[str, Any] = {}
        if window_start is not None:
            # A record belongs to the one-minute window it falls in.
            query["event_time"] = {
                "$gte": window_start,
                "$lt": window_start + timedelta(minutes=1),
            }
        if prediction:
            query["prediction"] = prediction
        if source:
            query["source"] = source
        if query_text:
            # Tweet text is untrusted input. Escaping it keeps a crafted query
            # from reaching the regex engine as a pattern.
            query["text_raw"] = {"$regex": re.escape(query_text), "$options": "i"}

        if cursor:
            position = _decode_cursor(cursor)
            after = datetime.fromisoformat(position["event_time"])
            if after.tzinfo is None:
                after = after.replace(tzinfo=timezone.utc)
            query["$and"] = [
                {
                    "$or": [
                        {"event_time": {"$lt": after}},
                        {
                            "event_time": after,
                            "_id": {"$lt": ObjectId(position["id"])},
                        },
                    ]
                }
            ]

        try:
            rows = list(
                self._db[COLL_SCORED]
                .find(query)
                .sort([("event_time", DESCENDING), ("_id", DESCENDING)])
                .limit(limit + 1)
            )
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

        next_cursor = None
        if len(rows) > limit:
            rows = rows[:limit]
            last = rows[-1]
            next_cursor = _encode_cursor(
                {"event_time": last["event_time"].isoformat(), "id": str(last["_id"])}
            )
        return rows, next_cursor

    def recent_scored(
        self, limit: int = 8, source: str | None = None
    ) -> list[dict[str, Any]]:
        """The live strip on the Overview page."""
        rows, _ = self.scored(limit=limit, source=source)
        return rows

    # --- metrics and runs ----------------------------------------------------

    def metrics(self) -> list[dict[str, Any]]:
        """Every evaluation document, newest first.

        Returns an empty list when nothing has been evaluated. The UI renders
        "not yet evaluated" rather than zeros (PRD rule 4).
        """
        try:
            return list(
                self._db[COLL_METRICS].find({}).sort([("trained_at", DESCENDING)])
            )
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

    def runs(self, limit: int = 50) -> list[dict[str, Any]]:
        try:
            return list(
                self._db[COLL_RUNS]
                .find({})
                .sort([("started_at", DESCENDING)])
                .limit(limit)
            )
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

    def latest_run(self) -> dict[str, Any] | None:
        runs = self.runs(limit=1)
        return runs[0] if runs else None

    # --- data correctness (PRD 12.5) ------------------------------------------

    def reconciliation(self, *, hours: int = 23) -> dict[str, Any]:
        """Check the serving layer against itself, for Spark-written data.

        * the sum of `windows.count` must equal the number of `scored`
          documents in the same range -- the stream derives one from the other,
          so any gap means a write was lost or doubled;
        * no window may be stored with `count <= 0` -- absence and zero differ;
        * every `scored.run_id` must exist in `runs`.

        Bounded to the last `hours` because `scored` expires after 24h while
        `windows` is durable; comparing beyond the TTL would report the expiry
        itself as a discrepancy. Seeded data is excluded: its `scored` rows are
        a documented sample of its windows, not an expansion of them.
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        try:
            # "Within watermark tolerance" (12.5): windows at or after the
            # current watermark are still being written by an in-flight batch,
            # so comparing them measures the moment of sampling, not the data.
            # Everything strictly before the watermark's minute is final; a
            # disagreement there is a real lost or doubled write.
            heartbeat = self._db[COLL_HEARTBEAT].find_one({"_id": HEARTBEAT_ID})
            cutoff = None
            # Only while a batch could be in flight. Once the stream is
            # stopped nothing is being written, every window is final, and a
            # frozen watermark would otherwise exclude the last run's final
            # minutes from the comparison forever.
            last_batch_at = heartbeat.get("last_batch_at") if heartbeat else None
            in_flight = (
                last_batch_at is not None
                and (datetime.now(timezone.utc) - last_batch_at).total_seconds()
                < STALLED_GRACE_SECONDS
            )
            if in_flight and heartbeat.get("watermark"):
                cutoff = heartbeat["watermark"].replace(second=0, microsecond=0)
            window_range: dict[str, Any] = {"$gte": since}
            event_range: dict[str, Any] = {"$gte": since}
            if cutoff is not None:
                window_range["$lt"] = cutoff
                event_range["$lt"] = cutoff

            windows_total = next(
                iter(
                    self._db[COLL_WINDOWS].aggregate(
                        [
                            {"$match": {"source": "spark", "window_start": window_range}},
                            {"$group": {"_id": None, "n": {"$sum": "$count"}}},
                        ]
                    )
                ),
                {"n": 0},
            )["n"]
            scored_total = self._db[COLL_SCORED].count_documents(
                {"source": "spark", "event_time": event_range}
            )
            zero_windows = self._db[COLL_WINDOWS].count_documents({"count": {"$lte": 0}})
            scored_runs = set(self._db[COLL_SCORED].distinct("run_id", {"source": "spark"}))
            known_runs = set(self._db[COLL_RUNS].distinct("_id"))
        except PyMongoError as exc:
            raise RepositoryUnavailable(str(exc)) from exc

        orphans = sorted(scored_runs - known_runs)
        return {
            "range_hours": hours,
            "compared_before": cutoff,
            "windows_total": windows_total,
            "scored_total": scored_total,
            "difference": windows_total - scored_total,
            "zero_count_windows": zero_windows,
            "orphan_run_ids": orphans,
            "consistent": windows_total == scored_total and zero_windows == 0 and not orphans,
        }

    # --- storage introspection ----------------------------------------------

    def collection_stats(self) -> list[dict[str, Any]]:
        """C19: document counts and sizes, proving Mongo holds aggregates only."""
        names = (
            COLL_WINDOWS,
            COLL_SCORED,
            COLL_HASHTAGS,
            COLL_METRICS,
            COLL_HEARTBEAT,
            COLL_RUNS,
            COLL_BATCHES,
        )
        out = []
        for name in names:
            try:
                stats = self._db.command("collStats", name)
            except PyMongoError:
                # A collection that does not exist yet reports as empty rather
                # than failing the whole page.
                stats = {}
            out.append(
                {
                    "collection": name,
                    "documents": stats.get("count", 0),
                    "size_bytes": stats.get("size", 0),
                    "storage_bytes": stats.get("storageSize", 0),
                    "indexes": stats.get("nindexes", 0),
                    "index_bytes": stats.get("totalIndexSize", 0),
                }
            )
        return out

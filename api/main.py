"""FastAPI backend -- REST for history, WebSocket for live push (PRD 9.1).

Rules this file holds to:

*   Every Mongo access goes through `repository.py`. No route builds a query.
*   Timestamps are timezone-aware UTC internally. Conversion to Asia/Kolkata
    happens at render time in the frontend, not here.
*   A connection failure returns **200** with populated `warnings` and
    `data: null`. The dashboard degrades; it does not error-boundary.
*   `/api/health` never returns 5xx. A health endpoint that 500s when the
    thing it monitors is down is useless precisely when it is needed.
*   Every response is a Pydantic model. No bare dicts.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, Query, WebSocket
from fastapi.middleware.cors import CORSMiddleware

from api import mode as mode_module
from api import ws as ws_module
from api.models import ComponentHealth, Envelope, HealthStatus, Mode
from api.repository import Repository, RepositoryUnavailable
from pipeline import versions
from pipeline.config import settings
from storage import hdfs

state: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["repository"] = Repository()
    yield
    state["repository"].close()


app = FastAPI(
    title="Sentiment Stream API",
    version="1.0.0",
    description="Serving layer for the real-time sentiment analytics pipeline.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    # The Vite dev server. Not a production CORS policy, and not claimed to be.
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def repository() -> Repository:
    # Created on first use as well as at startup: a serverless host (the Vercel
    # deployment) may not run the lifespan hook before the first request.
    if "repository" not in state:
        state["repository"] = Repository()
    return state["repository"]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _current_mode() -> tuple[Mode, float | None, list[str]]:
    """Derive the mode, tolerating a dead database."""
    try:
        heartbeat = repository().heartbeat()
        has_windows, has_spark = repository().window_presence()
    except RepositoryUnavailable as exc:
        return Mode.EMPTY, None, [f"MongoDB unavailable: {exc}"]

    last_batch_at = heartbeat.get("last_batch_at") if heartbeat else None
    age = mode_module.heartbeat_age_seconds(last_batch_at)
    derived = mode_module.derive_mode(
        last_batch_at=last_batch_at,
        has_windows=has_windows,
        has_spark_windows=has_spark,
    )
    return derived, age, []


def _authoritative_source() -> str | None:
    """Which `source` the dashboard should read, so nothing is ever blended.

    Once a Spark run has written windows, seeded documents stop contributing
    to any figure -- otherwise the KPI row would sum seeded and real counts
    while the badge read LIVE, which is exactly the misrepresentation G6
    forbids. Returns None only when the database cannot be reached.
    """
    try:
        has_windows, has_spark = repository().window_presence()
    except RepositoryUnavailable:
        return None
    if has_spark:
        return "spark"
    return "seed" if has_windows else None


def envelope(data: Any, warnings: list[str] | None = None) -> Envelope:
    derived, _, mode_warnings = _current_mode()
    return Envelope(
        mode=derived,
        server_time=utcnow(),
        data=data,
        warnings=(warnings or []) + mode_warnings,
    )


def _degraded(exc: RepositoryUnavailable) -> Envelope:
    """A failed query returns 200 with a warning and no data."""
    return Envelope(
        mode=Mode.EMPTY,
        server_time=utcnow(),
        data=None,
        warnings=[f"MongoDB unavailable: {exc}"],
    )


_LAKE_PROBE: dict[str, Any] = {"at": 0.0, "value": None}
LAKE_PROBE_TTL_SECONDS = 15.0


def _lake_reachable() -> bool:
    """Probe the lake, at most once every 15 seconds.

    Under the HDFS backend this shells out to `docker exec`, which takes a
    second or two. Doing it on every health poll made the whole endpoint slow
    enough to delay the STALLED badge. The lake's reachability does not change
    on a sub-15s timescale, so a short cache loses nothing.
    """
    import time

    now = time.monotonic()
    if _LAKE_PROBE["value"] is None or now - _LAKE_PROBE["at"] > LAKE_PROBE_TTL_SECONDS:
        _LAKE_PROBE["value"] = hdfs.exists(hdfs.ROOT)
        _LAKE_PROBE["at"] = now
    return bool(_LAKE_PROBE["value"])


# --- health and mode ---------------------------------------------------------


@app.get("/api/health", response_model=Envelope)
def health() -> Envelope:
    """Per-component status. Never 5xx, by construction."""
    components: list[ComponentHealth] = []
    warnings: list[str] = []

    try:
        version = repository().ping()
        components.append(
            ComponentHealth(
                name="mongo",
                status=HealthStatus.OK,
                detail=f"MongoDB {version}",
                last_contact=utcnow(),
                start_command="docker compose up -d mongo",
            )
        )
        mongo_up = True
    except RepositoryUnavailable as exc:
        components.append(
            ComponentHealth(
                name="mongo",
                status=HealthStatus.ERROR,
                detail=str(exc),
                start_command="docker compose up -d mongo",
            )
        )
        warnings.append("MongoDB is unreachable; the dashboard is showing no data.")
        mongo_up = False

    if mongo_up:
        # Mongo can die between the ping above and this read. A health
        # endpoint that raises when the thing it monitors goes down is useless
        # exactly when it is needed, so this is guarded too.
        try:
            heartbeat = repository().heartbeat()
        except RepositoryUnavailable as exc:
            heartbeat = None
            warnings.append(f"heartbeat unreadable: {exc}")
        last_batch_at = heartbeat.get("last_batch_at") if heartbeat else None
        stream_status = mode_module.derive_stream_health(last_batch_at)
        detail = "no heartbeat recorded"
        if heartbeat:
            age = mode_module.heartbeat_age_seconds(last_batch_at)
            detail = (
                f"batch {heartbeat.get('last_batch_id')} "
                f"{age:.0f}s ago, {heartbeat.get('rows_per_sec')} rec/s"
            )
        components.append(
            ComponentHealth(
                name="spark_stream",
                status=stream_status,
                detail=detail,
                last_contact=last_batch_at,
                start_command="make demo-socket",
            )
        )
    else:
        components.append(
            ComponentHealth(
                name="spark_stream",
                status=HealthStatus.UNKNOWN,
                detail="cannot determine without MongoDB",
                start_command="make demo-socket",
            )
        )

    # PRD 12.5: divergence between windows and scored is surfaced, not hidden.
    if mongo_up:
        try:
            check = repository().reconciliation()
            if check["windows_total"] == 0 and check["scored_total"] == 0:
                status, detail = HealthStatus.UNKNOWN, "no Spark-written data to reconcile yet"
            elif check["consistent"]:
                status = HealthStatus.OK
                detail = (
                    f"windows sum {check['windows_total']:,} = "
                    f"scored {check['scored_total']:,}; no zero-count windows; no orphan runs"
                )
            else:
                status = HealthStatus.ERROR
                detail = (
                    f"windows sum {check['windows_total']:,} vs scored "
                    f"{check['scored_total']:,} (diff {check['difference']:+,}); "
                    f"zero-count windows {check['zero_count_windows']}; "
                    f"orphan runs {check['orphan_run_ids'] or 'none'}"
                )
                warnings.append(f"data reconciliation failed: {detail}")
        except RepositoryUnavailable as exc:
            status, detail = HealthStatus.ERROR, str(exc)
        components.append(
            ComponentHealth(
                name="reconciliation",
                status=status,
                detail=detail,
                last_contact=utcnow(),
                start_command="python -m scripts.evidence.reconciliation",
            )
        )

    # Actually probe the lake rather than asserting it is fine. Reporting
    # `ok` without looking is the same class of defect as a fabricated metric.
    if settings.hosted_snapshot:
        components.append(
            ComponentHealth(
                name="storage",
                status=HealthStatus.UNKNOWN,
                detail="HDFS runs on the team laptop; this hosted copy serves a "
                "MongoDB snapshot only",
                start_command="make up (on the laptop)",
            )
        )
        return envelope([component.model_dump() for component in components], warnings)
    try:
        reachable = _lake_reachable()
        storage_status = HealthStatus.OK if reachable else HealthStatus.OFFLINE
        storage_detail = (
            f"backend={hdfs.backend()}, {hdfs.ROOT} "
            f"{'present' if reachable else 'not found'}"
        )
    except hdfs.StorageError as exc:
        storage_status = HealthStatus.ERROR
        storage_detail = str(exc)
    components.append(
        ComponentHealth(
            name="storage",
            status=storage_status,
            detail=storage_detail,
            last_contact=utcnow() if storage_status is HealthStatus.OK else None,
            start_command="docker compose -f docker-compose.hdfs.yml up -d",
        )
    )

    return envelope([component.model_dump() for component in components], warnings)


@app.get("/api/mode", response_model=Envelope)
def current_mode() -> Envelope:
    """The derived mode plus the reasoning behind it (G6)."""
    derived, age, warnings = _current_mode()
    return Envelope(
        mode=derived,
        server_time=utcnow(),
        data={
            "mode": derived.value,
            "reason": mode_module.explain_mode(derived, age),
            "heartbeat_age_seconds": age,
        },
        warnings=warnings,
    )


# --- data --------------------------------------------------------------------


def _time_anchor(source: str | None) -> datetime:
    """The instant that "last N hours" is measured back from.

    While the stream is LIVE that is now. Otherwise it is the end of the
    newest window: a range measured back from now would show nothing at all
    once a run is more than a day old -- an empty dashboard in REPLAY mode, or
    on the hosted snapshot, the day after the data was produced.
    """
    derived, _, _ = _current_mode()
    if derived is Mode.LIVE:
        return utcnow()
    return repository().latest_window_end(source=source) or utcnow()


@app.get("/api/summary", response_model=Envelope)
def summary(hours: int = Query(default=6, ge=1, le=168)) -> Envelope:
    try:
        source = _authoritative_source()
        since = _time_anchor(source) - timedelta(hours=hours)
        return envelope(repository().summary(since=since, source=source))
    except RepositoryUnavailable as exc:
        return _degraded(exc)


@app.get("/api/windows", response_model=Envelope)
def windows(
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = Query(default=None),
    hours: int | None = Query(default=None, ge=1, le=168),
    run_id: str | None = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=10_000),
) -> Envelope:
    try:
        source = _authoritative_source()
        if from_ is None and hours is not None:
            from_ = _time_anchor(source) - timedelta(hours=hours)
        rows = repository().windows(
            start=from_,
            end=to,
            run_id=run_id,
            source=source,
            limit=limit,
        )
        return envelope([_serialise(row) for row in rows])
    except RepositoryUnavailable as exc:
        return _degraded(exc)


@app.get("/api/hashtags", response_model=Envelope)
def hashtags(
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = Query(default=None),
    hours: int | None = Query(default=None, ge=1, le=168),
    limit: int = Query(default=20, ge=1, le=100),
) -> Envelope:
    try:
        source = _authoritative_source()
        if from_ is None and hours is not None:
            from_ = _time_anchor(source) - timedelta(hours=hours)
        return envelope(
            repository().hashtags(start=from_, end=to, source=source, limit=limit)
        )
    except RepositoryUnavailable as exc:
        return _degraded(exc)


@app.get("/api/scored", response_model=Envelope)
def scored(
    window_start: datetime | None = Query(default=None),
    prediction: str | None = Query(default=None),
    q: str | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> Envelope:
    try:
        rows, next_cursor = repository().scored(
            window_start=window_start,
            prediction=prediction,
            query_text=q,
            source=_authoritative_source(),
            cursor=cursor,
            limit=limit,
        )
    except RepositoryUnavailable as exc:
        return _degraded(exc)
    except ValueError as exc:
        return Envelope(
            mode=Mode.EMPTY, server_time=utcnow(), data=None, warnings=[str(exc)]
        )
    return envelope(
        {"records": [_serialise(row) for row in rows], "next_cursor": next_cursor}
    )


@app.get("/api/stream", response_model=Envelope)
def stream_state(
    run_id: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> Envelope:
    """Current heartbeat plus the micro-batch history (C17, C22, C23)."""
    try:
        heartbeat = repository().heartbeat()
        batches = repository().batches(run_id=run_id, limit=limit)
        latest_run = repository().latest_run()
    except RepositoryUnavailable as exc:
        return _degraded(exc)

    return envelope(
        {
            "heartbeat": _serialise(heartbeat) if heartbeat else None,
            "batches": [_serialise(row) for row in batches],
            "run": _serialise(latest_run) if latest_run else None,
        }
    )


@app.get("/api/reconciliation", response_model=Envelope)
def reconciliation(hours: int = Query(default=23, ge=1, le=23)) -> Envelope:
    """PRD 12.5 data-correctness check over Spark-written documents."""
    try:
        return envelope(repository().reconciliation(hours=hours))
    except RepositoryUnavailable as exc:
        return _degraded(exc)


@app.get("/api/metrics", response_model=Envelope)
def metrics() -> Envelope:
    """Every evaluation document.

    An empty list means no model has been evaluated. The UI must render
    "not yet evaluated" -- never zeros (PRD rule 4).
    """
    try:
        return envelope([_serialise(row) for row in repository().metrics()])
    except RepositoryUnavailable as exc:
        return _degraded(exc)


@app.get("/api/runs", response_model=Envelope)
def runs(limit: int = Query(default=50, ge=1, le=200)) -> Envelope:
    try:
        return envelope([_serialise(row) for row in repository().runs(limit=limit)])
    except RepositoryUnavailable as exc:
        return _degraded(exc)


@app.get("/api/pipeline/config", response_model=Envelope)
def pipeline_config() -> Envelope:
    """Live configuration: Spark settings, storage backend, pinned versions."""
    try:
        latest_run = repository().latest_run()
        collections = repository().collection_stats()
    except RepositoryUnavailable as exc:
        return _degraded(exc)

    return envelope(
        {
            "versions": versions.version_report(),
            "storage": hdfs.describe(),
            "spark": {
                "master": settings.spark_master,
                "driver_memory": settings.spark_driver_memory,
            },
            "mongo_uri": settings.mongo_uri,
            "kafka_bootstrap": settings.kafka_bootstrap,
            "display_timezone": settings.display_timezone,
            "latest_run": _serialise(latest_run) if latest_run else None,
            "collections": collections,
        }
    )


@app.get("/api/storage", response_model=Envelope)
def storage() -> Envelope:
    """C4/C19: lake listing plus MongoDB collection sizes, side by side."""
    try:
        collections = repository().collection_stats()
    except RepositoryUnavailable as exc:
        return _degraded(exc)

    if settings.hosted_snapshot:
        entries = []
        lake_warning = [
            "Hosted snapshot: the HDFS data lake runs on the team laptop and is "
            "not reachable from here. Lake sizes are in docs/evidence/hdfs_listing.txt."
        ]
    else:
        try:
            entries = [
                {"path": entry.path, "size_bytes": entry.size_bytes, "is_dir": entry.is_dir}
                for entry in hdfs.listing()
            ]
            lake_warning = []
        except hdfs.StorageError as exc:
            entries = []
            lake_warning = [f"data lake unavailable: {exc}"]

    return envelope(
        {
            "backend": hdfs.describe(),
            "lake": entries,
            "lake_total_bytes": sum(entry["size_bytes"] for entry in entries),
            "collections": collections,
        },
        lake_warning,
    )


@app.websocket("/ws/stream")
async def stream(websocket: WebSocket) -> None:
    """Push `heartbeat`, `window_update` and `mode_change` notifications."""
    await ws_module.stream_endpoint(websocket, repository())


def _serialise(document: dict[str, Any] | None) -> dict[str, Any] | None:
    """Make a Mongo document JSON-safe without reshaping it.

    ObjectId becomes a string and naive datetimes are tagged UTC. Field names
    are left exactly as the frozen contracts define them.
    """
    if document is None:
        return None
    out: dict[str, Any] = {}
    for key, value in document.items():
        if isinstance(value, datetime) and value.tzinfo is None:
            out[key] = value.replace(tzinfo=timezone.utc)
        elif isinstance(value, dict):
            out[key] = _serialise(value)
        elif key == "_id" and not isinstance(value, str):
            out[key] = str(value)
        else:
            out[key] = value
    return out

"""Source-mode and health derivation (PRD §9.1, G6).

The mode badge is always visible and always *derived*. It is never configured,
because a configured badge is a badge that can lie about pipeline state -- and
G6 says the interface must never misrepresent it.

The functions here take plain values rather than reaching into MongoDB, so the
four branches and their exact boundaries (15s, 10min) are unit-testable
without a database.
"""

from __future__ import annotations

from datetime import datetime, timezone

from api.models import HealthStatus, Mode
from pipeline.config import LIVE_MAX_AGE_SECONDS, STALLED_MAX_AGE_SECONDS


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def heartbeat_age_seconds(last_batch_at: datetime | None, *, now: datetime | None = None) -> float | None:
    """Seconds since the last micro-batch, or None if there is no heartbeat."""
    if last_batch_at is None:
        return None
    now = now or utcnow()
    if last_batch_at.tzinfo is None:
        last_batch_at = last_batch_at.replace(tzinfo=timezone.utc)
    return (now - last_batch_at).total_seconds()


def derive_mode(
    *,
    last_batch_at: datetime | None,
    has_windows: bool,
    has_spark_windows: bool,
    now: datetime | None = None,
) -> Mode:
    """The four-branch derivation, in the PRD's exact order.

        heartbeat age < 15s                       -> LIVE
        windows non-empty and any source=="spark" -> REPLAY
        windows non-empty                         -> SEED
        otherwise                                 -> EMPTY

    REPLAY is what keeps the viva safe: a previous run renders fully even if
    Spark is not running (PRD §13, "demo machine stalls during viva").
    """
    age = heartbeat_age_seconds(last_batch_at, now=now)
    if age is not None and age < LIVE_MAX_AGE_SECONDS:
        return Mode.LIVE
    if has_windows and has_spark_windows:
        return Mode.REPLAY
    if has_windows:
        return Mode.SEED
    return Mode.EMPTY


def derive_stream_health(
    last_batch_at: datetime | None, *, now: datetime | None = None
) -> HealthStatus:
    """Status of the Spark streaming job from its heartbeat alone.

        age < 15s     -> ok
        age < 10min   -> stalled
        older         -> offline
        no heartbeat  -> unknown
    """
    age = heartbeat_age_seconds(last_batch_at, now=now)
    if age is None:
        return HealthStatus.UNKNOWN
    if age < LIVE_MAX_AGE_SECONDS:
        return HealthStatus.OK
    if age < STALLED_MAX_AGE_SECONDS:
        return HealthStatus.STALLED
    return HealthStatus.OFFLINE


def explain_mode(mode: Mode, age: float | None) -> str:
    """Human-readable reasoning, returned by /api/mode.

    The UI shows this so the badge is never a bare assertion the user has to
    take on trust.
    """
    if mode is Mode.LIVE:
        return f"Spark reported a micro-batch {age:.1f}s ago (threshold {LIVE_MAX_AGE_SECONDS}s)."
    if mode is Mode.REPLAY:
        if age is None:
            return "Windows written by Spark exist, but no heartbeat -- the stream is not running."
        return (
            f"Windows written by Spark exist, but the last heartbeat was "
            f"{age:.0f}s ago (threshold {LIVE_MAX_AGE_SECONDS}s)."
        )
    if mode is Mode.SEED:
        return "All windows came from scripts/seed.py. No Spark run has written to this database."
    return "No windows in MongoDB. Run `make demo-seed` or start the pipeline."

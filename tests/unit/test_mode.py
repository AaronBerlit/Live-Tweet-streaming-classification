"""Mode and health derivation across all four branches plus exact boundaries.

PRD 12.1 calls for "mode derivation across all four branches plus exact
boundaries at 15s and 10min". The boundaries are the interesting part: an
off-by-one there means the badge claims LIVE for a dead pipeline, which is
exactly what G6 forbids.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from api.mode import derive_mode, derive_stream_health, heartbeat_age_seconds
from api.models import HealthStatus, Mode

NOW = datetime(2026, 9, 22, 14, 30, 0, tzinfo=timezone.utc)


def at(seconds_ago: float) -> datetime:
    return NOW - timedelta(seconds=seconds_ago)


# --- the four branches -------------------------------------------------------


def test_live_when_heartbeat_is_fresh():
    assert (
        derive_mode(
            last_batch_at=at(3), has_windows=True, has_spark_windows=True, now=NOW
        )
        is Mode.LIVE
    )


def test_live_wins_even_without_windows():
    """A stream that has started but not yet closed a window is still LIVE."""
    assert (
        derive_mode(
            last_batch_at=at(1), has_windows=False, has_spark_windows=False, now=NOW
        )
        is Mode.LIVE
    )


def test_replay_when_spark_windows_exist_but_heartbeat_is_stale():
    """The viva safety net: a finished run renders fully with Spark stopped."""
    assert (
        derive_mode(
            last_batch_at=at(600), has_windows=True, has_spark_windows=True, now=NOW
        )
        is Mode.REPLAY
    )


def test_replay_with_no_heartbeat_at_all():
    assert (
        derive_mode(
            last_batch_at=None, has_windows=True, has_spark_windows=True, now=NOW
        )
        is Mode.REPLAY
    )


def test_seed_when_only_seeded_windows_exist():
    assert (
        derive_mode(
            last_batch_at=None, has_windows=True, has_spark_windows=False, now=NOW
        )
        is Mode.SEED
    )


def test_empty_when_nothing_exists():
    assert (
        derive_mode(
            last_batch_at=None, has_windows=False, has_spark_windows=False, now=NOW
        )
        is Mode.EMPTY
    )


# --- the 15s boundary --------------------------------------------------------


@pytest.mark.parametrize("age", [0.0, 1.0, 14.0, 14.999])
def test_below_fifteen_seconds_is_live(age):
    assert (
        derive_mode(
            last_batch_at=at(age), has_windows=True, has_spark_windows=True, now=NOW
        )
        is Mode.LIVE
    )


@pytest.mark.parametrize("age", [15.0, 15.001, 30.0])
def test_at_or_above_fifteen_seconds_is_not_live(age):
    """15s exactly is NOT live -- the threshold is strict."""
    assert (
        derive_mode(
            last_batch_at=at(age), has_windows=True, has_spark_windows=True, now=NOW
        )
        is Mode.REPLAY
    )


# --- health statuses ---------------------------------------------------------


def test_health_unknown_without_heartbeat():
    assert derive_stream_health(None, now=NOW) is HealthStatus.UNKNOWN


@pytest.mark.parametrize("age", [0.0, 14.999])
def test_health_ok_below_fifteen_seconds(age):
    assert derive_stream_health(at(age), now=NOW) is HealthStatus.OK


@pytest.mark.parametrize("age", [15.0, 60.0, 599.999])
def test_health_stalled_between_fifteen_seconds_and_ten_minutes(age):
    """Killing the producer must show STALLED within 15s (DoD check 6)."""
    assert derive_stream_health(at(age), now=NOW) is HealthStatus.STALLED


@pytest.mark.parametrize("age", [600.0, 601.0, 86_400.0])
def test_health_offline_at_or_beyond_ten_minutes(age):
    assert derive_stream_health(at(age), now=NOW) is HealthStatus.OFFLINE


# --- age helper --------------------------------------------------------------


def test_age_is_none_without_heartbeat():
    assert heartbeat_age_seconds(None, now=NOW) is None


def test_naive_timestamps_are_treated_as_utc():
    """Mongo can hand back naive datetimes; they must not shift by the local
    offset, which on this machine would be 5.5 hours of silent error."""
    naive = NOW.replace(tzinfo=None) - timedelta(seconds=10)
    assert heartbeat_age_seconds(naive, now=NOW) == pytest.approx(10.0)


def test_future_heartbeat_yields_negative_age_and_still_reads_live():
    """Clock skew between the Spark host and the API must not read as offline."""
    assert heartbeat_age_seconds(at(-5), now=NOW) == pytest.approx(-5.0)
    assert derive_stream_health(at(-5), now=NOW) is HealthStatus.OK

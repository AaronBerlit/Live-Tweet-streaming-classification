"""WebSocket push for dashboard freshness (PRD §9.1 `/ws/stream`).

Design note: the server pushes *notifications*, not data. A client that
receives `{"type": "heartbeat"}` re-fetches through REST. Pushing payloads
would mean the UI's state depends on having received every message, and a
dropped frame during a reconnect would leave the dashboard silently wrong --
which is the one thing G6 forbids.

Polling MongoDB rather than using change streams: change streams require a
replica set, and this deployment runs a standalone mongod. A 2s poll of one
small document is cheap, and the 3s push requirement in §12.2 is met with
room to spare.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime

from fastapi import WebSocket, WebSocketDisconnect

from api import mode as mode_module
from api.repository import Repository, RepositoryUnavailable

POLL_SECONDS = 2.0


class Watcher:
    """Tracks what changed since the last poll, so pushes mean something."""

    def __init__(self) -> None:
        self.last_batch_id: int | None = None
        self.last_mode: str | None = None
        self.last_window_update: datetime | None = None

    def diff(self, repository: Repository) -> list[dict[str, str]]:
        """Return the events this poll should emit."""
        events: list[dict[str, str]] = []

        try:
            heartbeat = repository.heartbeat()
            has_windows, has_spark = repository.window_presence()
        except RepositoryUnavailable:
            # A dead database is not a reason to drop the socket; the client
            # will see the degraded REST responses and render accordingly.
            return events

        last_batch_at = heartbeat.get("last_batch_at") if heartbeat else None

        if heartbeat:
            batch_id = heartbeat.get("last_batch_id")
            if batch_id != self.last_batch_id:
                self.last_batch_id = batch_id
                events.append({"type": "heartbeat"})
                events.append({"type": "window_update"})

        derived = mode_module.derive_mode(
            last_batch_at=last_batch_at,
            has_windows=has_windows,
            has_spark_windows=has_spark,
        ).value
        if derived != self.last_mode:
            self.last_mode = derived
            events.append({"type": "mode_change"})

        return events


async def stream_endpoint(websocket: WebSocket, repository: Repository) -> None:
    """Push change notifications until the client goes away."""
    await websocket.accept()
    watcher = Watcher()

    # Tell the client its current mode immediately, so it does not have to
    # wait a poll interval to render the right badge.
    with contextlib.suppress(RepositoryUnavailable):
        for event in watcher.diff(repository):
            await websocket.send_text(json.dumps(event))

    try:
        while True:
            await asyncio.sleep(POLL_SECONDS)
            for event in watcher.diff(repository):
                await websocket.send_text(json.dumps(event))
    except WebSocketDisconnect:
        return
    except (RuntimeError, ConnectionError):
        # The socket died mid-send; the client reconnects on its own.
        return

"""Block until Mongo, Kafka and (if selected) HDFS answer, or report why not.

`make setup` runs this before touching either service, so a slow container
start reads as "waiting" rather than as a connection-refused stack trace.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
from urllib.parse import urlparse

from pipeline.config import settings
from storage import hdfs


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait(label: str, check, timeout: int = 120) -> bool:
    deadline = time.monotonic() + timeout
    print(f"waiting for {label} ", end="", flush=True)
    while time.monotonic() < deadline:
        if check():
            print(" ok")
            return True
        print(".", end="", flush=True)
        time.sleep(3)
    print(" TIMEOUT")
    return False


def _mongo_ready() -> bool:
    parsed = urlparse(settings.mongo_uri)
    return _port_open(parsed.hostname or "localhost", parsed.port or 27017)


def _kafka_ready() -> bool:
    host, _, port = settings.kafka_bootstrap.partition(":")
    return _port_open(host, int(port or 9092))


def _hdfs_ready() -> bool:
    """The namenode is only useful once it has left safe mode."""
    try:
        result = subprocess.run(
            [hdfs.docker_binary(), "exec", hdfs.NAMENODE_CONTAINER, "hdfs", "dfsadmin", "-safemode", "get"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0 and "OFF" in result.stdout


def main() -> int:
    failures = []
    if not _wait("mongo", _mongo_ready):
        failures.append("mongo -- docker compose up -d mongo")
    if not _wait("kafka", _kafka_ready):
        failures.append("kafka -- docker compose up -d kafka")
    if settings.storage_backend == "hdfs":
        if not _wait("hdfs namenode (leaving safe mode)", _hdfs_ready, timeout=180):
            failures.append(
                "hdfs -- docker compose -f docker-compose.hdfs.yml up -d, "
                "or set STORAGE_BACKEND=local"
            )
    else:
        print("storage backend is 'local' -- skipping HDFS check")

    if failures:
        print("\nnot ready:", file=sys.stderr)
        for line in failures:
            print(f"  {line}", file=sys.stderr)
        return 1
    print("\nall services ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

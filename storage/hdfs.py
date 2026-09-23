"""Thin path abstraction over the data lake (PRD C4, §5.2).

``STORAGE_BACKEND`` selects ``hdfs`` or ``local``. The pipeline must run
correctly on ``local`` when HDFS is unavailable -- that is the demo safety
valve, and the Pipeline Health page displays which backend is active.

Every module asks this file for paths. Nothing else in the codebase builds a
lake URI by string concatenation.
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from pipeline.config import settings

#: Logical layout, identical under both backends (PRD §5.2).
ROOT = PurePosixPath("/sentiment")
RAW = ROOT / "raw"
CLEAN = ROOT / "clean"
TRAIN = ROOT / "train"
TEST = ROOT / "test"
MODELS = ROOT / "models"
CHECKPOINTS = ROOT / "checkpoints"

#: The container the hdfs CLI runs inside when the backend is `hdfs`.
NAMENODE_CONTAINER = "sentiment-namenode"


class StorageError(RuntimeError):
    """Raised when the configured backend cannot be reached or used."""


def backend() -> str:
    """``'hdfs'`` or ``'local'``."""
    return settings.storage_backend


def uri(path: PurePosixPath | str) -> str:
    """Resolve a logical lake path to a URI Spark can read and write.

    Under ``local`` the lake is rooted at ``LOCAL_DATA_ROOT`` and the leading
    ``/sentiment`` is stripped, so the same logical path works on both
    backends without any caller needing to know which is active.
    """
    path = PurePosixPath(str(path))
    if backend() == "hdfs":
        return f"{settings.hdfs_uri.rstrip('/')}{path}"
    relative = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
    resolved = Path(settings.local_data_root) / relative
    return resolved.resolve().as_uri().replace("file:///", "file:///")


def model_uri(model_key: str) -> str:
    """Persisted PipelineModel location for ``nb`` / ``lr``."""
    return uri(MODELS / model_key)


def checkpoint_uri(run_id: str) -> str:
    """Streaming checkpoint location for a run (PRD §8 step 12)."""
    return uri(CHECKPOINTS / run_id)


def ensure_local_root() -> None:
    """Create the local lake root. No-op under the hdfs backend."""
    if backend() == "local":
        Path(settings.local_data_root).mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Entry:
    """One listed path in the lake."""

    path: str
    size_bytes: int
    is_dir: bool


def docker_binary() -> str:
    """Locate the docker CLI.

    Spark runs inside WSL while Docker Desktop runs on the Windows side. If
    WSL integration is not enabled for this distro there is no `docker` on the
    WSL PATH, but the Windows executable is reachable through /mnt/c and WSL
    interop runs it fine. Falling back to it means the HDFS evidence commands
    work either way, without asking anyone to toggle a Docker Desktop setting.
    """
    # docker.exe first: inside WSL without Docker Desktop's WSL integration,
    # the first `docker` on PATH is Docker Desktop's Linux shim, which exits 1
    # with no output -- every existence check would then silently say "no".
    for candidate in ("docker.exe", "docker"):
        found = shutil.which(candidate)
        if found:
            return found
    windows_path = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
    if Path(windows_path).exists():
        return windows_path
    raise StorageError(
        "docker CLI not found on PATH. Start Docker Desktop, enable WSL "
        "integration for this distro, or set STORAGE_BACKEND=local."
    )


def _hdfs_cli(*args: str, check: bool = True) -> str:
    """Run an `hdfs dfs` command inside the namenode container."""
    cmd = [docker_binary(), "exec", NAMENODE_CONTAINER, "hdfs", "dfs", *args]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except FileNotFoundError as exc:
        raise StorageError(
            "docker is not on PATH, so the HDFS CLI cannot be reached. "
            "Start Docker Desktop, or set STORAGE_BACKEND=local."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise StorageError("hdfs CLI timed out after 120s") from exc
    if check and result.returncode != 0:
        raise StorageError(
            f"hdfs dfs {' '.join(args)} failed ({result.returncode}): "
            f"{result.stderr.strip()}"
        )
    return result.stdout


def listing(path: PurePosixPath | str = ROOT) -> list[Entry]:
    """Recursive listing of a lake path, backend-agnostic.

    This is what ``make evidence-hdfs`` and the Data & Storage page render.
    """
    path = PurePosixPath(str(path))
    if backend() == "hdfs":
        out = _hdfs_cli("-ls", "-R", str(path), check=False)
        entries: list[Entry] = []
        for line in out.splitlines():
            parts = line.split(maxsplit=7)
            if len(parts) < 8:
                continue
            entries.append(
                Entry(
                    path=parts[7],
                    size_bytes=int(parts[4]),
                    is_dir=parts[0].startswith("d"),
                )
            )
        return entries

    relative = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
    root = Path(settings.local_data_root) / relative
    if not root.exists():
        return []
    entries = []
    for item in sorted(root.rglob("*")):
        entries.append(
            Entry(
                path=str(ROOT / relative / item.relative_to(root)).replace("\\", "/"),
                size_bytes=item.stat().st_size if item.is_file() else 0,
                is_dir=item.is_dir(),
            )
        )
    return entries


def total_size(path: PurePosixPath | str) -> int:
    """Bytes stored under a lake path."""
    return sum(entry.size_bytes for entry in listing(path))


def exists(path: PurePosixPath | str) -> bool:
    """Whether a lake path exists."""
    path = PurePosixPath(str(path))
    if backend() == "hdfs":
        cmd = [docker_binary(), "exec", NAMENODE_CONTAINER, "hdfs", "dfs", "-test", "-e", str(path)]
        try:
            return subprocess.run(cmd, capture_output=True, timeout=60).returncode == 0
        except (OSError, subprocess.SubprocessError):
            return False
    relative = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
    return (Path(settings.local_data_root) / relative).exists()


def describe() -> dict[str, str]:
    """Backend summary for the Pipeline Health page."""
    return {
        "backend": backend(),
        "root": settings.hdfs_uri if backend() == "hdfs" else settings.local_data_root,
        "replication": "1" if backend() == "hdfs" else "n/a",
    }

"""Central configuration, read once from the environment.

Every default here is sized for the target machine in PRD §0 rule 6:
16 GB RAM, ``local[2]``, limited disk. Nothing in this codebase assumes a
cluster, and nothing defaults to the full 1.6M-row corpus.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:  # python-dotenv is a convenience, not a requirement
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:  # pragma: no cover
    pass

REPO_ROOT = Path(__file__).resolve().parent.parent

# --- database ----------------------------------------------------------------

#: Overridable so the test suite runs against its own database and can never
#: touch the data the dashboard is showing.
DB_NAME = os.environ.get("MONGO_DB", "sentiment_stream")

COLL_WINDOWS = "windows"
COLL_SCORED = "scored"
COLL_HASHTAGS = "hashtags"
COLL_METRICS = "metrics"
COLL_HEARTBEAT = "heartbeat"
COLL_RUNS = "runs"
#: Append-only micro-batch history. The frozen 7.5 `heartbeat` is a single
#: upserted document and therefore has no history, but C22/C23 evidence and
#: the Stream Monitor both need the series. Additive: no frozen contract is
#: changed. Recorded in docs/LIMITATIONS.md.
COLL_BATCHES = "batches"

#: The single heartbeat document's _id (PRD §7.5).
HEARTBEAT_ID = "spark_stream"

#: PRD §7.2 -- the mitigation for uncontrolled `scored` growth. Do not omit.
SCORED_TTL_SECONDS = 24 * 60 * 60

#: Batch history is diagnostic, not durable -- bounded like `scored`.
BATCHES_TTL_SECONDS = 24 * 60 * 60

# --- mode derivation thresholds (PRD §9.1) -----------------------------------

LIVE_MAX_AGE_SECONDS = 15
STALLED_MAX_AGE_SECONDS = 10 * 60

# --- defaults ----------------------------------------------------------------

DEFAULT_TRAIN_ROWS = 200_000
DEFAULT_NUM_FEATURES = 65_536
DEFAULT_SEED = 42
DEFAULT_TRIGGER_SECONDS = 5
DEFAULT_WINDOW_MINUTES = 1
DEFAULT_WATERMARK_MINUTES = 2
DEFAULT_REPLAY_RATE = 200
SOCKET_HOST = "localhost"
SOCKET_PORT = 9999
KAFKA_TOPIC = "tweets.raw"

EMOJI_STRATEGIES = ("strip", "keep", "map")
NEUTRAL_STRATEGIES = ("none", "threshold", "external")
MODEL_NAMES = {"nb": "naive_bayes", "lr": "logistic_regression"}


def _env(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    """Resolved runtime configuration."""

    mongo_uri: str = field(
        default_factory=lambda: _env("MONGO_URI", "mongodb://localhost:27017")
    )
    kafka_bootstrap: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092")
    )
    storage_backend: str = field(
        default_factory=lambda: _env("STORAGE_BACKEND", "local").lower()
    )
    hdfs_uri: str = field(
        default_factory=lambda: _env("HDFS_URI", "hdfs://localhost:9000")
    )
    local_data_root: str = field(
        default_factory=lambda: _env("LOCAL_DATA_ROOT", str(REPO_ROOT / "data" / "lake"))
    )
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[2]"))
    spark_driver_memory: str = field(
        default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "4g")
    )
    api_host: str = field(default_factory=lambda: _env("API_HOST", "127.0.0.1"))
    api_port: int = field(default_factory=lambda: int(_env("API_PORT", "8000")))
    display_timezone: str = field(
        default_factory=lambda: _env("DISPLAY_TIMEZONE", "Asia/Kolkata")
    )

    def __post_init__(self) -> None:
        if self.storage_backend not in ("hdfs", "local"):
            raise ValueError(
                f"STORAGE_BACKEND must be 'hdfs' or 'local', got "
                f"{self.storage_backend!r}"
            )


settings = Settings()

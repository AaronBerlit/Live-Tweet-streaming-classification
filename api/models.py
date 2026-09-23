"""MongoDB data contracts -- FROZEN (PRD §7).

These are the contract between the streaming team and the application team.
Producers and consumers both conform to them exactly: no field is added,
renamed, or inferred. If a document does not validate against the model here,
the producer is wrong, not the model.

Every timestamp is timezone-aware UTC internally. Conversion to Asia/Kolkata
happens only at render time, in the frontend.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Prediction = Literal["positive", "negative", "neutral"]
Source = Literal["spark", "seed"]
InputSource = Literal["socket", "kafka"]


class Mode(str, Enum):
    """Derived source mode (PRD §9.1). Computed, never configured."""

    LIVE = "LIVE"
    REPLAY = "REPLAY"
    SEED = "SEED"
    EMPTY = "EMPTY"


class HealthStatus(str, Enum):
    OK = "ok"
    STALLED = "stalled"
    OFFLINE = "offline"
    ERROR = "error"
    UNKNOWN = "unknown"


class Contract(BaseModel):
    """Base for the six collections. Populated by alias so `_id` round-trips."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- §7.1 windows ------------------------------------------------------------


class Window(Contract):
    """One (window_start, prediction) aggregate.

    `_id` is deterministic -- `window_start|prediction` -- which is what makes
    the upsert idempotent under outputMode("update"). Restarting the stream
    re-emits windows; without this, counts double (PRD §4.2 Rule B).
    """

    id: str = Field(alias="_id")
    window_start: datetime
    window_end: datetime
    prediction: Prediction
    count: int = Field(ge=0)
    avg_confidence: float = Field(ge=0.0, le=1.0)
    run_id: str
    source: Source
    updated_at: datetime

    @staticmethod
    def make_id(window_start: datetime, prediction: str) -> str:
        """The deterministic _id. The only place this format is defined."""
        stamp = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"{stamp}|{prediction}"


# --- §7.2 scored -------------------------------------------------------------


class Scored(Contract):
    """One classified record.

    A TTL index on `ingest_time` expires these after 24h (see
    scripts/init_mongo.py). The aggregates in `windows` are the durable
    artifact; this collection is the drill-down.
    """

    id: Any | None = Field(default=None, alias="_id")
    text_raw: str
    text_clean: str
    prediction: Prediction
    confidence: float = Field(ge=0.0, le=1.0)
    hashtags: list[str]
    event_time: datetime
    ingest_time: datetime
    dedup_key: str
    run_id: str
    source: Source


# --- §7.3 hashtags -----------------------------------------------------------


class SentimentSplit(Contract):
    positive: int = Field(default=0, ge=0)
    negative: int = Field(default=0, ge=0)
    neutral: int = Field(default=0, ge=0)


class Hashtag(Contract):
    id: str = Field(alias="_id")
    window_start: datetime
    tag: str
    count: int = Field(ge=0)
    sentiment_split: SentimentSplit
    run_id: str
    source: Source

    @staticmethod
    def make_id(window_start: datetime, tag: str) -> str:
        stamp = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"{stamp}|{tag}"


# --- §7.4 metrics ------------------------------------------------------------


class FeatureConfig(Contract):
    vectorizer: str
    num_features: int
    idf: bool
    emoji_strategy: Literal["strip", "keep", "map"]


class Metrics(Contract):
    """One evaluation run. Appended, never overwritten, so history is visible.

    PRD §0 rule 4: never fabricate metrics. If no document exists, the API
    returns an empty array and the UI says "not yet evaluated". A placeholder
    accuracy number anywhere is a defect.
    """

    id: Any | None = Field(default=None, alias="_id")
    model_name: str
    trained_at: datetime
    train_rows: int = Field(ge=0)
    test_rows: int = Field(ge=0)
    accuracy: float = Field(ge=0.0, le=1.0)
    precision: dict[str, float]
    recall: dict[str, float]
    f1: dict[str, float]
    confusion_matrix: list[list[int]]
    feature_config: FeatureConfig
    neutral_strategy: Literal["none", "threshold", "external"]
    neutral_threshold: float | None = None
    train_duration_sec: float = Field(ge=0)
    notes: str = ""


# --- §7.5 heartbeat ----------------------------------------------------------


class Heartbeat(Contract):
    """Single document, upserted every micro-batch.

    This is what makes liveness detection possible -- it is not optional.
    `batch_duration_ms` and `rows_per_sec` are the real measurements behind
    C22 and C23.
    """

    id: str = Field(alias="_id")
    last_batch_id: int = Field(ge=0)
    last_batch_at: datetime
    rows_in_batch: int = Field(ge=0)
    batch_duration_ms: int = Field(ge=0)
    rows_per_sec: float = Field(ge=0)
    input_source: InputSource
    watermark: datetime | None = None
    late_records_dropped: int = Field(default=0, ge=0)
    run_id: str


# --- §7.6 runs ---------------------------------------------------------------


class SparkConfig(Contract):
    master: str
    trigger: str
    window: str
    watermark: str


class Run(Contract):
    id: str = Field(alias="_id")
    started_at: datetime
    ended_at: datetime | None = None
    model_id: Any | None = None
    input_source: InputSource
    replay_rate_per_sec: int = Field(ge=0)
    spark_config: SparkConfig
    storage_backend: Literal["hdfs", "local"]


# --- API response envelope (PRD §9.1) ---------------------------------------


class ComponentHealth(BaseModel):
    name: str
    status: HealthStatus
    detail: str | None = None
    last_contact: datetime | None = None
    start_command: str | None = None


class Envelope(BaseModel):
    """`{"mode": ..., "server_time": ..., "data": ..., "warnings": [...]}`

    A connection failure returns 200 with populated `warnings` and `data: null`
    -- the dashboard degrades rather than error-boundarying.
    """

    mode: Mode
    server_time: datetime
    data: Any | None = None
    warnings: Annotated[list[str], Field(default_factory=list)]

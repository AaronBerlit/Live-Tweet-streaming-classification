"""Populate MongoDB with seeded aggregates so the dashboard works with no
pipeline running (PRD Phase 3).

What this writes, per the PRD:
  - 6 hours of one-minute windows
  - 2000 scored records
  - 40 hashtags
  - NO heartbeat  (that absence is what makes the mode derive to SEED)

Every document carries `source: "seed"`, is written into real MongoDB by this
real script, and is read back by the dashboard through the real query path.
There is no code path where the frontend receives data MongoDB did not return
(PRD rule 5).

Two deliberate departures, both for honesty rather than convenience:

1.  **No metrics document.** PRD Phase 3 lists "one metrics doc", but rule 4
    says a placeholder accuracy number anywhere in this codebase is a defect,
    and the frozen 7.4 contract has no field that could mark a metrics
    document as fabricated. A seeded accuracy would be indistinguishable from
    a measured one on the Model page. So the Model page says "not yet
    evaluated" until `make train` writes a real evaluation.

2.  **No runs document.** `runs.input_source` is `socket|kafka`; a seed run is
    neither, and inventing one would put a fake pipeline run in the run
    history. The 12.5 invariant ("every scored.run_id exists in runs") is
    therefore scoped to `source == "spark"` records, which is what it is
    actually about.

Seeded `scored` records are a 2000-record *sample* of the seeded windows, not
a complete expansion of them -- the PRD asks for both numbers, and 2000 rows
is what the Raw Records page needs to be worth looking at.
"""

from __future__ import annotations

import argparse
import math
import hashlib
import random
import re
import sys
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

from api.models import Hashtag, Scored, SentimentSplit, Window
from pipeline.config import (
    COLL_HASHTAGS,
    COLL_HEARTBEAT,
    COLL_SCORED,
    COLL_WINDOWS,
    DB_NAME,
    settings,
)

SEED_RUN_PREFIX = "seed_"
HOURS = 6
WINDOW_MINUTES = 1
SCORED_RECORDS = 2000
HASHTAG_COUNT = 40

TOPICS = [
    "election", "results", "monsoon", "traffic", "cricket", "exams",
    "placements", "startup", "metro", "loadshedding", "heatwave", "festival",
    "concert", "launch", "update", "outage", "refund", "delivery",
    "customerservice", "flight", "delay", "hostel", "mess", "wifi",
    "internship", "semester", "project", "deadline", "coffee", "monday",
    "weekend", "rain", "power", "budget", "market", "rupee", "review",
    "bug", "release", "demo",
]

POSITIVE_TEMPLATES = [
    "finally got my {topic} sorted, genuinely impressed #{tag}",
    "the {topic} update is so much better than i expected #{tag}",
    "shoutout to whoever fixed the {topic} situation #{tag} 10/10",
    "honestly the best {topic} experience i've had in years #{tag}",
    "can't believe how smooth the {topic} was today #{tag}",
    "really happy with how the {topic} turned out #{tag}",
]

NEGATIVE_TEMPLATES = [
    "third day of {topic} problems and still no response #{tag}",
    "the {topic} is completely broken again, this is exhausting #{tag}",
    "waited two hours for {topic} and got nothing #{tag} terrible",
    "why is the {topic} always down exactly when i need it #{tag}",
    "absolutely fed up with the {topic} situation #{tag}",
    "worst {topic} i have dealt with, zero communication #{tag}",
]

NOISE = [
    " http://t.co/{noise}",
    " @{user}",
    " @{user} http://bit.ly/{noise}",
    "",
    "",
    "",
]

URL_RE = re.compile(r"(https?://\S+|www\.\S+)")
MENTION_RE = re.compile(r"@\w+")
WHITESPACE_RE = re.compile(r"\s+")
HASHTAG_RE = re.compile(r"#(\w+)")


def seed_clean(text: str) -> str:
    """A local mirror of `preprocess.clean_text` for seeded rows only.

    Deliberately NOT imported from pipeline/preprocess.py: that module returns
    Spark column expressions, and `make demo-seed` must work with no Spark,
    no HDFS and no JVM at all -- that is the whole point of the seed path.

    Nothing in the production batch or streaming path uses this function, so
    it cannot cause training/serving skew. Rule A is enforced where it matters
    by tests/test_parity.py.
    """
    text = text.lower()
    text = URL_RE.sub(" ", text)
    text = MENTION_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def _diurnal(minute_of_run: int, total_minutes: int) -> float:
    """A smooth activity curve, so the charts have a shape worth looking at.

    Two overlapping sine waves plus a slow ramp: enough structure that the
    range selector and the stacked area chart show something other than noise.
    """
    phase = minute_of_run / total_minutes
    slow = math.sin(phase * math.pi)
    fast = 0.25 * math.sin(phase * math.pi * 9)
    return max(0.15, slow + fast)


def build_documents(now: datetime, rng: random.Random) -> tuple[list, list, list]:
    run_id = f"{SEED_RUN_PREFIX}{now.strftime('%Y%m%d_%H%M')}"
    total_minutes = HOURS * 60
    start = (now - timedelta(minutes=total_minutes)).replace(second=0, microsecond=0)

    tags = TOPICS[:HASHTAG_COUNT]
    windows: list[dict] = []
    hashtags: list[dict] = []
    # (event_time, prediction, tag) triples to sample scored records from.
    population: list[tuple[datetime, str, str]] = []

    for minute in range(total_minutes):
        window_start = start + timedelta(minutes=minute)
        window_end = window_start + timedelta(minutes=WINDOW_MINUTES)
        intensity = _diurnal(minute, total_minutes)

        volume = int(rng.gauss(220 * intensity, 25 * intensity))
        volume = max(8, volume)
        # Sentiment drifts over the run instead of sitting at a constant ratio.
        negative_share = 0.42 + 0.18 * math.sin(minute / total_minutes * math.pi * 2.3)
        counts = {
            "negative": max(1, int(volume * negative_share)),
            "positive": max(1, int(volume * (1 - negative_share))),
        }

        for prediction, count in counts.items():
            # A window with count 0 is never written: absence and zero are
            # different things, and 12.5 depends on that distinction.
            if count <= 0:
                continue
            windows.append(
                Window(
                    _id=Window.make_id(window_start, prediction),
                    window_start=window_start,
                    window_end=window_end,
                    prediction=prediction,
                    count=count,
                    avg_confidence=round(rng.uniform(0.62, 0.93), 4),
                    run_id=run_id,
                    source="seed",
                    updated_at=window_end + timedelta(seconds=4),
                ).model_dump(by_alias=True)
            )

        # A handful of tags are active in any given window.
        for tag in rng.sample(tags, k=rng.randint(3, 7)):
            tag_total = max(1, int(counts["negative"] + counts["positive"]) // rng.randint(8, 25))
            tag_negative = int(tag_total * rng.uniform(0.25, 0.75))
            hashtags.append(
                Hashtag(
                    _id=Hashtag.make_id(window_start, tag),
                    window_start=window_start,
                    tag=tag,
                    count=tag_total,
                    sentiment_split=SentimentSplit(
                        positive=tag_total - tag_negative,
                        negative=tag_negative,
                        neutral=0,
                    ),
                    run_id=run_id,
                    source="seed",
                ).model_dump(by_alias=True)
            )

        for prediction, count in counts.items():
            for _ in range(min(count, 12)):
                event_time = window_start + timedelta(seconds=rng.randint(0, 59))
                population.append((event_time, prediction, rng.choice(tags)))

    scored = _build_scored(population, run_id, rng)
    return windows, hashtags, scored


def _build_scored(
    population: list[tuple[datetime, str, str]], run_id: str, rng: random.Random
) -> list[dict]:
    sample = rng.sample(population, k=min(SCORED_RECORDS, len(population)))
    documents = []
    for event_time, prediction, tag in sample:
        templates = POSITIVE_TEMPLATES if prediction == "positive" else NEGATIVE_TEMPLATES
        text_raw = rng.choice(templates).format(topic=rng.choice(TOPICS), tag=tag)
        text_raw += rng.choice(NOISE).format(
            noise=f"{rng.randrange(16**6):06x}", user=f"user{rng.randint(100, 999)}"
        )
        text_clean = seed_clean(text_raw)
        documents.append(
            Scored(
                text_raw=text_raw,
                text_clean=text_clean,
                prediction=prediction,
                confidence=round(rng.uniform(0.55, 0.97), 4),
                hashtags=HASHTAG_RE.findall(text_clean),
                event_time=event_time,
                ingest_time=event_time + timedelta(milliseconds=rng.randint(200, 1800)),
                # sha1, not Python's hash(): hash() is salted per process,
                # so --seed 42 would not reproduce across runs.
                dedup_key=hashlib.sha1(
                    f"{text_clean}|{event_time.isoformat()}".encode("utf-8")
                ).hexdigest(),
                run_id=run_id,
                source="seed",
            ).model_dump(by_alias=True, exclude_none=True)
        )
    return documents


def _upsert(collection, documents: list[dict], *, key: str) -> None:
    """Idempotent bulk write, so re-seeding never duplicates or raises."""
    if not documents:
        return
    operations = [
        UpdateOne({key: document[key]}, {"$set": document}, upsert=True)
        for document in documents
    ]
    collection.bulk_write(operations, ordered=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42, help="RNG seed")
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep existing seeded documents instead of replacing them",
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    print(f"building {HOURS}h of seeded documents (rng seed {args.seed})...")
    windows, hashtags, scored = build_documents(now, rng)
    print(f"  windows  {len(windows):,}")
    print(f"  hashtags {len(hashtags):,}")
    print(f"  scored   {len(scored):,}")

    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10_000)
    db = client[DB_NAME]
    try:
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"\ncannot reach MongoDB at {settings.mongo_uri}: {exc}", file=sys.stderr)
        print("start it with: docker compose up -d mongo", file=sys.stderr)
        return 1

    if not args.keep:
        for collection in (COLL_WINDOWS, COLL_HASHTAGS, COLL_SCORED):
            removed = db[collection].delete_many({"source": "seed"}).deleted_count
            if removed:
                print(f"  removed {removed:,} existing seeded docs from {collection}")

    print("\nwriting to MongoDB...")
    db[COLL_WINDOWS].insert_many(windows, ordered=False)
    db[COLL_HASHTAGS].insert_many(hashtags, ordered=False)
    db[COLL_SCORED].insert_many(scored, ordered=False)

    # The absence of a heartbeat is what makes the mode derive to SEED rather
    # than LIVE. Removing any stale one is part of seeding, not an afterthought.
    removed_heartbeat = db[COLL_HEARTBEAT].delete_many({}).deleted_count
    if removed_heartbeat:
        print(f"  cleared {removed_heartbeat} stale heartbeat document(s)")

    total = sum(doc["count"] for doc in windows)
    print(
        f"\nseeded {len(windows):,} windows covering {HOURS}h "
        f"({total:,} classified records represented)"
    )
    print("mode will derive to SEED -- no heartbeat, no spark-sourced windows")
    print("the Model page will say 'not yet evaluated' until `make train` runs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

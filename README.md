# Real-Time Sentiment Analytics of Social Media Streams


A Big Data pipeline that replays the Sentiment140 corpus as a continuous
stream, classifies each record with a Spark MLlib model, aggregates results
into one-minute event-time windows, persists them to MongoDB, and visualises
them live.

**The point is the pipeline, not the classifier.** Naive Bayes on TF-IDF is a
deliberate baseline; the engineering is in ingestion, distributed storage,
stream semantics (windows, watermarks, deduplication, idempotent writes), the
serving-layer split, and measured stream behaviour.

```
Sentiment140 CSV ──► HDFS /sentiment/raw ──► clean_batch ──► /clean, /train, /test
                                                                    │
                                          trainer ──► PipelineModel + metrics
                                                                    │
producer ──► socket :9999 | Kafka tweets.raw ──► Spark Structured Streaming
                                                 (preprocess → model → watermark
                                                  → dedup → foreachBatch)
                                                                    │
                                            MongoDB (windows · scored · hashtags
                                             · metrics · heartbeat · runs · batches)
                                                                    │
                                              FastAPI (REST + WebSocket) ──► React
```

## Requirements

Windows 11 with WSL2, 16 GB RAM. Everything else is installed by setup:

| Where | What |
|---|---|
| Windows | Python 3.11, Node 20+, Docker Desktop, GNU Make |
| WSL Ubuntu | OpenJDK 17, Python 3.11 (deadsnakes) — Spark runs here |
| Docker | MongoDB 7.0, Kafka 3.7 (KRaft), Hadoop 3.3.6 (HDFS) |

Why Spark runs in WSL: see `docs/LIMITATIONS.md` §8.

## Setup

1. Put your Kaggle credentials in `.env` (copy `.env.example`). The file is
   gitignored.
2. Then:

```bash
make setup          # venvs, containers, Mongo indexes, HDFS directories
make verify-spark   # C5
make verify-mongo   # C6
```

## Running it

```bash
make demo-seed      # dashboard with seeded data, no pipeline needed
make api            # backend on :8000
make web            # dashboard on :5173

make ingest         # download, profile, load to HDFS
make clean-batch    # preprocess, train/test split
make train          # Naive Bayes, 200k rows → real metrics document
make train-all      # NB vs LR × 3 emoji strategies → docs/experiments.md

make demo-socket    # producer → socket → Spark → Mongo; badge goes LIVE
make demo-kafka     # same, through Kafka

make evidence       # regenerate every proof in docs/evidence/
make test           # full test suite
```

## Where each claim is proved

The 26-row traceability matrix is PRD §2. Proof commands:

| Claim | Command | Artifact |
|---|---|---|
| Volume (C1) | `make evidence-volume` | `docs/evidence/volume.txt` |
| Schema, labels (C2, C3) | `make ingest` | `docs/dataset_profile.md` |
| HDFS lake (C4, C8) | `make evidence-hdfs` | `docs/evidence/hdfs_listing.txt` |
| Preprocessing (C7, C24) | `make test` | `tests/unit/test_preprocess.py` |
| Training/serving parity | `make test` | `tests/integration/test_parity.py` |
| NB vs LR, metrics (C9–C11) | `make train-all` | Model page, `docs/experiments.md` |
| Kafka resume from checkpoint (C13) | `make evidence-kafka-resume` | `docs/evidence/kafka_resume.txt` |
| Reproducible replay (C15) | `make evidence-reproducible` | `docs/evidence/reproducible.txt` |
| Watermarks, late data (C17) | `make evidence-watermark`, `make test` | Stream Monitor, `tests/integration/test_late_data.py` |
| Deduplication (C18) | `make test` | `tests/integration/test_dedup.py` |
| Storage split (C19) | `make evidence-storage-split` | Data & Storage page |
| Index use (C20) | `make evidence-query-plan` | `docs/evidence/query_plan.txt` |
| Latency, throughput (C22, C23) | `make evidence-velocity` | Stream Monitor |
| Data correctness (§12.5) | `make evidence-reconciliation` | Pipeline Health page |
| Version pinning (C25) | `make verify-spark` | `pipeline/versions.py` |
| Ownership (C26) | — | `docs/OWNERSHIP.md` |

## Honesty rules this codebase holds to

- No fabricated metric anywhere. An un-evaluated model shows "not yet
  evaluated", never zeros. Latency and throughput come from Spark's own query
  progress, not a stopwatch around the sink.
- The mode badge (`LIVE` / `REPLAY` / `SEED` / `EMPTY`) is derived by the
  backend, never configured. Seeded and Spark-written data are never summed
  together.
- Everything this hardware cannot do honestly is recorded in
  `docs/LIMITATIONS.md`.

## Troubleshooting

**Docker Desktop will not start after an abrupt shutdown** (crash, forced quit,
full disk). The backend log
(`%LOCALAPPDATA%\Docker\log\host\com.docker.backend.exe.log`) shows
`rename ... sailor-ingest.sock ... The file cannot be accessed by the system`,
or the same for `engine.sock`. Docker left stale Unix-socket files that Windows
itself cannot rename or delete. Quit Docker Desktop completely, remove them from
WSL, then start Docker Desktop again:

```bash
wsl -d Ubuntu -u root -- bash -c "rm -f /mnt/c/Users/$USER/AppData/Local/Docker/run/* /mnt/c/Users/$USER/AppData/Local/docker-secrets-engine/*"
```

(Replace `$USER` with your Windows user name if it differs from your WSL one.)
These are runtime sockets Docker recreates on every start; nothing is lost.

**Spark cannot reach HDFS / "No FileSystem for scheme C"** when running `hdfs`
commands from Git Bash: Git Bash rewrote `/sentiment/...` into a Windows path.
Prefix the command with `MSYS_NO_PATHCONV=1`.

**The badge never leaves LIVE after the producer stops.** It should fall to
REPLAY about 16 seconds later (see `docs/LIMITATIONS.md` §15). If it does not,
the stream job is still receiving data from somewhere.

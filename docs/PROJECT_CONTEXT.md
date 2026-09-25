# Sentiment Stream — full project context

> **For Claude:** this file summarises a long build session between the student (Aaron) and Claude Code. The project is a university Big Data project, complete and working on Aaron's laptop. Use this file as the ground truth about what exists, how it works, which decisions were made and why, and what is still open. Every number here was measured on the real system. Do not invent numbers that are not in this file.

---

## 1. Who and what

- **Course:** BCSE402L Big Data Analytics, DA-2 final project, VIT Chennai.
- **Team:** Aaron Berlit (23BLC1316), Arnav Mishra (23BLC1257), Sachin S S (23BLC1235).
- **Presentation (viva):** the day after this session. The teacher may ask about anything.
- **Spec:** a PRD (`PRD-full-system-build.md`, v2.0) with 26 traceability claims (C1–C26), frozen MongoDB data contracts (§7), 11 build phases (§11), a test plan (§12), and hard rules:
  - never fabricate metrics;
  - never mock at layer boundaries;
  - one preprocessing module (Rule A);
  - idempotent writes (Rule B);
  - size everything for a 16 GB laptop.
- **Code:** `D:\PROJECTS\sentiment-stream` (git repo, first commit `4dab8f0`, 125 files).
- **Team study guide (shareable doc):** https://claude.ai/code/artifact/694a2a5f-80b6-47e2-9a0e-a5db93bf9be7 — 20 sections, including a viva Q&A bank.

**One-line pitch:** a Big Data pipeline that replays 1.6 million real tweets as a live stream, classifies each with Spark ML, counts them in one-minute event-time windows, stores results in MongoDB and shows them on a live dashboard. **The pipeline is the point, not the classifier.**

---

## 2. Machine and environment

- **Hardware:** Windows 11 Home, 15.3 GB RAM, 16 logical cores. C: is small (it once filled to 0.21 GB and crashed Docker). D: has about 190 GB free.
- **Where things run:**

| Where | What |
|---|---|
| Windows | Python 3.11 venv (`.venv`), Node 24, the FastAPI backend, the React dashboard (Vite), GNU Make, Git, GitHub CLI |
| WSL2 Ubuntu 24.04 (disk moved to `D:\WSL\Ubuntu`) | OpenJDK 17, Python 3.11 (deadsnakes), Spark venv at `~/.venvs/sentiment` — **all Spark jobs run here** |
| Docker Desktop (disk moved to `D:\WSL\DockerDesktopWSL`) | MongoDB 7.0, Kafka 3.7.1 (KRaft, no Zookeeper), Hadoop 3.3.6 namenode and datanode |

- **Why Spark runs in WSL:** on Windows, Hadoop's filesystem layer needs unofficial `winutils.exe` and `hadoop.dll` binaries. We chose not to run unsigned third-party binaries.
- **Makefile routing:** the variable `SPY` sends Spark targets through `wsl.exe ... scripts/wsl_run.sh`; `PY` runs Windows Python.
- **WSL sudo:** the user didn't know their WSL password, so installs were run with `wsl -u root`.
- **Kaggle API key:** lives in `.env`, which is gitignored and was verified absent from the commit. It's a newer `KGAT_`-format key and works with kaggle 1.6.17.
- **Network:** slow (about 0.27–0.8 MB/s); large downloads needed retries.
- **Pinned versions** (`pipeline/versions.py`, asserted at every Spark entry point): Spark 3.5.3, Scala 2.12, Hadoop 3.3.6, JDK 17, Python 3.11, mongo-spark-connector 10.4.0, spark-sql-kafka 3.5.3.

---

## 3. Architecture

```
Sentiment140 CSV (1.6M) -> load_raw.py -> HDFS /sentiment/raw (Parquet, by label)
  -> clean_batch.py -> /sentiment/clean, /train (80%), /test (20%), seed 42
  -> train.py + evaluate.py -> /sentiment/models/<nb|lr> (whole PipelineModel) + Mongo `metrics`
producer.py (200 tweets/s, seeded order, token bucket)
  -> TCP socket :9999  or  Kafka topic tweets.raw
  -> stream_job.py (Spark Structured Streaming, 5 s trigger)
     preprocess (same module) -> model.transform -> watermark 2 min
     -> dropDuplicatesWithinWatermark(dedup_key) -> foreachBatch:
        late-data filter -> upsert scored -> recompute windows/hashtags from scored
     + StreamingQueryListener -> heartbeat + batches (Spark's own metrics)
  -> MongoDB `sentiment_stream`: windows, scored (24h TTL), hashtags, metrics, heartbeat, runs, batches
  -> FastAPI (REST + WebSocket push)  -> React dashboard (6 pages)
```

### Repository layout

| Path | Contents |
|---|---|
| `ingest/` | `download.py`, `profile_dataset.py`, `load_raw.py` |
| `storage/hdfs.py` | Lake path abstraction (`STORAGE_BACKEND=hdfs|local`), HDFS CLI via `docker.exe` |
| `pipeline/` | `preprocess.py` (THE shared module), `emoji_lexicon.py`, `clean_batch.py`, `producer.py`, `stream_job.py`, `hashtags.py`, `versions.py`, `config.py` |
| `trainer/` | `train.py`, `evaluate.py`, `label_policy.py` |
| `api/` | `main.py`, `repository.py` (all Mongo access), `models.py` (frozen Pydantic contracts, `extra="forbid"`), `mode.py`, `ws.py` |
| `web/src/` | React: `pages/` (Overview, StreamMonitor, ModelPage, DataStorage, RawRecords, PipelineHealth, GettingStarted), `components/`, `hooks/useApi.ts`, `lib/` |
| `scripts/` | `demo.py`, `seed.py`, `init_mongo.py`, `init_hdfs.py`, `verify_spark.py`, `verify_mongo.py`, `run_experiments.py`, `snapshot_to_atlas.py`, `wsl_run.sh`, `wsl_setup.sh`, `evidence/*.py` (11 scripts) |
| `tests/` | `unit/` (preprocess, mode, versions, model_loaded_once), `integration/` (api, parity, dedup, late_data) |
| `infra/hadoop/` | Explicit Hadoop XML config (the image's generated configs were 0 bytes) |
| `hosted/` | Vercel entry point `index.py` + slim `requirements.txt` |
| `docs/` | `LIMITATIONS.md` (18 entries), `OWNERSHIP.md`, `dataset_profile.md`, `experiments.md`, `evidence/*.txt`, this file |
| Root | `vercel.json`, `.vercelignore`, `docker-compose.yml`, `docker-compose.hdfs.yml`, `Makefile`, `README.md` (with Troubleshooting) |

---

## 4. Key design decisions (and why)

1. **Rule A — one preprocessing module.** `pipeline/preprocess.py` is used by both batch and stream. `prepare()` runs these steps in order:
   - `filter_retweets` on the raw text (leading `RT ` only);
   - `clean_text`: lowercase, strip URLs, strip @mentions, apply the emoji strategy (strip, keep or map), collapse whitespace, keep `#`;
   - `extract_hashtags` from the cleaned text;
   - `add_dedup_key` = sha1(text_clean | user).

   `test_parity.py` proves batch and stream give byte-identical output.
2. **The whole PipelineModel is saved:** Tokenizer → StopWordsRemover → HashingTF(65,536) → IDF(minDocFreq 2) → NaiveBayes(multinomial) or LogisticRegression(maxIter 20, regParam 0.01). The stream only calls `.transform()`.
3. **Row-level `foreachBatch` with windows recomputed from `scored`.** There's no Spark `groupBy(window)`, because a query emits aggregates or rows, not both, and the PRD wants one `foreachBatch`. The recomputed counts are `$set` on deterministic `_id`s (`"<window_start ISO>|<prediction>"`), which makes restarts idempotent (Rule B) and keeps `windows` and `scored` exactly reconciled.
4. **`scored` is upserted on `(dedup_key, event_time)`.** That keeps the frozen ObjectId `_id` while making re-writes idempotent. With `dedup_key` alone, looping replays moved records between windows.
5. **Late data is dropped by our code** (`LateDataFilter` in `stream_job.py`). Measured: in Spark 3.5 the watermark on dedup operators evicts state but does **not** reject late input. The filter uses Spark's own rule, watermark for batch N = max(previous watermark, max event time of batch N−1 − 2 min), read via `next_batch_watermark(query.lastProgress, delay)`. Drops are counted into `heartbeat.late_records_dropped`.
6. **Measurements come from Spark.** `ProgressListener` (a StreamingQueryListener) writes `heartbeat` and `batches` from the query progress: `triggerExecution` ms and `processedRowsPerSecond`. Empty batches are ignored, so a dead producer makes the heartbeat stale.
7. **Event time = the moment of emission** (`--time-compression` defaults to 1). Compression above 1 pushed event time into the future, and restarts then overlapped old windows, which looked like double counting.
8. **The mode badge is derived, never configured.**
   - **LIVE:** heartbeat younger than 15 s.
   - **REPLAY:** Spark-written windows exist, but the heartbeat is stale.
   - **SEED:** only seed data exists.
   - **EMPTY:** nothing exists.
9. **No blending of data sources.** Once Spark data exists, every API query filters `source: "spark"`. The seed script writes **no** metrics document (a fake accuracy would be indistinguishable from a real one) and no runs document.
10. **Outside LIVE, time ranges anchor to the newest data** (`_time_anchor` in `api/main.py`), so REPLAY and the hosted snapshot aren't empty the next day.
11. **The API never returns 500.** A dead Mongo gives 200 with `data: null` plus warnings. All access goes through `repository.py`. Pagination is keyset-based on `(event_time, _id)`. Search is regex-escaped. The Mongo client uses `tz_aware=True`.
12. **The WebSocket carries only "something changed"**; the browser re-fetches via REST.
13. **Kafka uses `maxOffsetsPerTrigger=2000`** (backpressure).
14. **HDFS** uses explicit config files. The datanode advertises `localhost` (`dfs.client.use.datanode.hostname=true`). The client is forced to `dfs.replication=1`.

---

## 5. Measured results

| What | Result |
|---|---|
| Corpus | 1,600,000 rows, 227.7 MB, ISO-8859-1; 800,000 negative / 800,000 positive / 0 neutral |
| Token classes | URL 4.73%, @mention 46.16%, #hashtag 2.24%, **emoji 0.00%**, **retweets 0.00%** (removed by dataset authors) |
| Text length | mean 74.1, median 69, max 374 characters |
| CSV → Parquet | 227.7 MB → 117.3 MB (about 2×, not the PRD's ~5×) |
| Load into HDFS | 1.6M rows in 40 s |
| Clean | 1,597,185 rows (2,815 empty after cleaning dropped); train 1,278,254 / test 318,931 |
| Naive Bayes | 200k balanced sample (99,891 / 100,109). Accuracy **70.66%**; neg P .703 R .715 F1 .709; pos P .711 R .698 F1 .704; confusion [[114,055, 45,366], [48,194, 111,316]]; fit 12 s |
| Logistic Regression | Accuracy **71.11%**, macro F1 0.7110 (+0.45 pts over NB), fit 20 s |
| Emoji strategies | Identical scores for all three (corpus has no emoji) |
| Stream (10-min socket run) | 120 batches, 115,491 records in 590.8 s → 195.5/s end to end (producer 200/s); Spark median 820 rows/s (about 4× headroom); batch duration p50 1,154 ms, p95 2,466 ms |
| Chart query (`explain`) | IXSCAN, 522 returned / 522 examined / 522 keys, 1 ms |
| Kafka crash + resume | 20,000 published; SIGKILL after 6,435 (6 batches); restart read 13,565; total **exactly 20,000**, 13 batches |
| Socket restart | All 6 pre-restart windows unchanged |
| Dead producer | Badge leaves LIVE **16.1 s** after the kill (last batch landed 1.1 s after, plus the 15 s threshold) |
| Reconciliation at rest | windows total = scored total exactly (e.g. 86,593 = 86,593; later 201,281 = 201,281) |
| HDFS fsck | HEALTHY, 0 under-replicated, average replication 1.0 |
| Storage split (latest) | lake 666 MB; Mongo 79.5 MB, of which `scored` is 78 MB (TTL 24 h); `windows` 159 KB |
| Tests | **113 passing** (53 on Windows against real Mongo, 60 on WSL Spark); frontend `tsc` 0 errors |

---

## 6. Bugs found and fixed during the build

| # | Bug | Caught by | Fix |
|---|---|---|---|
| 1 | Training sample was 100% negative (`limit` on label-partitioned data) | Trainer's label-balance print | `orderBy(rand(seed)).limit(n)`; the trainer refuses if any class is below 10% |
| 2 | Late tweets never dropped (Spark dedup watermark lets late input through) | Late-data test | `LateDataFilter` + `next_batch_watermark` |
| 3 | Time compression 6× made restarts overlap windows (looked like double counting) | Restart test (14/48 windows changed) | Event time = emission time |
| 4 | Latency timed only the Mongo write | Independent audit agent | `ProgressListener` uses Spark's `triggerExecution` |
| 5 | Stream timestamps 5h30m off (naive local datetimes from `collect()`) | Audit | `to_utc()` before writing |
| 6 | HDFS files written with replication 3 (73 under-replicated blocks) | `hdfs fsck` | `spark.hadoop.dfs.replication=1` + `setrep -R 1` |
| 7 | `transform()` on the `probability` vector crashes | Audit | `vector_to_array` |
| 8 | Hashtags empty (`\w` eaten inside a SQL string) | Unit tests | `F.regexp_extract_all(..., F.lit(pattern), 1)` |
| 9 | Kafka resume test passed vacuously (one batch swallowed all 20k) | Reading the result table | `maxOffsetsPerTrigger`, wait for "query started", refuse to pass without a backlog |
| 10 | Summary timestamps missing "Z" (browser shifted them) | Manual check | `tz_aware=True` |
| 11 | Mode badge defaulted to EMPTY while loading | Browser check | Shows "checking…"; API error shows "API UNREACHABLE" |
| 12 | Neutral series drawn at zero | Browser check | Only draw classes present in the data |
| 13 | Throughput KPI derived from the compressed event span | Browser check | Uses heartbeat `rows_per_sec`, only while LIVE |
| 14 | Heartbeat refreshed on empty batches (badge would stay LIVE after the producer died) | Review | Listener skips empty batches |
| 15 | Watermark lag chart showed the 1970 epoch | Browser check | Treat epoch as no watermark; lag = max event time − watermark |
| 16 | Reconciliation flagged ERROR mid-batch | Sampling | Compare only before the watermark while in flight; compare everything at rest |
| 17 | Phone layout overflow (Overview 990 px, Health table 401 px) | 375 px test | `min-w-0` on cards; table wrapped in `overflow-x-auto` |
| 18 | PRD faint-text colour `#5A6275` fails WCAG (2.6–3.2:1) | Contrast calculation | Changed to `#80889B` (4.51–5.44:1) |
| 19 | Interrupted runs shown as "running" | Review | "no end recorded" unless it's the live run |
| 20 | Health endpoint slow (Docker probe on every poll) | Timing | 15 s cache → 0.17 s |
| 21 | uvicorn `--reload` hung and orphaned workers on Windows | Outage | `make api` runs without reload; `make api-dev` keeps reload |
| 22 | Dates failed to parse (`EEE` not allowed in Spark 3) | load_raw error | Strip weekday + zone, parse `MMM dd HH:mm:ss yyyy` |
| 23 | Local CSV path resolved on HDFS (`fs.defaultFS`) | Profile error | `Path.as_uri()` (`file://`) |
| 24 | PySpark wheel built on Windows lacked exec bits in WSL | `spark-submit` EACCES | chmod in `wsl_setup.sh` |
| 25 | `docker` inside WSL is a broken shim | `exists()` always False | Prefer `docker.exe` |
| 26 | Docker won't start after an abrupt stop (stale `sailor-ingest.sock` / `engine.sock`) | Backend log | Delete them from WSL (in README Troubleshooting) |
| 27 | C: drive filled (0.21 GB) | Docker I/O errors | Moved WSL + Docker disks to D:, cleared caches |

---

## 7. Limitations recorded in `docs/LIMITATIONS.md` (18)

1. HDFS replication factor 1 (single node, no fault tolerance).
2. Kafka in KRaft mode, with no Zookeeper.
3. Training on 200k rows by default (`--full` exists).
4. Throughput figures are single-laptop measurements.
5. `scored` is kept for 24 h only (TTL).
6. Python 3.11, not the system's 3.13.
7. No neutral class in the dataset.
8. Spark runs in WSL.
9. Windows recomputed from `scored`, not a Spark `groupBy`.
10. An additive `batches` collection.
11. The seed writes no metrics and no run.
12. The emoji question can't be decided on this corpus.
13. The retweet filter removes nothing on this corpus.
14. Parquet is about 2× smaller, not 5×.
15. A dead producer reads STALLED after 16.1 s, not 15 s.
16. Event time is emission time, not compressed dataset time.
17. Late records are dropped by the pipeline, not by Spark's dedup.
18. The faint-text colour was lightened for WCAG.

**Not done:** the Lighthouse accessibility audit (target ≥ 95) was never run. Contrast, keyboard access and the 375 px layout were checked manually.

---

## 8. How to run (daily)

1. Start Docker Desktop and wait for "Engine running". `docker ps` should show 4 containers; if not, run `make up`.
2. `make api` (port 8000) and `make web` (http://localhost:5173) in two terminals. They can also be started from the Claude desktop app as `sentiment-api` / `sentiment-web` (defined in `D:\.claude\launch.json`).
3. `make demo-socket` (or `make demo-kafka`): after about 1 minute the badge turns LIVE. `Ctrl+C` stops it, and the badge leaves LIVE about 16 s later.
4. Other commands:
   - `make evidence` (~10 min; the Kafka test recreates the topic)
   - `make test` (113 tests)
   - `make train-all`
   - `make down`
5. **Quit Docker from its tray icon, never Task Manager.** The fix for stale sockets is in the README.
6. On a fresh machine: `.env` with Kaggle keys → `make setup` → `make ingest` → `make clean-batch` → `make train-all`.

**Keyboard shortcuts in the dashboard:** `?` sheet, `/` search, `g` + `o/s/m/d/r/h` to jump between pages, `Esc` to close.

---

## 9. Hosting (Vercel) and GitHub — prepared, user is doing it

**Prepared in code:**
- `hosted/index.py` imports `api.main.app`; `hosted/requirements.txt` holds fastapi, pymongo, pydantic, python-dotenv only.
- `vercel.json` uses legacy `builds`: `@vercel/static-build` for `web/package.json` (distDir `dist`) and `@vercel/python` for `hosted/index.py`, with `includeFiles` api/pipeline/storage.
- Routes: `/api/*` → the function, `/ws/*` → 404, `/assets/*` → `/web/assets/*`, everything else → `/web/index.html`.
- Region `bom1`; build env `VITE_HOSTED_SNAPSHOT=1`.
- `.vercelignore` excludes .venv, data, node_modules, dist, tests, evidence.
- `HOSTED_SNAPSHOT=1` makes the API report storage as "HDFS runs on the team laptop" and skip lake listings. The frontend (`VITE_HOSTED_SNAPSHOT`) skips the WebSocket and shows a yellow "Hosted snapshot" banner.
- `scripts/snapshot_to_atlas.py --replace` copies all collections from the local Mongo to `ATLAS_URI` (read from `.env`, never printed). It recreates indexes **except the 24 h TTL**, which would delete the drill-down within a day.
- **Not yet deployed or tested on Vercel.** Claude couldn't log in, so the first deploy may need one fix. Send the Vercel build log if it fails.

**User's steps:**
1. GitHub:
   - `gh auth login`
   - `git branch -M main`
   - `gh repo create sentiment-stream --private --source=. --push`
   - Add teammates as collaborators.
2. MongoDB Atlas:
   - Create a free M0 cluster on AWS, Mumbai (ap-south-1).
   - Add a database user (password without `@ : / ?`).
   - Network Access: `0.0.0.0/0`.
   - Copy the `mongodb+srv://…` connection string into `.env` as `ATLAS_URI=...`.
   - Run `.venv\Scripts\python.exe scripts\snapshot_to_atlas.py --replace`. It should end with "TTL indexes on target: 0".
3. Vercel:
   - Sign up with GitHub and import the repo.
   - Leave framework and root settings as they are.
   - Environment variables: `MONGO_URI` = the Atlas string, `HOSTED_SNAPSHOT` = `1`.
   - Deploy. Check the banner, the REPLAY badge, and that `/api/mode` returns JSON.
4. The hosted site can only show **REPLAY**. Live streaming (Spark/Kafka/HDFS) still runs on the laptop.
5. **Never paste the Atlas password or connection string into chat or git.**

---

## 10. Open decisions (the user's call)

1. **LIVE threshold:** lower from 15 s to 10 s so a dead producer reads stalled within the PRD's 15 s (currently 16.1 s)? It's one line: `LIVE_MAX_AGE_SECONDS` in `pipeline/config.py`, which the mode tests would need updating for.
2. **Neutral class:** stay binary (`none`, current) or enable the `threshold` policy (`--neutral threshold`)?
3. **Demo roles:** does each teammate present their own module, or one presenter? Suggested order:
   - Pipeline Health (Sachin)
   - Data & Storage (Aaron)
   - Model (Arnav)
   - Live demo with `make demo-socket` (Sachin)
   - Click a chart point to show raw vs cleaned tweets (Aaron)
   - `Ctrl+C` to show the badge leaving LIVE (Sachin)

## 11. Ownership (C26, `docs/OWNERSHIP.md`)

| Member | Owns | Hands off |
|---|---|---|
| Aaron | `ingest/`, `storage/`, `pipeline/preprocess.py`, `pipeline/clean_batch.py` | The clean corpus to Arnav |
| Arnav | `trainer/` | The saved PipelineModel + metrics to Sachin |
| Sachin | `pipeline/producer.py`, `pipeline/stream_job.py`, `api/`, `web/` | The demo |

---

## 12. Quick viva answers (most likely questions)

- **Why only ~71%?** NB is a deliberate baseline; the labels are noisy (auto-labelled from emoticons); stop-word removal drops negations like "not"; the project is graded on the pipeline.
- **Why HashingTF?** There's no vocabulary to hold in memory on one machine, so it scales; the cost is hash collisions.
- **What is a watermark?** Newest event time minus 2 minutes. It bounds state, and anything older is late (dropped and counted by our filter).
- **Why don't counts double on restart?** Checkpoints resume at the right offset, and windows are recomputed from stored tweets and `$set` on fixed IDs. Proven by the Kafka crash test: exactly 20,000 of 20,000.
- **Socket vs Kafka?** The socket is simple but loses in-flight data on a crash; Kafka is durable and resumes from its offset.
- **Why MongoDB and HDFS both?** HDFS is for big sequential scans (666 MB lake); MongoDB serves small indexed results in about 1 ms to the dashboard.
- **How do you know the numbers are right?** The reconciliation check (windows total = scored total, exactly), click-to-drill to the actual tweets, and 113 tests.
- **Hardest bug?** Late data. The counter always read 0, and it looked fine until we injected a deliberately late tweet and found Spark let it through.

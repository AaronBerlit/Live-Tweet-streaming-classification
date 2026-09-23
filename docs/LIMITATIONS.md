# Limitations

PRD §0 rule 8: where this hardware cannot do something honestly, it is done at
reduced scale and the reason is recorded here. Nothing on this list is hidden
in the report or papered over in the UI.

Target machine: **15.3 GB RAM, 16 logical cores, Windows 11**, Spark on
`local[2]`.

---

## 1. HDFS replication factor is 1 on a single node

`dfs.replication=1`, one namenode, one datanode, both in containers on one
host.

**What this still gives us:** the real HDFS API, real path semantics, the real
`hdfs dfs` CLI, block-based splitting, and Parquet read/write through the
Hadoop filesystem layer — everything the pipeline actually exercises.

**What it does not give us:** fault tolerance. There is no second replica, so a
lost datanode is lost data. A multi-node cluster is what HDFS is *for*, and we
are not demonstrating that property.

This is stated in the report rather than left to be discovered in a viva.

## 2. Kafka runs in KRaft mode — there is no Zookeeper container

The PRD's repository tree lists `zookeeper` in `docker-compose.yml`. Kafka 3.7
does not need it: KRaft replaced the Zookeeper dependency, and the official
`apache/kafka` image runs broker and controller in one process.

Dropping it saves roughly 500 MB of RAM on a machine that also has to run
Spark, and removes a container that could fail independently. The Kafka path
(C13) is otherwise exactly as specified.

## 3. Training defaults to 200,000 rows, not 1.6M

`trainer/train.py --rows` defaults to 200,000 (PRD §6.1). `--full` opts into
the complete corpus.

The default is what gets reported unless a `--full` run is recorded in
`docs/experiments.md` with its own metrics document. Every figure on the Model
page carries its `train_rows`, so a 200k result is never presented as a
full-corpus result.

## 4. Single-machine throughput is the measurement, not a cluster's

C22 and C23 (latency, throughput) are real numbers read from
`heartbeat.batch_duration_ms` and `heartbeat.rows_per_sec` — but they are
`local[2]` numbers on a laptop that is simultaneously running the producer,
MongoDB, Kafka, HDFS and a Vite dev server.

They are honest measurements of *this* deployment. They are not a claim about
what Spark can do on a cluster, and the Stream Monitor labels them as observed
values from the current run.

## 5. `scored` is retained for 24 hours only

A TTL index on `scored.ingest_time` expires raw scored records after 24 hours
(PRD §7.2). At replay speed this collection grows fast enough to exhaust the
disk, and the aggregates in `windows` are the durable artifact.

Consequence: the Raw Records page cannot show records older than a day. If a
full-run dump is ever needed for evaluation, add an export step — do not remove
the TTL.

## 6. Python 3.11, not the system 3.13

PySpark 3.5.3 does not support Python 3.12 or 3.13. The machine's system
interpreter is 3.13, so the project venv is built on a separately installed
3.11. `pipeline/versions.py` asserts this at every Spark entrypoint, so the
mismatch surfaces as a named error instead of an obscure failure inside py4j.

## 7. The dataset has no neutral class

Sentiment140 is labelled 0 (negative) and 4 (positive) only. There is no
neutral ground truth, and none is invented.

The `neutral_strategy` field on every metrics document records which policy
produced the numbers, and the UI renders two series with a stated dataset
footnote under `none`. A permanently-zero neutral series would falsely imply
the model looked for neutral records and found none.

## 8. Spark runs in WSL2, not natively on Windows

Hadoop's filesystem layer on Windows requires `winutils.exe` and `hadoop.dll`,
which Apache does not distribute; they exist only in third-party mirrors. Rather
than execute unsigned binaries on every Spark job, all Spark entrypoints run
inside WSL2 Ubuntu (`scripts/wsl_run.sh`, the `SPY` variable in the Makefile)
against the same repository on `/mnt/d`. MongoDB, Kafka and HDFS stay in Docker
Desktop and are reached from WSL on `localhost`. The API, seed script and
frontend run on Windows. Nothing about the pipeline's behaviour changes; only
where the JVM runs.

## 9. Windows are recomputed from `scored`, not by a stateful Spark aggregation

A streaming query emits either aggregates or rows, not both, and the PRD asks
for one `foreachBatch` writing `windows`, `scored` and `hashtags` together. The
stream therefore stays row-level; each batch upserts its rows into `scored` and
recomputes the touched windows' counts from `scored`. This makes writes
idempotent under restart (Rule B) and makes the 12.5 reconciliation exact by
construction. The watermark still bounds `dropDuplicatesWithinWatermark` state,
and late rows are counted from Spark's own `numRowsDroppedByWatermark`.

## 10. An additive `batches` collection

The frozen 7.5 `heartbeat` is one upserted document and has no history, but the
Stream Monitor's latency chart and `make evidence-velocity` need a series. A
seventh collection, `batches`, stores one sample per micro-batch (24h TTL). No
frozen contract is changed.

## 11. The seed writes no metrics document and no run

PRD Phase 3 lists "one metrics doc" in the seed, but rule 4 forbids any
placeholder accuracy, and the frozen 7.4 contract has no field that could mark a
metrics document as fabricated. The Model page therefore reads "not yet
evaluated" until `make train` runs. A seed run is likewise not written to `runs`,
so the run history never shows a pipeline run that did not happen.

## 12. The emoji experiment cannot be decided on this corpus

The file is `training.1600000.processed.noemoticon.csv`: emoticons were removed
upstream, and its ISO-8859-1 encoding cannot represent emoji. The three emoji
strategies are implemented and measured (`docs/experiments.md`), but
near-identical scores are the expected result. Settling the question needs a
corpus that retains emoji.

## 13. The retweet filter (C24) removes nothing on this corpus

`docs/dataset_profile.md` measures 0.00% of records starting with `RT `, and an
independent pass over the raw CSV finds zero occurrences of `RT @` anywhere in
the text: Sentiment140's authors removed retweets before publishing it. The
filter is implemented, runs on every batch and stream record, and is proved by
`tests/unit/test_preprocess.py::test_retweet_filter` -- but the report must not
claim it reduced skew *on this dataset*. It protects against retweets in a live
feed, which this replayed corpus does not contain.

## 14. Parquet is about 2x smaller than the CSV here, not the ~5x the PRD cites

PRD §4.3 justifies Parquet as "~5x smaller than CSV on this corpus". Measured on
this deployment: the 227.7 MB source CSV becomes 117.3 MB of Parquet in
`/sentiment/raw` (Data & Storage page, `make evidence-volume`). Short tweet text
compresses less well than the estimate assumed. The columnar, splittable and
schema-carrying arguments for Parquet still hold; the report should quote the
measured ratio.

## 15. A dead producer reads STALLED after about 16 seconds, not within 15

Measured: the producer was killed at 06:56:56.0; the last micro-batch carrying
data completed 1.1s later; the heartbeat crossed the PRD's 15s LIVE threshold
16.1s after the kill. The two PRD figures -- "LIVE while heartbeat age < 15s"
(§9.1) and "kill the producer, STALLED within 15s" (DoD 6) -- cannot both hold,
because the in-flight batch always lands up to one trigger (5s) after the
producer dies. The PRD threshold is kept as written. Lowering the LIVE threshold
to 10s (`LIVE_MAX_AGE_SECONDS` in `pipeline/config.py`) would meet DoD 6 with
margin, at the cost of departing from §9.1.

## 16. Event time is emission time, not compressed dataset time

PRD §8 describes `--time-compression` as mapping dataset timestamps onto the
wall clock. Any ratio above 1 moves event time into the future; a restarted
replay then writes into windows an earlier run already filled, which reads on
the dashboard exactly like double counting. This was observed in testing and is
why the default is 1: event time is the moment each record is emitted, so
windows fill in real time and every restart continues forward. The flag remains
for experiments.

## 17. Late records are dropped by the pipeline, not by Spark's dedup operator

Measured on Spark 3.5.3: a watermark on `dropDuplicatesWithinWatermark` (and on
`dropDuplicates`) evicts old deduplication *state* but does not reject late
*input*. A record eight minutes behind a 2-minute watermark passed straight
through, with `numRowsDroppedByWatermark = 0`. Only stateful aggregations and
joins drop late rows, and this pipeline deliberately has neither (§9).

Until this was found, late rows silently landed in old windows and the
`late_records_dropped` counter was structurally always 0. The fix
(`LateDataFilter` in `pipeline/stream_job.py`) drops, before any write, every
row older than the watermark Spark establishes for the batch --
max(previous watermark, newest event time seen - delay), read from
`query.lastProgress` -- and counts it. `tests/integration/test_late_data.py`
proves the drop, the count, and that the count reaches the heartbeat, and keeps
a characterisation test that will fail if a future Spark starts dropping late
rows itself.

## 18. The faint-text colour is lighter than the PRD's token

PRD §9.2 specifies `#5A6275` for tertiary text and, separately, WCAG 2.1 AA
contrast of at least 4.5:1 against the actual surfaces. The two conflict:
`#5A6275` measures 3.16:1 on the background, 2.91:1 on cards and 2.63:1 on
raised elements, and it is used for real text (labels, timestamps, table
headers). The token was lightened to `#80889B`, the smallest change that passes
on all three surfaces (5.44 / 5.00 / 4.51). Every other PRD colour already
passes.

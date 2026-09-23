# Ownership and handoffs (C26)

Three-person split matching the DA-2 progress report. Module boundaries are
real boundaries: each owner's code is importable by the others, and nobody
reaches past an interface into someone else's internals.

| Owner | Modules | Produces for | Consumes from |
|---|---|---|---|
| **Aaron Berlit** (23BLC1316) | `ingest/`, `storage/`, `pipeline/preprocess.py`, `pipeline/clean_batch.py` | Arnav — the clean corpus in the lake | — |
| **Arnav Mishra** (23BLC1257) | `trainer/` | Sachin — the persisted `PipelineModel` and the `metrics` document | Aaron — `/sentiment/clean` |
| **Sachin S S** (23BLC1235) | `pipeline/producer.py`, `pipeline/stream_job.py`, `api/`, `web/` | The demo | Arnav — the model; Aaron — the preprocessing module |

## The two interfaces that had to be agreed before anyone started

Everything else can be renegotiated mid-build. These two cannot, because both
sides commit to them independently.

### 1. The clean-corpus schema — Aaron → Arnav

Written to `/sentiment/clean` as Parquet by `pipeline/clean_batch.py`:

| Column | Type | Notes |
|---|---|---|
| `target` | int | 0 = negative, 4 = positive (source encoding preserved) |
| `label` | string | `negative` / `positive` |
| `id` | long | source tweet id |
| `date` | timestamp | parsed from the source's non-ISO format |
| `query` | string | frequently `NO_QUERY` |
| `user` | string | |
| `text` | string | **raw**, untouched — keeps `text_raw` available downstream |
| `text_clean` | string | lowercased, URLs and `@mentions` stripped, `#` preserved |
| `hashtags` | array&lt;string&gt; | lowercased, `#` removed |
| `dedup_key` | string | sha1 of normalised text + user |

`/sentiment/train` and `/sentiment/test` carry the identical schema, split 80/20
on a fixed seed.

### 2. The persisted model — Arnav → Sachin

`/sentiment/models/<nb|lr>` holds a **whole `PipelineModel`**, not a bare
classifier: `Tokenizer → StopWordsRemover → HashingTF → IDF → classifier`.

The streaming job calls `PipelineModel.load(...)` once at startup and then only
`.transform()`. It never reimplements featurisation. This is what makes
training/serving skew structurally impossible rather than merely unlikely, and
`tests/test_parity.py` enforces the other half of it.

The matching `metrics` document (§7.4 of the PRD) records the exact
`feature_config` the model was built with, so the Model page can state what was
actually run rather than what was intended.

## Shared, owned by nobody alone

`pipeline/versions.py`, `pipeline/config.py` and the MongoDB data contracts in
`api/models.py` are frozen shared ground. Changing one is a three-way
conversation, not a commit.

/**
 * Types mirroring the frozen MongoDB contracts in PRD §7 and api/models.py.
 *
 * These match the backend field-for-field. No `any` anywhere: if the shape
 * changes on one side, the other side must fail to compile rather than
 * silently render `undefined`.
 */

export type Prediction = "positive" | "negative" | "neutral";
export type Source = "spark" | "seed";
export type InputSource = "socket" | "kafka";

/** Derived, never configured (PRD §9.1). */
export type Mode = "LIVE" | "REPLAY" | "SEED" | "EMPTY";

export type HealthStatus = "ok" | "stalled" | "offline" | "error" | "unknown";

/** Every response is wrapped in this. */
export interface Envelope<T> {
  mode: Mode;
  server_time: string;
  data: T | null;
  warnings: string[];
}

/** §7.1 */
export interface Window {
  _id: string;
  window_start: string;
  window_end: string;
  prediction: Prediction;
  count: number;
  avg_confidence: number;
  run_id: string;
  source: Source;
  updated_at: string;
}

/** §7.2 */
export interface Scored {
  _id: string;
  text_raw: string;
  text_clean: string;
  prediction: Prediction;
  confidence: number;
  hashtags: string[];
  event_time: string;
  ingest_time: string;
  dedup_key: string;
  run_id: string;
  source: Source;
}

export interface SentimentSplit {
  positive: number;
  negative: number;
  neutral: number;
}

/** §7.3, as aggregated by /api/hashtags. */
export interface HashtagRow {
  tag: string;
  count: number;
  sentiment_split: SentimentSplit;
}

export interface FeatureConfig {
  vectorizer: string;
  num_features: number;
  idf: boolean;
  emoji_strategy: "strip" | "keep" | "map";
}

/** §7.4 */
export interface Metrics {
  _id: string;
  model_name: string;
  trained_at: string;
  train_rows: number;
  test_rows: number;
  accuracy: number;
  precision: Record<string, number>;
  recall: Record<string, number>;
  f1: Record<string, number>;
  confusion_matrix: number[][];
  feature_config: FeatureConfig;
  neutral_strategy: "none" | "threshold" | "external";
  neutral_threshold: number | null;
  train_duration_sec: number;
  notes: string;
}

/** §7.5 */
export interface Heartbeat {
  _id: string;
  last_batch_id: number;
  last_batch_at: string;
  rows_in_batch: number;
  batch_duration_ms: number;
  rows_per_sec: number;
  input_source: InputSource;
  watermark: string | null;
  late_records_dropped: number;
  run_id: string;
}

export interface SparkConfig {
  master: string;
  trigger: string;
  window: string;
  watermark: string;
}

/** §7.6 */
export interface Run {
  _id: string;
  started_at: string;
  ended_at: string | null;
  model_id: string | null;
  input_source: InputSource;
  replay_rate_per_sec: number;
  spark_config: SparkConfig;
  storage_backend: "hdfs" | "local";
}

export interface ComponentHealth {
  name: string;
  status: HealthStatus;
  detail: string | null;
  last_contact: string | null;
  start_command: string | null;
}

export interface ModeInfo {
  mode: Mode;
  reason: string;
  heartbeat_age_seconds: number | null;
}

export interface Summary {
  total_classified: number;
  by_prediction: Partial<Record<Prediction, number>>;
  avg_confidence: number | null;
  window_start: string | null;
  window_end: string | null;
}

export interface ScoredPage {
  records: Scored[];
  next_cursor: string | null;
}

export interface CollectionStat {
  collection: string;
  documents: number;
  size_bytes: number;
  storage_bytes: number;
  indexes: number;
  index_bytes: number;
}

export interface LakeEntry {
  path: string;
  size_bytes: number;
  is_dir: boolean;
}

export interface StorageInfo {
  backend: { backend: string; root: string; replication: string };
  lake: LakeEntry[];
  lake_total_bytes: number;
  collections: CollectionStat[];
}

export interface PipelineConfig {
  versions: Record<string, string>;
  storage: { backend: string; root: string; replication: string };
  spark: { master: string; driver_memory: string };
  mongo_uri: string;
  kafka_bootstrap: string;
  display_timezone: string;
  latest_run: Run | null;
  collections: CollectionStat[];
}

/** One recorded micro-batch. Populated from Spark's own query progress. */
export interface BatchSample {
  _id: string;
  run_id: string;
  batch_id: number;
  batch_at: string;
  rows_in_batch: number;
  batch_duration_ms: number;
  rows_per_sec: number;
  input_source: InputSource;
  watermark: string | null;
  /** Newest event time Spark saw in this batch; absent on older samples. */
  max_event_time?: string | null;
  late_records_dropped: number;
}

export interface StreamState {
  heartbeat: Heartbeat | null;
  batches: BatchSample[];
  run: Run | null;
}

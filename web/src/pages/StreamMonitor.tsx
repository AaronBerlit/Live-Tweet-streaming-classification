/**
 * Stream Monitor (PRD §9.2 page 2) — where C17, C22 and C23 are demonstrated.
 *
 * Every number here is read from `batches`, which the streaming job populates
 * from Spark's own query progress. Nothing on this page is timed by the
 * dashboard or inferred from a configured setting.
 */

import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { AsyncBoundary, EmptyState, LoadingCard } from "../components/States";
import { useStream } from "../hooks/useApi";
import {
  formatDateTime,
  formatDecimal,
  formatNumber,
  formatTime,
  relativeAge,
} from "../lib/format";
import type { BatchSample, StreamState } from "../lib/types";

/** Nearest-rank percentile — interpolation misleads on small samples. */
function percentile(values: number[], fraction: number): number {
  if (values.length === 0) return 0;
  const ordered = [...values].sort((a, b) => a - b);
  const index = Math.min(
    ordered.length - 1,
    Math.max(0, Math.round(fraction * ordered.length) - 1),
  );
  return ordered[index] ?? 0;
}

function Stat({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail?: string;
}) {
  return (
    <div className="card min-w-0 p-4">
      <p className="text-xs uppercase tracking-wide text-text-faint">{label}</p>
      <p className="num mt-1 text-xl font-semibold">{value}</p>
      {detail ? <p className="mt-1 text-xs text-text-muted">{detail}</p> : null}
    </div>
  );
}

/**
 * How far the watermark trails the newest event time Spark has seen.
 *
 * Measured against event time, not the wall clock: the replay compresses event
 * time, so wall-clock minus watermark would mostly measure the compression.
 */
function watermarkLagSeconds(sample: BatchSample): number | null {
  if (!sample.watermark || !sample.max_event_time) return null;
  return (
    (new Date(sample.max_event_time).getTime() - new Date(sample.watermark).getTime()) /
    1000
  );
}

function Monitor({ state }: { state: StreamState }) {
  const { batches, heartbeat, run } = state;
  const durations = batches.map((b) => b.batch_duration_ms);
  const p50 = percentile(durations, 0.5);
  const p95 = percentile(durations, 0.95);

  const chartData = batches.map((b) => ({
    batch: b.batch_id,
    at: b.batch_at,
    duration: b.batch_duration_ms,
    rows: b.rows_in_batch,
    rate: b.rows_per_sec,
    lag: watermarkLagSeconds(b),
  }));

  const totalRows = batches.reduce((sum, b) => sum + b.rows_in_batch, 0);
  const lateDropped = batches.reduce(
    (max, b) => Math.max(max, b.late_records_dropped),
    0,
  );

  const tooltipStyle = {
    background: "var(--raised)",
    border: "1px solid var(--border)",
    borderRadius: 8,
    fontSize: 12,
  };

  return (
    <div className="space-y-6">
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Stat
          label="Last batch"
          value={heartbeat ? `#${heartbeat.last_batch_id}` : "—"}
          detail={heartbeat ? relativeAge(heartbeat.last_batch_at) : "no heartbeat"}
        />
        <Stat
          label="Throughput"
          value={heartbeat ? `${formatDecimal(heartbeat.rows_per_sec, 1)}/s` : "—"}
          detail="Spark processedRowsPerSecond"
        />
        <Stat
          label="Batch duration p50 / p95"
          value={`${formatNumber(Math.round(p50))} / ${formatNumber(Math.round(p95))} ms`}
          detail={`across ${formatNumber(batches.length)} batches`}
        />
        <Stat
          label="Late records dropped"
          value={formatNumber(lateDropped)}
          detail="numRowsDroppedByWatermark"
        />
      </div>

      <section className="card min-w-0 p-4" aria-labelledby="latency-heading">
        <h2 id="latency-heading" className="text-sm font-medium">
          Micro-batch duration (C22)
        </h2>
        <p className="mt-1 text-xs text-text-muted">
          Spark&rsquo;s <code className="num">triggerExecution</code> time — the whole
          trigger, including source read, preprocessing, inference, deduplication
          and the MongoDB write.
        </p>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={chartData} margin={{ top: 12, right: 8, left: 8 }}>
            <CartesianGrid stroke="var(--border)" vertical={false} />
            <XAxis
              dataKey="at"
              tickFormatter={formatTime}
              stroke="var(--text-faint)"
              fontSize={11}
              minTickGap={48}
            />
            <YAxis stroke="var(--text-faint)" fontSize={11} unit=" ms" width={64} />
            <Tooltip labelFormatter={formatTime} contentStyle={tooltipStyle} />
            <ReferenceLine
              y={p50}
              stroke="var(--neutral)"
              strokeDasharray="4 4"
              label={{ value: "p50", fill: "var(--text-faint)", fontSize: 11 }}
            />
            <ReferenceLine
              y={p95}
              stroke="var(--warning)"
              strokeDasharray="4 4"
              label={{ value: "p95", fill: "var(--warning)", fontSize: 11 }}
            />
            <Line
              type="monotone"
              dataKey="duration"
              stroke="var(--accent)"
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </section>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="card min-w-0 p-4" aria-labelledby="rows-heading">
          <h2 id="rows-heading" className="text-sm font-medium">
            Rows per micro-batch (C23)
          </h2>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={chartData} margin={{ top: 12, right: 8, left: 8 }}>
              <CartesianGrid stroke="var(--border)" vertical={false} />
              <XAxis
                dataKey="at"
                tickFormatter={formatTime}
                stroke="var(--text-faint)"
                fontSize={11}
                minTickGap={48}
              />
              <YAxis stroke="var(--text-faint)" fontSize={11} />
              <Tooltip labelFormatter={formatTime} contentStyle={tooltipStyle} />
              <Bar dataKey="rows" fill="var(--accent)" isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
          <p className="num mt-2 text-xs text-text-muted">
            {formatNumber(totalRows)} records across {formatNumber(batches.length)}{" "}
            batches
          </p>
        </section>

        <section className="card min-w-0 p-4" aria-labelledby="watermark-heading">
          <h2 id="watermark-heading" className="text-sm font-medium">
            Watermark lag (C17)
          </h2>
          <p className="mt-1 text-xs text-text-muted">
            How far the watermark trails the newest event time. It should hold
            at the configured delay; this is what bounds deduplication state.
          </p>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={chartData} margin={{ top: 12, right: 8, left: 8 }}>
              <CartesianGrid stroke="var(--border)" vertical={false} />
              <XAxis
                dataKey="at"
                tickFormatter={formatTime}
                stroke="var(--text-faint)"
                fontSize={11}
                minTickGap={48}
              />
              <YAxis stroke="var(--text-faint)" fontSize={11} unit=" s" width={56} />
              <Tooltip labelFormatter={formatTime} contentStyle={tooltipStyle} />
              <Line
                type="stepAfter"
                dataKey="lag"
                stroke="var(--positive)"
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
          <p className="num mt-2 text-xs text-text-muted">
            watermark:{" "}
            {heartbeat?.watermark ? formatDateTime(heartbeat.watermark) : "not set yet"}
          </p>
        </section>
      </div>

      <section className="card min-w-0 p-4" aria-labelledby="run-heading">
        <h2 id="run-heading" className="text-sm font-medium">
          Current run configuration
        </h2>
        {run ? (
          <dl className="num mt-3 grid gap-2 text-xs sm:grid-cols-2">
            {[
              ["run_id", run._id],
              ["input source", run.input_source],
              ["started", formatDateTime(run.started_at)],
              ["ended", run.ended_at ? formatDateTime(run.ended_at) : heartbeat && Date.now() - new Date(heartbeat.last_batch_at).getTime() < 15_000 ? "still running" : "no end recorded"],
              ["spark master", run.spark_config.master],
              ["trigger", run.spark_config.trigger],
              ["window", run.spark_config.window],
              ["watermark", run.spark_config.watermark],
              ["configured replay rate", `${run.replay_rate_per_sec}/s`],
              ["storage backend", run.storage_backend],
            ].map(([label, value]) => (
              <div key={label} className="flex justify-between gap-4 border-b border-border/50 py-1">
                <dt className="text-text-muted">{label}</dt>
                <dd className="text-text">{value}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="mt-2 text-sm text-text-muted">No run recorded.</p>
        )}
      </section>

      <details className="card min-w-0 p-4">
        <summary className="cursor-pointer text-sm font-medium">
          Recent batches as a table
        </summary>
        <table className="num mt-3 w-full text-xs">
          <caption className="sr-only">The most recent recorded micro-batches</caption>
          <thead className="text-text-faint">
            <tr className="border-b border-border">
              <th scope="col" className="py-1 text-left">Batch</th>
              <th scope="col" className="py-1 text-left">At</th>
              <th scope="col" className="py-1 text-right">Rows</th>
              <th scope="col" className="py-1 text-right">Duration</th>
              <th scope="col" className="py-1 text-right">Rows/sec</th>
              <th scope="col" className="py-1 text-right">Late dropped</th>
            </tr>
          </thead>
          <tbody>
            {[...batches].reverse().slice(0, 25).map((b) => (
              <tr key={b._id} className="border-b border-border/40">
                <td className="py-1">{b.batch_id}</td>
                <td className="py-1">{formatTime(b.batch_at)}</td>
                <td className="py-1 text-right">{formatNumber(b.rows_in_batch)}</td>
                <td className="py-1 text-right">{formatNumber(b.batch_duration_ms)} ms</td>
                <td className="py-1 text-right">{formatDecimal(b.rows_per_sec, 1)}</td>
                <td className="py-1 text-right">{formatNumber(b.late_records_dropped)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </div>
  );
}

export function StreamMonitor() {
  const streamQuery = useStream();

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold">Stream Monitor</h1>
      <AsyncBoundary
        query={streamQuery}
        isEmpty={(state) => state.batches.length === 0}
        loading={<LoadingCard lines={8} title="stream state" />}
        empty={
          <EmptyState
            title="The streaming job has not run"
            detail="Micro-batch latency, throughput and watermark figures appear here once Spark has processed a batch. Nothing is shown until it has."
            command="make demo-socket"
          />
        }
      >
        {(state) => <Monitor state={state} />}
      </AsyncBoundary>
    </div>
  );
}

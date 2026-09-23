/**
 * Pipeline Health (PRD §9.2 page 6, G5: failure is legible).
 *
 * The §4.1 component map with each node's live status, last contact, and the
 * exact command to start it. Status comes from /api/health, which probes each
 * component rather than assuming it is up.
 */

import { AsyncBoundary, LoadingCard } from "../components/States";
import { useHealth, usePipelineConfig, useRuns } from "../hooks/useApi";
import { formatDateTime, relativeAge } from "../lib/format";
import type { ComponentHealth, HealthStatus } from "../lib/types";

const STATUS_STYLE: Record<HealthStatus, { label: string; className: string; mark: string }> = {
  ok: { label: "OK", className: "border-positive/50 text-positive", mark: "●" },
  stalled: { label: "STALLED", className: "border-warning/50 text-warning", mark: "◐" },
  offline: { label: "OFFLINE", className: "border-negative/50 text-negative", mark: "○" },
  error: { label: "ERROR", className: "border-negative/50 text-negative", mark: "✕" },
  unknown: { label: "UNKNOWN", className: "border-border text-text-muted", mark: "?" },
};

/** Pipeline stages in flow order. `component` links a stage to /api/health. */
const STAGES: Array<{ id: string; title: string; detail: string; component?: string; command: string }> = [
  { id: "lake", title: "Data lake", detail: "Sentiment140 as Parquet: raw, clean, train/test, models, checkpoints", component: "storage", command: "docker compose -f docker-compose.hdfs.yml up -d" },
  { id: "producer", title: "Producer", detail: "Rate-limited, seeded replay to socket :9999 or Kafka tweets.raw", command: "make demo-socket" },
  { id: "stream", title: "Spark Structured Streaming", detail: "preprocess → PipelineModel → watermark → dedup → foreachBatch", component: "spark_stream", command: "make demo-socket" },
  { id: "mongo", title: "MongoDB serving layer", detail: "windows · scored · hashtags · metrics · heartbeat · runs", component: "mongo", command: "docker compose up -d mongo" },
  { id: "api", title: "FastAPI", detail: "REST history + WebSocket live push", command: "make api" },
];

function StatusBadge({ status }: { status: HealthStatus }) {
  const style = STATUS_STYLE[status];
  return (
    <span className={`num inline-flex items-center gap-1.5 rounded border px-2 py-0.5 text-xs font-semibold ${style.className}`}>
      <span aria-hidden="true">{style.mark}</span>
      {style.label}
    </span>
  );
}

function statusFor(stageId: string, component: ComponentHealth | undefined, streamStatus: HealthStatus): HealthStatus {
  // The API answering at all is how this page rendered.
  if (stageId === "api") return "ok";
  // The producer has no heartbeat of its own; a live stream implies it is
  // feeding data, anything else is not knowable from here.
  if (stageId === "producer") return streamStatus === "ok" ? "ok" : "unknown";
  return component?.status ?? "unknown";
}

export function PipelineHealth() {
  const healthQuery = useHealth();
  const configQuery = usePipelineConfig();
  const runsQuery = useRuns();
  // Only one run can be running: the latest one, and only while the stream's
  // heartbeat is fresh. A run with no end time that is not live was
  // interrupted (a crash, a killed test) -- calling it "running" would be false.
  const streamLive =
    healthQuery.data?.data.some((c) => c.name === "spark_stream" && c.status === "ok") ?? false;
  const liveRunId = streamLive ? runsQuery.data?.data[0]?._id : undefined;

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold">Pipeline Health</h1>

      <AsyncBoundary query={healthQuery} loading={<LoadingCard lines={8} title="component health" />}>
        {(components) => {
          const byName = new Map(components.map((c) => [c.name, c]));
          const streamStatus = byName.get("spark_stream")?.status ?? "unknown";

          const reconciliation = byName.get("reconciliation");

          return (
            <>
            {reconciliation ? (
              <section className="card mb-4 p-4" aria-labelledby="reconcile-heading">
                <div className="flex flex-wrap items-center gap-3">
                  <h2 id="reconcile-heading" className="text-sm font-medium">
                    Data correctness (§12.5)
                  </h2>
                  <StatusBadge status={reconciliation.status} />
                </div>
                <p className="mt-1 text-xs text-text-muted">
                  Window counts must sum to the number of scored records, no
                  window may be stored with a zero count, and every scored
                  record&rsquo;s run must exist. Divergence is shown here, never hidden.
                </p>
                {reconciliation.detail ? (
                  <p className="num mt-2 break-words text-xs text-text">{reconciliation.detail}</p>
                ) : null}
              </section>
            ) : null}
            <ol className="space-y-2" aria-label="Pipeline components in data-flow order">
              {STAGES.map((stage, index) => {
                const component = stage.component ? byName.get(stage.component) : undefined;
                const status = statusFor(stage.id, component, streamStatus);
                return (
                  <li key={stage.id}>
                    <div className="card grid gap-3 p-4 md:grid-cols-[1fr_auto] md:items-center">
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-3">
                          <span className="num text-xs text-text-faint">{index + 1}</span>
                          <h2 className="text-sm font-medium">{stage.title}</h2>
                          <StatusBadge status={status} />
                        </div>
                        <p className="mt-1 text-xs text-text-muted">{stage.detail}</p>
                        {component?.detail ? (
                          <p className="num mt-1 break-words text-xs text-text-faint">{component.detail}</p>
                        ) : null}
                      </div>
                      <div className="flex flex-col gap-1 text-xs md:items-end">
                        <span className="num text-text-muted">
                          last contact: {component?.last_contact ? relativeAge(component.last_contact) : "—"}
                        </span>
                        <code className="num rounded bg-raised px-2 py-1 text-accent">{stage.command}</code>
                      </div>
                    </div>
                    {index < STAGES.length - 1 ? (
                      <div className="py-0.5 text-center text-text-faint" aria-hidden="true">↓</div>
                    ) : null}
                  </li>
                );
              })}
            </ol>
            </>
          );
        }}
      </AsyncBoundary>

      <section className="card min-w-0 p-4" aria-labelledby="config-heading">
        <h2 id="config-heading" className="text-sm font-medium">Live configuration</h2>
        {configQuery.data?.data ? (
          <dl className="num mt-3 grid gap-2 text-xs sm:grid-cols-2">
            {[
              ["storage backend", configQuery.data.data.storage.backend],
              ["lake root", configQuery.data.data.storage.root],
              ["spark master", configQuery.data.data.spark.master],
              ["driver memory", configQuery.data.data.spark.driver_memory],
              ["mongo", configQuery.data.data.mongo_uri],
              ["kafka", configQuery.data.data.kafka_bootstrap],
              ["display timezone", configQuery.data.data.display_timezone],
            ].map(([label, value]) => (
              <div key={label} className="flex justify-between gap-4 border-b border-border/50 py-1">
                <dt className="text-text-muted">{label}</dt>
                <dd className="truncate text-text">{value}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="mt-2 text-sm text-text-muted">Configuration unavailable.</p>
        )}
      </section>

      <section className="card min-w-0 p-4" aria-labelledby="runs-heading">
        <h2 id="runs-heading" className="text-sm font-medium">Run history</h2>
        {(runsQuery.data?.data.length ?? 0) === 0 ? (
          <p className="mt-2 text-sm text-text-muted">
            No streaming runs recorded. Seeded data has no run, by design.
          </p>
        ) : (
          <div className="mt-3 overflow-x-auto">
            <table className="num w-full min-w-[420px] text-xs">
            <thead className="text-text-faint">
              <tr className="border-b border-border">
                <th scope="col" className="py-1 text-left">Run</th>
                <th scope="col" className="py-1 text-left">Source</th>
                <th scope="col" className="py-1 text-left">Started</th>
                <th scope="col" className="py-1 text-left">Ended</th>
              </tr>
            </thead>
            <tbody>
              {runsQuery.data?.data.map((run) => (
                <tr key={run._id} className="border-b border-border/40">
                  <td className="py-1">{run._id}</td>
                  <td className="py-1">{run.input_source}</td>
                  <td className="py-1">{formatDateTime(run.started_at)}</td>
                  <td className="py-1">
                    {run.ended_at
                      ? formatDateTime(run.ended_at)
                      : run._id === liveRunId
                        ? "running"
                        : "no end recorded"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        )}
      </section>
    </div>
  );
}

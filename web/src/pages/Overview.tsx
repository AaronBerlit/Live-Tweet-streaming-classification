/**
 * Overview (PRD §9.2 page 1).
 *
 * KPI row, stacked-area time chart with a range selector, hashtag bar chart
 * segmented by sentiment, and a live strip of the last 8 scored records.
 *
 * The sentiment split is a stacked bar, not a pie: the PRD asks for it
 * explicitly, and a two-slice pie is harder to read than the bar it replaces.
 */

import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { AsyncBoundary, EmptyState, LoadingCard, Skeleton } from "../components/States";
import { useHashtags, useMode, useScored, useStream, useSummary, useWindows } from "../hooks/useApi";
import {
  SENTIMENT_COLOR,
  SENTIMENT_MARK,
  formatDecimal,
  formatNumber,
  formatTime,
} from "../lib/format";
import type { HashtagRow, Scored, Summary, Window } from "../lib/types";

const RANGES = [
  { hours: 1, label: "1h" },
  { hours: 6, label: "6h" },
  { hours: 24, label: "24h" },
];

interface ChartPoint {
  windowStart: string;
  positive: number;
  negative: number;
  neutral: number;
}

/** Pivot one row per (window, prediction) into one row per window. */
function toChartPoints(windows: Window[]): ChartPoint[] {
  const byWindow = new Map<string, ChartPoint>();
  for (const row of windows) {
    const point = byWindow.get(row.window_start) ?? {
      windowStart: row.window_start,
      positive: 0,
      negative: 0,
      neutral: 0,
    };
    point[row.prediction] = row.count;
    byWindow.set(row.window_start, point);
  }
  return [...byWindow.values()].sort((a, b) =>
    a.windowStart.localeCompare(b.windowStart),
  );
}

function KpiCard({
  label,
  value,
  detail,
  loading,
}: {
  label: string;
  value: string;
  detail?: string;
  loading?: boolean;
}) {
  return (
    <div className="card min-w-0 p-4">
      <p className="text-xs uppercase tracking-wide text-text-faint">{label}</p>
      {loading ? (
        <Skeleton className="mt-2 h-7 w-24" />
      ) : (
        <p className="num mt-1 text-2xl font-semibold text-text">{value}</p>
      )}
      {detail ? <p className="mt-1 text-xs text-text-muted">{detail}</p> : null}
    </div>
  );
}

function SentimentSplitBar({ summary }: { summary: Summary }) {
  const total = summary.total_classified || 1;
  const order: Array<"positive" | "negative" | "neutral"> = [
    "positive",
    "negative",
    "neutral",
  ];
  const present = order.filter((key) => (summary.by_prediction[key] ?? 0) > 0);

  return (
    <div className="card min-w-0 p-4">
      <p className="text-xs uppercase tracking-wide text-text-faint">
        Sentiment split
      </p>
      <div
        className="mt-3 flex h-3 overflow-hidden rounded"
        role="img"
        aria-label={present
          .map(
            (key) =>
              `${key} ${(((summary.by_prediction[key] ?? 0) / total) * 100).toFixed(1)}%`,
          )
          .join(", ")}
      >
        {present.map((key) => (
          <div
            key={key}
            style={{
              width: `${((summary.by_prediction[key] ?? 0) / total) * 100}%`,
              background: SENTIMENT_COLOR[key],
            }}
          />
        ))}
      </div>
      <dl className="mt-3 flex flex-wrap gap-4 text-xs">
        {present.map((key) => (
          <div key={key} className="flex items-center gap-1.5">
            <span style={{ color: SENTIMENT_COLOR[key] }} aria-hidden="true">
              {SENTIMENT_MARK[key]}
            </span>
            <dt className="text-text-muted capitalize">{key}</dt>
            <dd className="num text-text">
              {formatNumber(summary.by_prediction[key] ?? 0)}
            </dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

function HashtagChart({ rows }: { rows: HashtagRow[] }) {
  const data = rows.map((row) => ({
    tag: `#${row.tag}`,
    positive: row.sentiment_split.positive,
    negative: row.sentiment_split.negative,
    neutral: row.sentiment_split.neutral,
  }));

  return (
    <ResponsiveContainer width="100%" height={Math.max(220, data.length * 26)}>
      <BarChart data={data} layout="vertical" margin={{ left: 8, right: 16 }}>
        <CartesianGrid stroke="var(--border)" horizontal={false} />
        <XAxis type="number" stroke="var(--text-faint)" fontSize={11} />
        <YAxis
          type="category"
          dataKey="tag"
          stroke="var(--text-faint)"
          fontSize={11}
          width={96}
        />
        <Tooltip
          contentStyle={{
            background: "var(--raised)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            fontSize: 12,
          }}
        />
        {(["positive", "negative", "neutral"] as const)
          .filter((name) => data.some((row) => row[name] > 0))
          .map((name) => (
            <Bar key={name} dataKey={name} stackId="s" fill={SENTIMENT_COLOR[name]} />
          ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

function LiveStrip({ records }: { records: Scored[] }) {
  return (
    <ul className="divide-y divide-border">
      {records.map((record) => (
        <li key={record._id} className="flex items-start gap-3 py-2">
          <span
            style={{ color: SENTIMENT_COLOR[record.prediction] }}
            className="num text-xs"
            aria-hidden="true"
          >
            {SENTIMENT_MARK[record.prediction]}
          </span>
          <div className="min-w-0 flex-1">
            {/* React escapes this; tweet text is untrusted input. */}
            <p className="truncate text-sm text-text">{record.text_raw}</p>
            <p className="num mt-0.5 text-xs text-text-faint">
              {formatTime(record.event_time)} · {record.prediction} ·{" "}
              {formatDecimal(record.confidence)}
            </p>
          </div>
        </li>
      ))}
    </ul>
  );
}

export function Overview() {
  const [hours, setHours] = useState(6);
  const navigate = useNavigate();
  const summaryQuery = useSummary(hours);
  const windowsQuery = useWindows(hours);
  const hashtagsQuery = useHashtags(hours);
  const scoredQuery = useScored({ limit: 8 });

  const summary = summaryQuery.data?.data;
  const points = useMemo(
    () => toChartPoints(windowsQuery.data?.data ?? []),
    [windowsQuery.data],
  );
  // Only classes that actually occur are drawn. Under neutral_strategy
  // "none" there is no neutral class, and a permanently-zero neutral series
  // would falsely imply the model looked for neutral records and found none
  // (PRD §6.3).
  const classes = useMemo(() => {
    const seen = new Set((windowsQuery.data?.data ?? []).map((row) => row.prediction));
    return (["negative", "positive", "neutral"] as const).filter((name) => seen.has(name));
  }, [windowsQuery.data]);

  // Throughput is Spark's own measurement (C23), never derived here.
  // Dividing records by the event-time span would report the replay's
  // time-compressed event rate, not what the pipeline actually processes.
  const streamQuery = useStream();
  const modeQuery = useMode();
  const live = modeQuery.data?.data.mode === "LIVE";
  const heartbeat = streamQuery.data?.data.heartbeat;
  const throughput = live && heartbeat ? heartbeat.rows_per_sec : null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4">
        <h1 className="text-lg font-semibold">Overview</h1>
        <div
          className="flex gap-1 rounded border border-border p-0.5"
          role="group"
          aria-label="Time range"
        >
          {RANGES.map((range) => (
            <button
              key={range.hours}
              type="button"
              onClick={() => setHours(range.hours)}
              aria-pressed={hours === range.hours}
              className={`num rounded px-2.5 py-1 text-xs transition ${
                hours === range.hours
                  ? "bg-raised text-text"
                  : "text-text-muted hover:text-text"
              }`}
            >
              {range.label}
            </button>
          ))}
        </div>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCard
          label="Total classified"
          value={formatNumber(summary?.total_classified)}
          detail={`last ${hours}h`}
          loading={summaryQuery.isPending}
        />
        {summary ? (
          <SentimentSplitBar summary={summary} />
        ) : (
          <KpiCard label="Sentiment split" value="—" loading={summaryQuery.isPending} />
        )}
        <KpiCard
          label="Throughput"
          value={throughput !== null ? `${formatDecimal(throughput, 1)}/s` : "—"}
          detail={live ? "Spark, latest micro-batch" : "no live stream"}
          loading={streamQuery.isPending}
        />
        <KpiCard
          label="Window span"
          value={points.length ? `${points.length} windows` : "—"}
          detail={
            summary?.window_start
              ? `${formatTime(summary.window_start)} → ${formatTime(summary.window_end)}`
              : undefined
          }
          loading={windowsQuery.isPending}
        />
      </div>

      <section className="card min-w-0 p-4" aria-labelledby="volume-heading">
        <h2 id="volume-heading" className="text-sm font-medium">
          Classified volume by one-minute event-time window
        </h2>
        <p className="mt-1 text-xs text-text-muted">
          Click any point to open the records behind that window.
        </p>
        <AsyncBoundary
          query={windowsQuery}
          isEmpty={(rows) => rows.length === 0}
          loading={<LoadingCard lines={6} title="the volume chart" />}
          empty={
            <EmptyState
              title="No windows in this range"
              detail="Nothing has been classified in the selected period. Widen the range, or start the pipeline."
              command="make demo-socket"
            />
          }
        >
          {() => (
            <>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart
                  data={points}
                  margin={{ top: 12, right: 8, left: -12 }}
                  // Click-to-drill (DoD check 8): every number on this chart
                  // opens the MongoDB documents behind that window.
                  onClick={(state) => {
                    const label = state?.activeLabel;
                    if (typeof label === "string") {
                      navigate(`/records?window_start=${encodeURIComponent(label)}`);
                    }
                  }}
                  style={{ cursor: "pointer" }}
                >
                  <CartesianGrid stroke="var(--border)" vertical={false} />
                  <XAxis
                    dataKey="windowStart"
                    tickFormatter={formatTime}
                    stroke="var(--text-faint)"
                    fontSize={11}
                    minTickGap={48}
                  />
                  <YAxis stroke="var(--text-faint)" fontSize={11} />
                  <Tooltip
                    labelFormatter={formatTime}
                    contentStyle={{
                      background: "var(--raised)",
                      border: "1px solid var(--border)",
                      borderRadius: 8,
                      fontSize: 12,
                    }}
                  />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  {classes.map((name) => (
                    <Area
                      key={name}
                      type="monotone"
                      dataKey={name}
                      stackId="1"
                      stroke={SENTIMENT_COLOR[name]}
                      fill={SENTIMENT_COLOR[name]}
                      fillOpacity={0.25}
                      isAnimationActive={false}
                    />
                  ))}
                </AreaChart>
              </ResponsiveContainer>

              {/* Every chart has an accessible table fallback (WCAG 2.1 AA). */}
              <details className="mt-3">
                <summary className="cursor-pointer text-xs text-text-muted">
                  View as table
                </summary>
                <table className="num mt-2 w-full text-xs">
                  <caption className="sr-only">
                    Classified volume per one-minute window
                  </caption>
                  <thead className="text-text-faint">
                    <tr>
                      <th scope="col" className="py-1 text-left">Window</th>
                      {classes.map((name) => (
                        <th key={name} scope="col" className="py-1 text-right capitalize">
                          {name}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {points.slice(-30).map((point) => (
                      <tr key={point.windowStart} className="border-t border-border">
                        <td className="py-1">{formatTime(point.windowStart)}</td>
                        {classes.map((name) => (
                          <td key={name} className="py-1 text-right">
                            {formatNumber(point[name])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </details>
            </>
          )}
        </AsyncBoundary>
      </section>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="card min-w-0 p-4" aria-labelledby="hashtags-heading">
          <h2 id="hashtags-heading" className="text-sm font-medium">
            Top hashtags by sentiment
          </h2>
          <AsyncBoundary
            query={hashtagsQuery}
            isEmpty={(rows) => rows.length === 0}
            loading={<LoadingCard lines={5} title="hashtags" />}
            empty={
              <EmptyState
                title="No hashtags in this range"
                detail="Hashtags are extracted from cleaned text during preprocessing."
              />
            }
          >
            {(rows) => <HashtagChart rows={rows} />}
          </AsyncBoundary>
        </section>

        <section className="card min-w-0 p-4" aria-labelledby="live-heading">
          <h2 id="live-heading" className="text-sm font-medium">
            Latest classified records
          </h2>
          <AsyncBoundary
            query={scoredQuery}
            isEmpty={(page) => page.records.length === 0}
            loading={<LoadingCard lines={6} title="recent records" />}
            empty={
              <EmptyState
                title="No scored records"
                detail="Records appear here as the streaming job classifies them."
                command="make demo-socket"
              />
            }
          >
            {(page) => <LiveStrip records={page.records} />}
          </AsyncBoundary>
        </section>
      </div>
    </div>
  );
}

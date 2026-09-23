/**
 * Raw Records (PRD §9.2 page 5).
 *
 * Every number on the Overview is traceable to the documents listed here
 * (DoD check 8). Expanding a row shows `text_raw` beside `text_clean`, which
 * is what demonstrates that preprocessing actually ran (C7).
 *
 * Virtualised by hand: only the rows in view are rendered, so scrolling stays
 * smooth at 10k rows without pulling in a virtualisation library.
 * Pagination is the API's keyset cursor, so rows never duplicate or vanish
 * as new records arrive mid-scroll.
 */

import { useInfiniteQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { EmptyState, ErrorState, LoadingCard, Warnings } from "../components/States";
import { api } from "../lib/api";
import { SENTIMENT_COLOR, SENTIMENT_MARK, formatDateTime, formatDecimal } from "../lib/format";
import type { Scored } from "../lib/types";

const ROW_HEIGHT = 44;
const VIEWPORT_HEIGHT = 560;
const OVERSCAN = 8;
const PAGE_SIZE = 200;

function toCsv(records: Scored[]): string {
  const headers = [
    "event_time", "prediction", "confidence", "text_raw", "text_clean",
    "hashtags", "dedup_key", "run_id", "source",
  ];
  const escape = (value: string) => `"${value.replace(/"/g, '""')}"`;
  const rows = records.map((r) =>
    [
      r.event_time, r.prediction, String(r.confidence), r.text_raw, r.text_clean,
      r.hashtags.join(" "), r.dedup_key, r.run_id, r.source,
    ].map(escape).join(","),
  );
  return [headers.join(","), ...rows].join("\n");
}

function download(records: Scored[]) {
  const blob = new Blob([toCsv(records)], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `scored-${new Date().toISOString().slice(0, 19)}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

function Detail({ record, onClose }: { record: Scored; onClose: () => void }) {
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div className="card mt-4 p-4" role="region" aria-label="Record detail">
      <div className="flex items-start justify-between gap-4">
        <h2 className="text-sm font-medium">Record detail</h2>
        <button
          type="button"
          onClick={onClose}
          className="rounded px-2 py-1 text-xs text-text-muted hover:bg-raised"
        >
          Close (Esc)
        </button>
      </div>
      <div className="mt-3 grid gap-4 md:grid-cols-2">
        <div>
          <p className="text-xs uppercase tracking-wide text-text-faint">text_raw</p>
          {/* Rendered as text: React escapes it. Tweet text is untrusted input. */}
          <p className="mt-1 whitespace-pre-wrap break-words rounded bg-raised p-3 text-sm">
            {record.text_raw}
          </p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-wide text-text-faint">
            text_clean (after preprocessing)
          </p>
          <p className="num mt-1 whitespace-pre-wrap break-words rounded bg-raised p-3 text-sm">
            {record.text_clean || <span className="text-text-faint">(empty after cleaning)</span>}
          </p>
        </div>
      </div>
      <dl className="num mt-4 grid gap-2 text-xs sm:grid-cols-2 lg:grid-cols-3">
        {[
          ["prediction", `${SENTIMENT_MARK[record.prediction]} ${record.prediction}`],
          ["confidence", formatDecimal(record.confidence, 4)],
          ["hashtags", record.hashtags.length ? record.hashtags.map((t) => `#${t}`).join(" ") : "—"],
          ["event_time", formatDateTime(record.event_time)],
          ["ingest_time", formatDateTime(record.ingest_time)],
          ["source", record.source],
          ["run_id", record.run_id],
          ["dedup_key", record.dedup_key],
        ].map(([label, value]) => (
          <div key={label} className="flex justify-between gap-3 border-b border-border/50 py-1">
            <dt className="text-text-muted">{label}</dt>
            <dd className="truncate text-text" title={value}>{value}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

export function RawRecords() {
  const [searchParams, setSearchParams] = useSearchParams();
  const prediction = searchParams.get("prediction") ?? "";
  const windowStart = searchParams.get("window_start") ?? "";
  const [draft, setDraft] = useState(searchParams.get("q") ?? "");
  const q = searchParams.get("q") ?? "";
  const [selected, setSelected] = useState<Scored | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const searchRef = useRef<HTMLInputElement>(null);

  const query = useInfiniteQuery({
    queryKey: ["scored-infinite", { prediction, q, windowStart }],
    queryFn: ({ pageParam }) =>
      api.scored({
        prediction: prediction || undefined,
        q: q || undefined,
        window_start: windowStart || undefined,
        cursor: pageParam ?? undefined,
        limit: PAGE_SIZE,
      }),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.data.next_cursor,
  });

  const records = useMemo(
    () => query.data?.pages.flatMap((page) => page.data.records) ?? [],
    [query.data],
  );
  const warnings = query.data?.pages[0]?.warnings ?? [];

  // `/` focuses search (PRD §9.2 keyboard rule).
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement;
      if (event.key === "/" && target.tagName !== "INPUT" && target.tagName !== "TEXTAREA") {
        event.preventDefault();
        searchRef.current?.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const first = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const last = Math.min(
    records.length,
    Math.ceil((scrollTop + VIEWPORT_HEIGHT) / ROW_HEIGHT) + OVERSCAN,
  );
  const visible = records.slice(first, last);

  const onScroll = (event: React.UIEvent<HTMLDivElement>) => {
    const element = event.currentTarget;
    setScrollTop(element.scrollTop);
    const nearBottom =
      element.scrollHeight - element.scrollTop - element.clientHeight < ROW_HEIGHT * 20;
    if (nearBottom && query.hasNextPage && !query.isFetchingNextPage) {
      void query.fetchNextPage();
    }
  };

  const setParam = (key: string, value: string) => {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next, { replace: true });
    setScrollTop(0);
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <h1 className="text-lg font-semibold">Raw Records</h1>
        <button
          type="button"
          onClick={() => download(records)}
          disabled={records.length === 0}
          className="rounded border border-border px-3 py-1.5 text-sm text-accent transition hover:bg-raised disabled:opacity-40"
        >
          Export {records.length.toLocaleString("en-IN")} loaded rows as CSV
        </button>
      </div>

      <form
        className="card flex flex-wrap items-end gap-3 p-3"
        onSubmit={(event) => {
          event.preventDefault();
          setParam("q", draft.trim());
        }}
      >
        <label className="flex min-w-[200px] flex-1 flex-col gap-1 text-xs text-text-muted">
          Search text (press / to focus)
          <input
            ref={searchRef}
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            className="rounded border border-border bg-raised px-2 py-1.5 text-sm text-text"
            placeholder="e.g. metro"
          />
        </label>
        <label className="flex flex-col gap-1 text-xs text-text-muted">
          Prediction
          <select
            value={prediction}
            onChange={(event) => setParam("prediction", event.target.value)}
            className="rounded border border-border bg-raised px-2 py-1.5 text-sm text-text"
          >
            <option value="">all</option>
            <option value="positive">positive</option>
            <option value="negative">negative</option>
            <option value="neutral">neutral</option>
          </select>
        </label>
        <button
          type="submit"
          className="rounded bg-raised px-3 py-1.5 text-sm text-text hover:bg-border"
        >
          Search
        </button>
        {windowStart ? (
          <button
            type="button"
            onClick={() => setParam("window_start", "")}
            className="num rounded border border-accent/40 px-2 py-1 text-xs text-accent"
          >
            window {formatDateTime(windowStart)} ✕
          </button>
        ) : null}
      </form>

      <Warnings warnings={warnings} />

      {query.isPending ? (
        <LoadingCard lines={10} title="records" />
      ) : query.isError ? (
        <ErrorState
          detail={query.error instanceof Error ? query.error.message : "Request failed."}
          onRetry={() => void query.refetch()}
        />
      ) : records.length === 0 ? (
        <EmptyState
          title="No records match"
          detail="Either nothing has been classified yet, or the filters exclude everything. Scored records are kept for 24 hours."
        />
      ) : (
        <div className="card overflow-hidden">
          <div
            className="num grid grid-cols-[110px_90px_70px_1fr] gap-3 border-b border-border px-3 py-2 text-xs text-text-faint"
            role="row"
          >
            <span>Event time</span>
            <span>Prediction</span>
            <span className="text-right">Conf.</span>
            <span>Text</span>
          </div>
          <div
            style={{ height: VIEWPORT_HEIGHT }}
            className="overflow-y-auto"
            onScroll={onScroll}
            role="table"
            aria-label="Scored records"
            aria-rowcount={records.length}
          >
            <div style={{ height: records.length * ROW_HEIGHT, position: "relative" }}>
              {visible.map((record, offset) => {
                const index = first + offset;
                return (
                  <button
                    key={record._id}
                    type="button"
                    role="row"
                    aria-rowindex={index + 1}
                    onClick={() => setSelected(record)}
                    style={{ position: "absolute", top: index * ROW_HEIGHT, height: ROW_HEIGHT }}
                    className={`grid w-full grid-cols-[110px_90px_70px_1fr] items-center gap-3 border-b border-border/40 px-3 text-left text-sm transition hover:bg-raised ${
                      selected?._id === record._id ? "bg-raised" : ""
                    }`}
                  >
                    <span className="num text-xs text-text-muted">
                      {formatDateTime(record.event_time).slice(-8)}
                    </span>
                    <span className="flex items-center gap-1.5 text-xs">
                      <span style={{ color: SENTIMENT_COLOR[record.prediction] }} aria-hidden="true">
                        {SENTIMENT_MARK[record.prediction]}
                      </span>
                      {record.prediction}
                    </span>
                    <span className="num text-right text-xs">{formatDecimal(record.confidence)}</span>
                    <span className="truncate">{record.text_raw}</span>
                  </button>
                );
              })}
            </div>
          </div>
          <p className="num border-t border-border px-3 py-2 text-xs text-text-faint">
            {records.length.toLocaleString("en-IN")} loaded
            {query.hasNextPage ? " · scroll for more" : " · end of results"}
            {query.isFetchingNextPage ? " · loading…" : ""}
          </p>
        </div>
      )}

      {selected ? <Detail record={selected} onClose={() => setSelected(null)} /> : null}
    </div>
  );
}

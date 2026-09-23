/**
 * The only place this app talks to the backend.
 *
 * REST is the source of truth; the WebSocket only signals freshness. On
 * reconnect the app re-fetches rather than replaying missed messages, because
 * a replayed gap is indistinguishable from a wrong one (PRD §9.2).
 */

import type {
  ComponentHealth,
  Envelope,
  HashtagRow,
  Metrics,
  ModeInfo,
  PipelineConfig,
  Run,
  ScoredPage,
  StreamState,
  StorageInfo,
  Summary,
  Window,
} from "./types";

/** A failed request that still carries something the UI can show a user. */
export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

/** Warnings ride alongside data; the caller decides whether to surface them. */
export interface Result<T> {
  data: T;
  warnings: string[];
  mode: Envelope<T>["mode"];
  serverTime: string;
}

async function request<T>(path: string, fallback: T): Promise<Result<T>> {
  let response: Response;
  try {
    response = await fetch(path, { headers: { Accept: "application/json" } });
  } catch (cause) {
    // The API itself is unreachable -- distinct from the API reporting that
    // Mongo is unreachable, and the two need different copy on screen.
    throw new ApiError(
      "Cannot reach the API. Start it with `make api`.",
      0,
    );
  }

  if (!response.ok) {
    throw new ApiError(`Request to ${path} failed`, response.status);
  }

  const envelope = (await response.json()) as Envelope<T>;
  return {
    // `data: null` is the documented degraded response, not an error.
    data: envelope.data ?? fallback,
    warnings: envelope.warnings ?? [],
    mode: envelope.mode,
    serverTime: envelope.server_time,
  };
}

function query(params: Record<string, string | number | undefined | null>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      search.set(key, String(value));
    }
  }
  const rendered = search.toString();
  return rendered ? `?${rendered}` : "";
}

export const api = {
  health: () => request<ComponentHealth[]>("/api/health", []),

  mode: () =>
    request<ModeInfo>("/api/mode", {
      mode: "EMPTY",
      reason: "The API did not report a mode.",
      heartbeat_age_seconds: null,
    }),

  summary: (hours = 6) =>
    request<Summary>(`/api/summary${query({ hours })}`, {
      total_classified: 0,
      by_prediction: {},
      avg_confidence: null,
      window_start: null,
      window_end: null,
    }),

  windows: (params: { from?: string; to?: string; hours?: number; run_id?: string; limit?: number } = {}) =>
    request<Window[]>(`/api/windows${query(params)}`, []),

  hashtags: (params: { from?: string; to?: string; hours?: number; limit?: number } = {}) =>
    request<HashtagRow[]>(`/api/hashtags${query(params)}`, []),

  scored: (
    params: {
      window_start?: string;
      prediction?: string;
      q?: string;
      cursor?: string;
      limit?: number;
    } = {},
  ) =>
    request<ScoredPage>(`/api/scored${query(params)}`, {
      records: [],
      next_cursor: null,
    }),

  stream: () =>
    request<StreamState>("/api/stream", {
      heartbeat: null,
      batches: [],
      run: null,
    }),

  /** An empty array means "not yet evaluated" -- never render it as zeros. */
  metrics: () => request<Metrics[]>("/api/metrics", []),

  runs: (limit = 50) => request<Run[]>(`/api/runs${query({ limit })}`, []),

  pipelineConfig: () => request<PipelineConfig | null>("/api/pipeline/config", null),

  storage: () => request<StorageInfo | null>("/api/storage", null),
};

/** Messages the backend pushes over /ws/stream. */
export type StreamEvent =
  | { type: "heartbeat" }
  | { type: "window_update" }
  | { type: "mode_change" };

/**
 * Subscribe to the live stream.
 *
 * Reconnects with exponential backoff. Every event is treated purely as
 * "something changed, re-fetch" -- the payload is never merged into state,
 * so a missed message can never leave the UI holding a partial picture.
 */
export function subscribe(
  onEvent: (event: StreamEvent) => void,
  onStatus?: (connected: boolean) => void,
): () => void {
  let socket: WebSocket | null = null;
  let closed = false;
  let attempt = 0;
  let timer: number | undefined;

  const connect = () => {
    if (closed) return;
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    socket = new WebSocket(`${protocol}//${window.location.host}/ws/stream`);

    socket.onopen = () => {
      attempt = 0;
      onStatus?.(true);
    };

    socket.onmessage = (message) => {
      try {
        onEvent(JSON.parse(message.data as string) as StreamEvent);
      } catch {
        // A malformed frame is not worth tearing the connection down for.
      }
    };

    socket.onclose = () => {
      onStatus?.(false);
      if (closed) return;
      // 1s, 2s, 4s ... capped at 15s.
      const delay = Math.min(1000 * 2 ** attempt, 15_000);
      attempt += 1;
      timer = window.setTimeout(connect, delay);
    };

    socket.onerror = () => socket?.close();
  };

  connect();

  return () => {
    closed = true;
    if (timer) window.clearTimeout(timer);
    socket?.close();
  };
}

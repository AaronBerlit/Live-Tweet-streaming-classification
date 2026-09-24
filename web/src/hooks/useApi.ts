/**
 * Query hooks. Every server read in the app goes through one of these.
 *
 * The WebSocket does not carry data -- it invalidates queries, and TanStack
 * Query re-fetches through REST. That keeps REST as the single source of
 * truth and makes a dropped frame harmless.
 */

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { api, subscribe } from "../lib/api";

export const keys = {
  health: ["health"] as const,
  mode: ["mode"] as const,
  summary: (hours: number) => ["summary", hours] as const,
  windows: (hours: number) => ["windows", hours] as const,
  hashtags: (hours: number, limit: number) => ["hashtags", hours, limit] as const,
  scored: (params: Record<string, string | number | undefined>) =>
    ["scored", params] as const,
  stream: ["stream"] as const,
  metrics: ["metrics"] as const,
  runs: ["runs"] as const,
  pipelineConfig: ["pipeline-config"] as const,
  storage: ["storage"] as const,
};


export const useHealth = () =>
  useQuery({
    queryKey: keys.health,
    queryFn: api.health,
    refetchInterval: 10_000,
  });

export const useMode = () =>
  useQuery({
    queryKey: keys.mode,
    queryFn: api.mode,
    // The badge must notice a dead pipeline quickly; 15s is the LIVE
    // threshold, so polling faster keeps the badge honest between pushes.
    refetchInterval: 5_000,
  });

export const useSummary = (hours: number) =>
  useQuery({ queryKey: keys.summary(hours), queryFn: () => api.summary(hours) });

export const useWindows = (hours: number) =>
  useQuery({
    queryKey: keys.windows(hours),
    queryFn: () => api.windows({ hours }),
  });

export const useHashtags = (hours: number, limit = 12) =>
  useQuery({
    queryKey: keys.hashtags(hours, limit),
    queryFn: () => api.hashtags({ hours, limit }),
  });

export const useScored = (params: {
  window_start?: string;
  prediction?: string;
  q?: string;
  cursor?: string;
  limit?: number;
}) =>
  useQuery({ queryKey: keys.scored(params), queryFn: () => api.scored(params) });

export const useStream = () =>
  useQuery({
    queryKey: keys.stream,
    queryFn: api.stream,
    // The Stream Monitor is the page where staleness is the subject, so it
    // polls faster than the rest of the app.
    refetchInterval: 5_000,
  });

export const useMetrics = () =>
  useQuery({ queryKey: keys.metrics, queryFn: api.metrics });

export const useRuns = () => useQuery({ queryKey: keys.runs, queryFn: () => api.runs() });

export const usePipelineConfig = () =>
  useQuery({ queryKey: keys.pipelineConfig, queryFn: api.pipelineConfig });

export const useStorage = () =>
  useQuery({ queryKey: keys.storage, queryFn: api.storage });

/**
 * Connect the WebSocket and invalidate on every event.
 *
 * On reconnect we invalidate everything rather than trying to replay what was
 * missed -- re-fetching is cheap and always correct.
 */
export function useLiveUpdates(): { connected: boolean | undefined } {
  const queryClient = useQueryClient();
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const unsubscribe = subscribe(
      (event) => {
        if (event.type === "mode_change") {
          void queryClient.invalidateQueries({ queryKey: keys.mode });
          void queryClient.invalidateQueries({ queryKey: keys.health });
        }
        if (event.type === "heartbeat" || event.type === "window_update") {
          void queryClient.invalidateQueries({ queryKey: ["summary"] });
          void queryClient.invalidateQueries({ queryKey: ["windows"] });
          void queryClient.invalidateQueries({ queryKey: ["hashtags"] });
          void queryClient.invalidateQueries({ queryKey: ["scored"] });
          void queryClient.invalidateQueries({ queryKey: keys.health });
          void queryClient.invalidateQueries({ queryKey: keys.stream });
        }
      },
      (isConnected) => {
        setConnected(isConnected);
        if (isConnected) void queryClient.invalidateQueries();
      },
    );
    return unsubscribe;
  }, [queryClient]);

  return { connected };
}

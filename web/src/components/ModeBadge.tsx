/**
 * The source-mode badge (PRD §6.2, G6).
 *
 * Always visible, always derived from the backend's own computation. The
 * frontend never decides the mode itself -- if it did, the badge could
 * disagree with the data it sits above, which is precisely the
 * misrepresentation G6 exists to prevent.
 *
 * The reason text comes from the API too, so a viewer can see *why* the badge
 * says what it says rather than having to trust it.
 */

import type { Mode } from "../lib/types";

const PRESENTATION: Record<
  Mode,
  { label: string; className: string; description: string }
> = {
  LIVE: {
    label: "LIVE",
    className: "border-positive/50 bg-positive/10 text-positive",
    description: "Spark is processing micro-batches right now.",
  },
  REPLAY: {
    label: "REPLAY",
    className: "border-accent/50 bg-accent/10 text-accent",
    description: "Showing a completed pipeline run. The stream is not running.",
  },
  SEED: {
    label: "SEED",
    className: "border-warning/50 bg-warning/10 text-warning",
    description: "Showing seeded demonstration data. No pipeline has run.",
  },
  EMPTY: {
    label: "EMPTY",
    className: "border-border bg-raised text-text-muted",
    description: "No data in MongoDB yet.",
  },
};

export function ModeBadge({
  mode,
  reason,
  connected,
}: {
  mode: Mode;
  reason?: string;
  connected?: boolean;
}) {
  const presentation = PRESENTATION[mode];

  return (
    <div className="flex items-center gap-3">
      <span
        className={`num inline-flex items-center gap-2 rounded border px-2.5 py-1 text-xs font-semibold tracking-wide ${presentation.className}`}
        title={reason ?? presentation.description}
      >
        {/* A dot alone would convey state by colour only; the text label is
            what carries the meaning (WCAG 2.1 AA). */}
        <span
          className="inline-block h-1.5 w-1.5 rounded-full bg-current"
          aria-hidden="true"
        />
        {presentation.label}
      </span>

      <span className="hidden text-xs text-text-muted sm:inline">
        {reason ?? presentation.description}
      </span>

      {connected !== undefined ? (
        <span
          className="num text-xs text-text-faint"
          title={
            connected
              ? "Live updates connected"
              : "Live updates disconnected; falling back to polling"
          }
        >
          {connected ? "ws connected" : "ws reconnecting"}
        </span>
      ) : null}
    </div>
  );
}

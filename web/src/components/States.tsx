/**
 * Loading, empty and error states.
 *
 * PRD §9.2: every data-dependent component implements all three. A
 * success-only component is incomplete, because the states it omits are
 * exactly the ones a reviewer will see first on a cold start.
 *
 * Skeletons, not spinners: a skeleton shows the shape of what is coming, so
 * the layout does not jump when it arrives.
 */

import type { ReactNode } from "react";

export function Skeleton({ className = "" }: { className?: string }) {
  return (
    <div
      className={`animate-pulse rounded bg-raised ${className}`}
      aria-hidden="true"
    />
  );
}

export function LoadingCard({ lines = 3, title }: { lines?: number; title?: string }) {
  return (
    <div className="card p-4" role="status" aria-live="polite">
      <span className="sr-only">Loading{title ? ` ${title}` : ""}…</span>
      <Skeleton className="h-4 w-1/3 mb-4" />
      {Array.from({ length: lines }).map((_, index) => (
        <Skeleton key={index} className="h-3 w-full mb-2" />
      ))}
    </div>
  );
}

export function EmptyState({
  title,
  detail,
  command,
}: {
  title: string;
  detail: string;
  command?: string;
}) {
  return (
    <div className="card p-6 text-center">
      <p className="text-text font-medium">{title}</p>
      <p className="text-text-muted text-sm mt-2 max-w-md mx-auto">{detail}</p>
      {command ? (
        <code className="num mt-4 inline-block rounded bg-raised px-3 py-1.5 text-sm text-accent">
          {command}
        </code>
      ) : null}
    </div>
  );
}

export function ErrorState({
  title = "Something went wrong",
  detail,
  onRetry,
}: {
  title?: string;
  detail: string;
  onRetry?: () => void;
}) {
  return (
    <div className="card border-negative/40 p-6" role="alert">
      <p className="text-negative font-medium">{title}</p>
      <p className="text-text-muted text-sm mt-2">{detail}</p>
      {onRetry ? (
        <button
          type="button"
          onClick={onRetry}
          className="mt-4 rounded border border-border px-3 py-1.5 text-sm text-accent transition hover:bg-raised"
        >
          Retry
        </button>
      ) : null}
    </div>
  );
}

/** Non-fatal warnings from the response envelope. */
export function Warnings({ warnings }: { warnings: string[] }) {
  if (warnings.length === 0) return null;
  return (
    <div className="card border-warning/40 p-3 mb-4" role="status">
      <ul className="text-sm text-warning space-y-1">
        {warnings.map((warning) => (
          <li key={warning}>{warning}</li>
        ))}
      </ul>
    </div>
  );
}

/**
 * The one place that decides which of the four states to render.
 *
 * Centralising it means a new panel cannot accidentally ship with only the
 * success path implemented.
 */
export function AsyncBoundary<T>({
  query,
  isEmpty,
  empty,
  loading,
  children,
}: {
  query: {
    isPending: boolean;
    isError: boolean;
    error: unknown;
    data?: { data: T; warnings: string[] };
    refetch: () => void;
  };
  isEmpty?: (data: T) => boolean;
  empty?: ReactNode;
  loading?: ReactNode;
  children: (data: T, warnings: string[]) => ReactNode;
}) {
  if (query.isPending) return <>{loading ?? <LoadingCard />}</>;

  if (query.isError) {
    const message =
      query.error instanceof Error
        ? query.error.message
        : "The request failed for an unknown reason.";
    return <ErrorState detail={message} onRetry={query.refetch} />;
  }

  if (!query.data) {
    return <ErrorState detail="The API returned no response body." onRetry={query.refetch} />;
  }

  const { data, warnings } = query.data;
  if (isEmpty?.(data) && empty) return <>{empty}</>;

  return (
    <>
      <Warnings warnings={warnings} />
      {children(data, warnings)}
    </>
  );
}

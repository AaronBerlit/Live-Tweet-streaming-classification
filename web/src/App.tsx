/**
 * Application shell: navigation, the always-visible mode badge, and routing.
 *
 * In EMPTY mode the dashboard is replaced by the getting-started checklist
 * (PRD §9.2 page 6) -- showing empty charts would imply the pipeline ran and
 * found nothing, which is a different claim from "nothing has run yet".
 */

import { NavLink, Navigate, Route, Routes } from "react-router-dom";

import { ModeBadge } from "./components/ModeBadge";
import { ShortcutSheet } from "./components/ShortcutSheet";
import { ErrorState, LoadingCard } from "./components/States";
import { useLiveUpdates, useMode } from "./hooks/useApi";
import { DataStorage } from "./pages/DataStorage";
import { GettingStarted } from "./pages/GettingStarted";
import { ModelPage } from "./pages/ModelPage";
import { Overview } from "./pages/Overview";
import { PipelineHealth } from "./pages/PipelineHealth";
import { RawRecords } from "./pages/RawRecords";
import { StreamMonitor } from "./pages/StreamMonitor";

const NAV = [
  { to: "/overview", label: "Overview" },
  { to: "/stream", label: "Stream Monitor" },
  { to: "/model", label: "Model" },
  { to: "/storage", label: "Data & Storage" },
  { to: "/records", label: "Raw Records" },
  { to: "/health", label: "Pipeline Health" },
];

export default function App() {
  const { connected } = useLiveUpdates();
  const modeQuery = useMode();
  const modeInfo = modeQuery.data?.data;
  // No default mode. Until the backend has answered, the badge says so --
  // defaulting to EMPTY would flash "no data" over a populated database,
  // which is the misrepresentation G6 forbids.
  const mode = modeInfo?.mode;

  return (
    <div className="min-h-screen bg-bg">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded focus:bg-raised focus:px-3 focus:py-2"
      >
        Skip to content
      </a>

      <ShortcutSheet />

      <header className="border-b border-border bg-surface">
        <div className="mx-auto flex max-w-[1400px] flex-wrap items-center gap-4 px-4 py-3">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold">Sentiment Stream</span>
            <span className="num text-xs text-text-faint">BCSE402L</span>
          </div>
          <div className="ml-auto">
            {mode ? (
              <ModeBadge
                mode={mode}
                reason={modeInfo?.reason}
                connected={connected}
              />
            ) : (
              <span className="num rounded border border-border px-2.5 py-1 text-xs text-text-faint">
                {modeQuery.isError ? "API UNREACHABLE" : "checking…"}
              </span>
            )}
          </div>
        </div>

        <nav
          aria-label="Primary"
          className="mx-auto flex max-w-[1400px] gap-1 overflow-x-auto px-4 pb-2"
        >
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                `whitespace-nowrap rounded px-3 py-1.5 text-sm transition ${
                  isActive
                    ? "bg-raised text-text"
                    : "text-text-muted hover:bg-raised hover:text-text"
                }`
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </header>

      <main id="main" className="mx-auto max-w-[1400px] px-4 py-6">
        {modeQuery.isError ? (
          <ErrorState
            title="Cannot reach the API"
            detail="The dashboard reads everything through the backend. Start it with `make api`, then retry."
            onRetry={() => void modeQuery.refetch()}
          />
        ) : !mode ? (
          <LoadingCard lines={6} title="pipeline state" />
        ) : mode === "EMPTY" ? (
          <GettingStarted />
        ) : (
          <Routes>
            <Route path="/" element={<Navigate to="/overview" replace />} />
            <Route path="/overview" element={<Overview />} />
            <Route path="/stream" element={<StreamMonitor />} />
            <Route path="/model" element={<ModelPage />} />
            <Route path="/storage" element={<DataStorage />} />
            <Route path="/records" element={<RawRecords />} />
            <Route path="/health" element={<PipelineHealth />} />
            <Route path="*" element={<Navigate to="/overview" replace />} />
          </Routes>
        )}
      </main>
    </div>
  );
}

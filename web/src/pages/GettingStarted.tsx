/**
 * EMPTY-mode onboarding (PRD §9.2 page 6).
 *
 * In EMPTY mode this replaces the dashboard entirely. Rendering empty charts
 * instead would imply the pipeline ran and found nothing — a different and
 * false claim. The steps tick off as they become satisfied, so the checklist
 * reflects real state rather than being a static page of instructions.
 */

import { useHealth, useMetrics, useStorage } from "../hooks/useApi";

interface Step {
  id: string;
  title: string;
  detail: string;
  command: string;
  done: boolean;
  pending: boolean;
}

function StepRow({ step, index }: { step: Step; index: number }) {
  return (
    <li className="flex gap-4 py-4">
      <span
        className={`num flex h-7 w-7 shrink-0 items-center justify-center rounded-full border text-xs ${
          step.done
            ? "border-positive/50 bg-positive/10 text-positive"
            : "border-border bg-raised text-text-faint"
        }`}
        aria-hidden="true"
      >
        {step.done ? "✓" : index + 1}
      </span>
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-text">
          {step.title}
          <span className="sr-only">{step.done ? " — done" : " — not yet done"}</span>
        </p>
        <p className="mt-1 text-sm text-text-muted">{step.detail}</p>
        <code className="num mt-2 inline-block rounded bg-raised px-2.5 py-1 text-xs text-accent">
          {step.command}
        </code>
      </div>
      <span className="num shrink-0 self-start text-xs text-text-faint">
        {step.pending ? "checking…" : step.done ? "done" : "pending"}
      </span>
    </li>
  );
}

export function GettingStarted() {
  const healthQuery = useHealth();
  const storageQuery = useStorage();
  const metricsQuery = useMetrics();

  const components = healthQuery.data?.data ?? [];
  const mongoOk = components.some(
    (component) => component.name === "mongo" && component.status === "ok",
  );
  const storage = storageQuery.data?.data;
  const lakeHasData = (storage?.lake.length ?? 0) > 0;
  const hasModel =
    storage?.lake.some((entry) => entry.path.includes("/models/")) ?? false;
  const hasMetrics = (metricsQuery.data?.data.length ?? 0) > 0;

  const steps: Step[] = [
    {
      id: "services",
      title: "Start the services",
      detail:
        "MongoDB, Kafka and HDFS run in Docker with pinned versions, then the Mongo indexes and lake directories are created.",
      command: "make setup",
      done: mongoOk,
      pending: healthQuery.isPending,
    },
    {
      id: "seed",
      title: "See the dashboard with no pipeline running",
      detail:
        "Writes seeded aggregates into real MongoDB, read back through the real query path. The badge will read SEED.",
      command: "make demo-seed",
      done: false,
      pending: false,
    },
    {
      id: "ingest",
      title: "Load the corpus into the lake",
      detail:
        "Downloads Sentiment140, profiles it into docs/dataset_profile.md, and writes Parquet to /sentiment/raw.",
      command: "make ingest && make clean-batch",
      done: lakeHasData,
      pending: storageQuery.isPending,
    },
    {
      id: "train",
      title: "Train and evaluate a model",
      detail:
        "Fits the full Tokenizer → StopWordsRemover → HashingTF → IDF → classifier pipeline and writes one real metrics document.",
      command: "make train",
      done: hasModel && hasMetrics,
      pending: metricsQuery.isPending,
    },
    {
      id: "stream",
      title: "Run the pipeline",
      detail:
        "Replays the corpus through a socket into Spark Structured Streaming. The badge flips to LIVE without a refresh.",
      command: "make demo-socket",
      done: false,
      pending: false,
    },
  ];

  return (
    <div className="mx-auto max-w-2xl">
      <div className="card p-6">
        <h1 className="text-lg font-semibold">Nothing to show yet</h1>
        <p className="mt-2 text-sm text-text-muted">
          MongoDB holds no windows, so there is nothing to chart. This checklist
          updates itself as each step completes — nothing here is hardcoded.
        </p>

        <ol className="mt-4 divide-y divide-border border-t border-border">
          {steps.map((step, index) => (
            <StepRow key={step.id} step={step} index={index} />
          ))}
        </ol>

        <p className="mt-4 border-t border-border pt-4 text-xs text-text-faint">
          The quickest path to a working dashboard is{" "}
          <code className="num text-accent">make setup &amp;&amp; make demo-seed</code>,
          which needs no pipeline and no trained model.
        </p>
      </div>
    </div>
  );
}

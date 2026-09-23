/**
 * Model page (PRD §9.2 page 3) — C9, C10, C11.
 *
 * The empty state here matters more than the populated one. When no model has
 * been evaluated the API returns an empty array and this page says
 * "not yet evaluated". It never renders zeros, because a zero accuracy is a
 * measurement and we have not made one (PRD rule 4).
 *
 * The neutral-strategy banner reads `neutral_strategy` off the metrics
 * document. Three classes are never hardcoded, and a permanently-zero neutral
 * series is never drawn.
 */

import { AsyncBoundary, EmptyState, LoadingCard } from "../components/States";
import { useMetrics } from "../hooks/useApi";
import {
  formatDateTime,
  formatDuration,
  formatNumber,
  formatPercent,
} from "../lib/format";
import type { Metrics } from "../lib/types";

function NeutralBanner({ metrics }: { metrics: Metrics }) {
  const copy: Record<string, string> = {
    none: "Binary classification. Sentiment140 contains only positive (4) and negative (0) labels, so no neutral class was trained. A neutral series is not shown because the model never looked for one.",
    threshold: `Records whose highest class probability fell below ${metrics.neutral_threshold} were relabelled neutral at inference time. The underlying model remains binary.`,
    external:
      "A supplementary labelled neutral set was incorporated into training.",
  };
  return (
    <div className="card border-accent/30 p-3">
      <p className="text-xs uppercase tracking-wide text-text-faint">
        Neutral-class policy: <span className="num text-accent">{metrics.neutral_strategy}</span>
      </p>
      <p className="mt-1 text-sm text-text-muted">{copy[metrics.neutral_strategy]}</p>
    </div>
  );
}

function ConfusionMatrix({ metrics }: { metrics: Metrics }) {
  const classes = Object.keys(metrics.precision);
  const max = Math.max(...metrics.confusion_matrix.flat(), 1);

  return (
    <div className="overflow-x-auto">
      <table className="num text-xs">
        <caption className="sr-only">
          Confusion matrix for {metrics.model_name}; rows are the actual class,
          columns the predicted class.
        </caption>
        <thead>
          <tr className="text-text-faint">
            <th scope="col" className="px-2 py-1 text-left">actual \ predicted</th>
            {classes.map((name) => (
              <th key={name} scope="col" className="px-3 py-1 text-right">
                {name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {metrics.confusion_matrix.map((row, rowIndex) => (
            <tr key={classes[rowIndex] ?? rowIndex}>
              <th scope="row" className="px-2 py-1 text-left font-normal text-text-muted">
                {classes[rowIndex] ?? rowIndex}
              </th>
              {row.map((value, columnIndex) => (
                <td
                  key={columnIndex}
                  className="px-3 py-1.5 text-right"
                  // Diagonal cells are correct predictions; intensity shows
                  // magnitude, and the number is always present so the cell
                  // never relies on colour alone.
                  style={{
                    background:
                      rowIndex === columnIndex
                        ? `rgba(52, 211, 153, ${(value / max) * 0.3})`
                        : `rgba(248, 113, 113, ${(value / max) * 0.3})`,
                  }}
                >
                  {formatNumber(value)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ModelPage() {
  const metricsQuery = useMetrics();

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold">Model</h1>

      <AsyncBoundary
        query={metricsQuery}
        isEmpty={(rows) => rows.length === 0}
        loading={<LoadingCard lines={6} title="model metrics" />}
        empty={
          <EmptyState
            title="No model evaluated yet"
            detail="Nothing is shown here until a model has actually been trained and evaluated. No placeholder figures are displayed."
            command="make train"
          />
        }
      >
        {(rows) => {
          const latest = rows[0];
          if (!latest) return null;
          const classes = Object.keys(latest.precision);

          return (
            <div className="space-y-6">
              <NeutralBanner metrics={latest} />

              <section className="card min-w-0 p-4" aria-labelledby="comparison-heading">
                <h2 id="comparison-heading" className="text-sm font-medium">
                  Model comparison
                </h2>
                <p className="mt-1 text-xs text-text-muted">
                  Every evaluation run, newest first. Rows are appended, never
                  overwritten, so a model is always comparable against its own
                  history.
                </p>
                <div className="mt-3 overflow-x-auto">
                  <table className="num w-full text-xs">
                    <thead className="text-text-faint">
                      <tr className="border-b border-border">
                        <th scope="col" className="py-2 text-left">Model</th>
                        <th scope="col" className="py-2 text-right">Accuracy</th>
                        {classes.map((name) => (
                          <th key={name} scope="col" className="py-2 text-right">
                            F1 {name}
                          </th>
                        ))}
                        <th scope="col" className="py-2 text-right">Train rows</th>
                        <th scope="col" className="py-2 text-right">Emoji</th>
                        <th scope="col" className="py-2 text-right">Fit time</th>
                        <th scope="col" className="py-2 text-right">Evaluated</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row) => (
                        <tr key={row._id} className="border-b border-border/50">
                          <td className="py-2 text-left">{row.model_name}</td>
                          <td className="py-2 text-right">{formatPercent(row.accuracy, 2)}</td>
                          {classes.map((name) => (
                            <td key={name} className="py-2 text-right">
                              {row.f1[name] === undefined
                                ? "—"
                                : formatPercent(row.f1[name], 1)}
                            </td>
                          ))}
                          <td className="py-2 text-right">{formatNumber(row.train_rows)}</td>
                          <td className="py-2 text-right">
                            {row.feature_config.emoji_strategy}
                          </td>
                          <td className="py-2 text-right">
                            {formatDuration(row.train_duration_sec)}
                          </td>
                          <td className="py-2 text-right">{formatDateTime(row.trained_at)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>

              <div className="grid gap-6 lg:grid-cols-2">
                <section className="card min-w-0 p-4" aria-labelledby="matrix-heading">
                  <h2 id="matrix-heading" className="text-sm font-medium">
                    Confusion matrix — {latest.model_name}
                  </h2>
                  <p className="mt-1 text-xs text-text-muted">
                    {formatNumber(latest.test_rows)} held-out test records.
                  </p>
                  <div className="mt-3">
                    <ConfusionMatrix metrics={latest} />
                  </div>
                </section>

                <section className="card min-w-0 p-4" aria-labelledby="config-heading">
                  <h2 id="config-heading" className="text-sm font-medium">
                    Feature configuration
                  </h2>
                  <dl className="num mt-3 space-y-2 text-xs">
                    {[
                      ["Vectorizer", latest.feature_config.vectorizer],
                      ["Features", formatNumber(latest.feature_config.num_features)],
                      ["IDF", latest.feature_config.idf ? "on" : "off"],
                      ["Emoji strategy", latest.feature_config.emoji_strategy],
                      ["Neutral strategy", latest.neutral_strategy],
                      ["Train rows", formatNumber(latest.train_rows)],
                      ["Test rows", formatNumber(latest.test_rows)],
                      ["Notes", latest.notes || "—"],
                    ].map(([label, value]) => (
                      <div key={label} className="flex justify-between gap-4">
                        <dt className="text-text-muted">{label}</dt>
                        <dd className="text-text">{value}</dd>
                      </div>
                    ))}
                  </dl>

                  <h3 className="mt-4 text-xs uppercase tracking-wide text-text-faint">
                    Per-class scores
                  </h3>
                  <table className="num mt-2 w-full text-xs">
                    <thead className="text-text-faint">
                      <tr>
                        <th scope="col" className="py-1 text-left">Class</th>
                        <th scope="col" className="py-1 text-right">Precision</th>
                        <th scope="col" className="py-1 text-right">Recall</th>
                        <th scope="col" className="py-1 text-right">F1</th>
                      </tr>
                    </thead>
                    <tbody>
                      {classes.map((name) => (
                        <tr key={name} className="border-t border-border">
                          <td className="py-1 capitalize">{name}</td>
                          <td className="py-1 text-right">
                            {formatPercent(latest.precision[name], 1)}
                          </td>
                          <td className="py-1 text-right">
                            {formatPercent(latest.recall[name], 1)}
                          </td>
                          <td className="py-1 text-right">
                            {formatPercent(latest.f1[name], 1)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </section>
              </div>
            </div>
          );
        }}
      </AsyncBoundary>
    </div>
  );
}

/**
 * Data & Storage (PRD §9.2 page 4) — evidences C2, C4, C8 and C19.
 *
 * Shows the lake and the serving layer side by side, because the
 * storage-split claim is only credible when the two sets of numbers are
 * visible together.
 */

import { AsyncBoundary, EmptyState, LoadingCard } from "../components/States";
import { usePipelineConfig, useStorage } from "../hooks/useApi";
import { formatBytes, formatNumber } from "../lib/format";
import type { LakeEntry, StorageInfo } from "../lib/types";

const LAKE_PATHS = [
  { path: "/sentiment/raw", purpose: "Source corpus as Parquet, partitioned by label" },
  { path: "/sentiment/clean", purpose: "Post-preprocessing, the trainer's input" },
  { path: "/sentiment/train", purpose: "80% split, fixed seed" },
  { path: "/sentiment/test", purpose: "20% split, fixed seed" },
  { path: "/sentiment/models", purpose: "Persisted PipelineModel per classifier" },
  { path: "/sentiment/checkpoints", purpose: "Streaming checkpoints, one per run" },
];

function summarise(entries: LakeEntry[], prefix: string) {
  const files = entries.filter((entry) => !entry.is_dir && entry.path.startsWith(prefix));
  return {
    files: files.length,
    bytes: files.reduce((sum, entry) => sum + entry.size_bytes, 0),
    present: entries.some((entry) => entry.path.startsWith(prefix)),
  };
}

function StorageView({ storage }: { storage: StorageInfo }) {
  const mongoTotal = storage.collections.reduce(
    (sum, collection) => sum + collection.size_bytes,
    0,
  );
  const ratio = mongoTotal > 0 ? storage.lake_total_bytes / mongoTotal : null;

  return (
    <div className="space-y-6">
      <section className="card min-w-0 p-4" aria-labelledby="backend-heading">
        <h2 id="backend-heading" className="text-sm font-medium">
          Active storage backend
        </h2>
        <dl className="num mt-3 grid gap-2 text-xs sm:grid-cols-3">
          {[
            ["backend", storage.backend.backend],
            ["root", storage.backend.root],
            ["replication", storage.backend.replication],
          ].map(([label, value]) => (
            <div key={label} className="flex justify-between gap-4 border-b border-border/50 py-1">
              <dt className="text-text-muted">{label}</dt>
              <dd className="text-text">{value}</dd>
            </div>
          ))}
        </dl>
        {storage.backend.backend === "hdfs" ? (
          <p className="mt-3 text-xs text-text-muted">
            Replication factor 1 on a single datanode: real HDFS semantics,
            paths, CLI and block-based splitting, but not the fault tolerance of
            a multi-node cluster. Stated in <code className="num">docs/LIMITATIONS.md</code>.
          </p>
        ) : (
          <p className="mt-3 text-xs text-warning">
            Running on the local filesystem backend. Path layout is identical,
            but this does not evidence the HDFS claim (C4).
          </p>
        )}
      </section>

      <section className="card min-w-0 p-4" aria-labelledby="lake-heading">
        <h2 id="lake-heading" className="text-sm font-medium">
          Data lake layout (C4, C8)
        </h2>
        <table className="num mt-3 w-full text-xs">
          <caption className="sr-only">Lake paths with file counts and sizes</caption>
          <thead className="text-text-faint">
            <tr className="border-b border-border">
              <th scope="col" className="py-2 text-left">Path</th>
              <th scope="col" className="py-2 text-left">Purpose</th>
              <th scope="col" className="py-2 text-right">Files</th>
              <th scope="col" className="py-2 text-right">Size</th>
            </tr>
          </thead>
          <tbody>
            {LAKE_PATHS.map((entry) => {
              const stats = summarise(storage.lake, entry.path);
              return (
                <tr key={entry.path} className="border-b border-border/40">
                  <td className="py-2">{entry.path}</td>
                  <td className="py-2 font-sans text-text-muted">{entry.purpose}</td>
                  <td className="py-2 text-right">
                    {stats.present ? formatNumber(stats.files) : "—"}
                  </td>
                  <td className="py-2 text-right">
                    {stats.present ? formatBytes(stats.bytes) : "not created"}
                  </td>
                </tr>
              );
            })}
          </tbody>
          <tfoot>
            <tr className="border-t border-border">
              <td className="py-2 font-semibold" colSpan={3}>
                Lake total
              </td>
              <td className="py-2 text-right font-semibold">
                {formatBytes(storage.lake_total_bytes)}
              </td>
            </tr>
          </tfoot>
        </table>
      </section>

      <section className="card min-w-0 p-4" aria-labelledby="mongo-heading">
        <h2 id="mongo-heading" className="text-sm font-medium">
          MongoDB serving layer (C19)
        </h2>
        <table className="num mt-3 w-full text-xs">
          <caption className="sr-only">MongoDB collection sizes and document counts</caption>
          <thead className="text-text-faint">
            <tr className="border-b border-border">
              <th scope="col" className="py-2 text-left">Collection</th>
              <th scope="col" className="py-2 text-right">Documents</th>
              <th scope="col" className="py-2 text-right">Data</th>
              <th scope="col" className="py-2 text-right">Storage</th>
              <th scope="col" className="py-2 text-right">Indexes</th>
              <th scope="col" className="py-2 text-right">Index size</th>
            </tr>
          </thead>
          <tbody>
            {storage.collections.map((collection) => (
              <tr key={collection.collection} className="border-b border-border/40">
                <td className="py-2">{collection.collection}</td>
                <td className="py-2 text-right">{formatNumber(collection.documents)}</td>
                <td className="py-2 text-right">{formatBytes(collection.size_bytes)}</td>
                <td className="py-2 text-right">{formatBytes(collection.storage_bytes)}</td>
                <td className="py-2 text-right">{collection.indexes}</td>
                <td className="py-2 text-right">{formatBytes(collection.index_bytes)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr className="border-t border-border">
              <td className="py-2 font-semibold">Total</td>
              <td className="py-2 text-right font-semibold">
                {formatNumber(
                  storage.collections.reduce((sum, c) => sum + c.documents, 0),
                )}
              </td>
              <td className="py-2 text-right font-semibold">{formatBytes(mongoTotal)}</td>
              <td colSpan={3} />
            </tr>
          </tfoot>
        </table>
      </section>

      <section className="card min-w-0 p-4" aria-labelledby="rationale-heading">
        <h2 id="rationale-heading" className="text-sm font-medium">
          Why the split is shaped this way
        </h2>
        {ratio ? (
          <p className="num mt-2 text-sm text-text">
            The lake holds {formatDecimalRatio(ratio)}&times; more bytes than the
            serving layer.
          </p>
        ) : null}
        <ul className="mt-3 space-y-2 text-sm text-text-muted">
          <li>
            <strong className="text-text">windows / hashtags</strong> — small,
            aggregated, durable. This is what the dashboard reads, which is why
            every page loads from a handful of documents rather than a scan.
          </li>
          <li>
            <strong className="text-text">scored</strong> — individual classified
            records for drill-down, expired by a 24-hour TTL index on{" "}
            <code className="num">ingest_time</code>. At replay speed this
            collection would otherwise exhaust the disk.
          </li>
          <li>
            <strong className="text-text">the corpus itself</strong> — never enters
            MongoDB. It stays in the lake as Parquet, which is columnar,
            compressed and splittable.
          </li>
        </ul>
      </section>
    </div>
  );
}

function formatDecimalRatio(value: number): string {
  return value >= 10 ? Math.round(value).toLocaleString("en-IN") : value.toFixed(1);
}

export function DataStorage() {
  const storageQuery = useStorage();
  const configQuery = usePipelineConfig();

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold">Data &amp; Storage</h1>

      <AsyncBoundary
        query={storageQuery}
        isEmpty={(storage) => storage === null}
        loading={<LoadingCard lines={8} title="storage state" />}
        empty={
          <EmptyState
            title="Storage state unavailable"
            detail="Neither the data lake nor MongoDB could be inspected."
            command="make setup"
          />
        }
      >
        {(storage) => (storage ? <StorageView storage={storage} /> : null)}
      </AsyncBoundary>

      <section className="card min-w-0 p-4" aria-labelledby="versions-heading">
        <h2 id="versions-heading" className="text-sm font-medium">
          Pinned stack (C25)
        </h2>
        <p className="mt-1 text-xs text-text-muted">
          Asserted at the top of every Spark entrypoint. A mismatch raises a
          named error rather than a stack trace.
        </p>
        {configQuery.data?.data ? (
          <dl className="num mt-3 grid gap-2 text-xs sm:grid-cols-2 lg:grid-cols-3">
            {Object.entries(configQuery.data.data.versions).map(([key, value]) => (
              <div key={key} className="flex justify-between gap-4 border-b border-border/50 py-1">
                <dt className="text-text-muted">{key}</dt>
                <dd className="text-text">{value}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="mt-2 text-sm text-text-muted">Configuration unavailable.</p>
        )}
      </section>

      <section className="card min-w-0 p-4" aria-labelledby="profile-heading">
        <h2 id="profile-heading" className="text-sm font-medium">
          Dataset profile (C2, C3)
        </h2>
        <p className="mt-2 text-sm text-text-muted">
          The full profile — schema, per-column null rates, label distribution,
          text-length percentiles and the token-class breakdown — is generated
          from the corpus itself by{" "}
          <code className="num">ingest/profile_dataset.py</code> and written to{" "}
          <code className="num">docs/dataset_profile.md</code>. It is regenerated
          by <code className="num">make ingest</code>, never edited by hand.
        </p>
      </section>
    </div>
  );
}

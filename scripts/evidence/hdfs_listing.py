"""C4 / C8: the data lake listing -- `hdfs dfs -ls -R /sentiment`.

Under the `hdfs` backend this runs the real Hadoop CLI inside the namenode
container, so the output is HDFS's own, not a reconstruction. Under `local` it
lists the filesystem and says so plainly, because a local listing is not
evidence for a claim about HDFS.
"""

from __future__ import annotations

from scripts.evidence._common import Evidence, format_bytes
from storage import hdfs


def main() -> int:
    evidence = Evidence(
        "hdfs_listing",
        "HDFS is the data-lake layer for raw and cleaned data",
        "HDFS LAYOUT",
    )

    describe = hdfs.describe()
    evidence.line(f"backend:     {describe['backend']}")
    evidence.line(f"root:        {describe['root']}")
    evidence.line(f"replication: {describe['replication']}")
    evidence.line("")

    if describe["backend"] != "hdfs":
        evidence.line(
            "STORAGE_BACKEND is 'local', so what follows is a local filesystem "
            "listing. It demonstrates the path layout but NOT HDFS itself -- "
            "claim C4 needs STORAGE_BACKEND=hdfs with the namenode running."
        )
        evidence.line("")

    try:
        entries = hdfs.listing(hdfs.ROOT)
    except hdfs.StorageError as exc:
        return evidence.fail(
            str(exc),
            "docker compose -f docker-compose.hdfs.yml up -d && make setup",
        )

    if not entries:
        return evidence.fail(f"{hdfs.ROOT} is empty", "make ingest")

    if describe["backend"] == "hdfs":
        evidence.line("raw output of: hdfs dfs -ls -R /sentiment")
        evidence.line("")
        raw = hdfs._hdfs_cli("-ls", "-R", str(hdfs.ROOT), check=False)
        checkpoint_lines = 0
        for line in raw.splitlines():
            # Streaming checkpoints are thousands of small offset/commit files;
            # listing each one buries the lake layout the reader came for.
            if f"{hdfs.CHECKPOINTS}/" in line:
                checkpoint_lines += 1
                continue
            evidence.line(f"  {line}")
        if checkpoint_lines:
            evidence.line(
                f"  ... {checkpoint_lines:,} entries under {hdfs.CHECKPOINTS}/ "
                f"(Spark streaming offsets and commits) omitted"
            )
        evidence.line("")

    directories = [entry for entry in entries if entry.is_dir]
    files = [entry for entry in entries if not entry.is_dir]
    evidence.line("summary")
    evidence.table(
        ["metric", "value"],
        [
            ["directories", f"{len(directories):,}"],
            ["files", f"{len(files):,}"],
            ["total size", format_bytes(sum(entry.size_bytes for entry in files))],
        ],
    )
    evidence.line("")

    evidence.line("per top-level path")
    rows = []
    for path in (
        hdfs.RAW,
        hdfs.CLEAN,
        hdfs.TRAIN,
        hdfs.TEST,
        hdfs.MODELS,
        hdfs.CHECKPOINTS,
    ):
        matching = [
            entry
            for entry in files
            if entry.path.startswith(str(path))
        ]
        rows.append(
            [
                str(path),
                "present" if hdfs.exists(path) else "absent",
                f"{len(matching):,}",
                format_bytes(sum(entry.size_bytes for entry in matching)),
            ]
        )
    evidence.table(["path", "state", "files", "size"], rows)

    if describe["backend"] == "hdfs":
        # Prove the replication claim rather than assert it. Replication is set
        # by the writing client, and a missing client setting once left 73
        # blocks asking for 3 copies on a single datanode.
        import subprocess

        fsck = subprocess.run(
            [hdfs.docker_binary(), "exec", hdfs.NAMENODE_CONTAINER, "hdfs", "fsck", str(hdfs.ROOT)],
            capture_output=True, text=True, timeout=300,
        ).stdout
        keep = ("Status", "Total blocks", "Under-replicated", "Missing replicas",
                "Default replication", "Average block replication", "Corrupt blocks")
        evidence.line("")
        evidence.line("hdfs fsck /sentiment")
        for line in fsck.splitlines():
            if line.strip().startswith(keep):
                evidence.line(f"  {line.strip()}")

        evidence.line("")
        evidence.line(
            "Replication factor is 1 on a single datanode: this provides real "
            "HDFS semantics, paths, CLI and block-based splitting, but not the "
            "fault tolerance of a multi-node cluster. See docs/LIMITATIONS.md."
        )

    return evidence.done()


if __name__ == "__main__":
    raise SystemExit(main())

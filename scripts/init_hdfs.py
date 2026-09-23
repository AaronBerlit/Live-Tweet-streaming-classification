"""Create the lake directory layout (PRD §5.2). Idempotent."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from pipeline.config import settings
from storage import hdfs

LAYOUT = [hdfs.RAW, hdfs.CLEAN, hdfs.TRAIN, hdfs.TEST, hdfs.MODELS, hdfs.CHECKPOINTS]


def main() -> int:
    if hdfs.backend() == "local":
        hdfs.ensure_local_root()
        for path in LAYOUT:
            target = Path(settings.local_data_root) / path.relative_to(hdfs.ROOT)
            target.mkdir(parents=True, exist_ok=True)
            print(f"created {target}")
        print(f"\nlocal lake ready at {settings.local_data_root}")
        return 0

    for path in LAYOUT:
        cmd = [hdfs.docker_binary(), "exec", hdfs.NAMENODE_CONTAINER, "hdfs", "dfs", "-mkdir", "-p", str(path)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"failed to create {path}: {result.stderr.strip()}", file=sys.stderr)
            return 1
        print(f"created hdfs://{path}")

    print(f"\nHDFS lake ready under {hdfs.ROOT} (dfs.replication=1)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

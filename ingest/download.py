"""C1: fetch Sentiment140 from Kaggle, verify it, extract to data/raw/.

If credentials are absent this exits with instructions for manual placement
rather than failing obscurely (PRD §5.1).

On checksums: the PRD asks for archive verification, but hardcoding a hash we
have not computed ourselves would be a fabricated constant. Instead the first
successful download records the archive's SHA-256 in `data/raw/CHECKSUMS.txt`,
and every later run verifies against it. Integrity of the *file we actually
have* is what matters for reproducibility across the three team machines.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import zipfile
from pathlib import Path

from pipeline.config import REPO_ROOT

DATASET = "kazanova/sentiment140"
CSV_NAME = "training.1600000.processed.noemoticon.csv"
RAW_DIR = REPO_ROOT / "data" / "raw"
CHECKSUM_FILE = RAW_DIR / "CHECKSUMS.txt"
EXPECTED_ROWS = 1_600_000

MANUAL_INSTRUCTIONS = f"""
Kaggle credentials not found.

Either set them in .env:

    KAGGLE_USERNAME=your-username
    KAGGLE_KEY=your-api-key

  (create a token at https://www.kaggle.com/settings -> API -> Create New Token,
   and accept the dataset terms at https://www.kaggle.com/datasets/{DATASET})

or place the file manually:

    1. download {CSV_NAME} yourself
    2. put it at {RAW_DIR / CSV_NAME}
    3. re-run this command -- it will verify and continue

The download is the only step that needs Kaggle. Everything after it reads
from data/raw/.
"""


def sha256(path: Path, *, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(chunk):
            digest.update(block)
    return digest.hexdigest()


def _record_or_verify(path: Path) -> bool:
    """Record this file's checksum on first sight; verify it thereafter."""
    actual = sha256(path)
    recorded: dict[str, str] = {}
    if CHECKSUM_FILE.exists():
        for line in CHECKSUM_FILE.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#"):
                value, _, name = line.partition("  ")
                recorded[name.strip()] = value.strip()

    expected = recorded.get(path.name)
    if expected is None:
        recorded[path.name] = actual
        CHECKSUM_FILE.parent.mkdir(parents=True, exist_ok=True)
        CHECKSUM_FILE.write_text(
            "# sha256 of the corpus files, recorded on first download.\n"
            "# Share this file across machines to guarantee identical inputs.\n"
            + "".join(f"{v}  {k}\n" for k, v in sorted(recorded.items())),
            encoding="utf-8",
        )
        print(f"  recorded sha256 {actual[:16]}... for {path.name}")
        return True

    if expected != actual:
        print(
            f"  CHECKSUM MISMATCH for {path.name}\n"
            f"    recorded {expected}\n"
            f"    actual   {actual}\n"
            f"  The corpus differs from the one this repo was built against.",
            file=sys.stderr,
        )
        return False
    print(f"  checksum ok ({actual[:16]}...)")
    return True


def _have_credentials() -> bool:
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return True
    return (Path.home() / ".kaggle" / "kaggle.json").exists()


def _download() -> bool:
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        print("the `kaggle` package is not installed -- pip install -r requirements.txt", file=sys.stderr)
        return False

    api = KaggleApi()
    try:
        api.authenticate()
    except Exception as exc:  # noqa: BLE001 - kaggle raises bare exceptions
        print(f"Kaggle authentication failed: {exc}", file=sys.stderr)
        return False

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"downloading {DATASET} -> {RAW_DIR}")
    try:
        api.dataset_download_files(DATASET, path=str(RAW_DIR), unzip=False, quiet=False)
    except Exception as exc:  # noqa: BLE001
        print(f"download failed: {exc}", file=sys.stderr)
        print(
            "If this is a 403, accept the dataset terms on Kaggle first:\n"
            f"  https://www.kaggle.com/datasets/{DATASET}",
            file=sys.stderr,
        )
        return False

    archive = RAW_DIR / "sentiment140.zip"
    if not archive.exists():
        candidates = sorted(RAW_DIR.glob("*.zip"))
        if not candidates:
            print("no archive appeared after download", file=sys.stderr)
            return False
        archive = candidates[0]

    print(f"verifying {archive.name}")
    if not _record_or_verify(archive):
        return False

    print(f"extracting {archive.name}")
    with zipfile.ZipFile(archive) as zf:
        zf.extractall(RAW_DIR)
    return True


def _verify_csv(path: Path) -> int:
    """Confirm the corpus is the size C1 claims, without loading it into RAM."""
    print(f"\nverifying {path.name}")
    print(f"  size    {path.stat().st_size / 1024 / 1024:.1f} MB")
    with path.open("rb") as handle:
        rows = sum(chunk.count(b"\n") for chunk in iter(lambda: handle.read(1 << 20), b""))
    print(f"  rows    {rows:,}")
    if rows != EXPECTED_ROWS:
        print(
            f"  NOTE: expected {EXPECTED_ROWS:,} rows for the full Sentiment140 corpus.\n"
            f"  Row-count claims in the report must match what is actually here.",
            file=sys.stderr,
        )
    _record_or_verify(path)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force", action="store_true", help="re-download even if the CSV is present"
    )
    args = parser.parse_args()

    csv_path = RAW_DIR / CSV_NAME

    if csv_path.exists() and not args.force:
        print(f"{csv_path.name} already present -- skipping download")
        _verify_csv(csv_path)
        return 0

    if not _have_credentials():
        print(MANUAL_INSTRUCTIONS, file=sys.stderr)
        return 1

    if not _download():
        return 1

    if not csv_path.exists():
        extracted = sorted(RAW_DIR.glob("*.csv"))
        if not extracted:
            print("no CSV found after extraction", file=sys.stderr)
            return 1
        csv_path = extracted[0]
        print(f"note: expected {CSV_NAME}, found {csv_path.name}")

    _verify_csv(csv_path)
    print(f"\nC1 satisfied: corpus available at {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

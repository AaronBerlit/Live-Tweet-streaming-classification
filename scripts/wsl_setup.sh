#!/usr/bin/env bash
# One-time: build the Spark venv inside WSL (Linux filesystem, for speed).
# Requires python3.11 + openjdk-17 already installed in the distro.
set -euo pipefail
cd "$(dirname "$0")/.."
VENV="$HOME/.venvs/sentiment"
if [ ! -x "$VENV/bin/python" ]; then
  python3.11 -m venv "$VENV"
fi
"$VENV/bin/pip" install --quiet --upgrade pip
# PySpark is pure Python: reuse the wheel already built on Windows instead of
# re-downloading 317 MB over a slow link.
if ls data/wheels/pyspark-3.5.3-*.whl >/dev/null 2>&1; then
  "$VENV/bin/pip" install --quiet data/wheels/pyspark-3.5.3-*.whl
fi
"$VENV/bin/pip" install --quiet -r requirements.txt
# A wheel built on Windows carries no Unix permission bits, so PySpark's
# launcher scripts arrive non-executable and spark-submit fails with EACCES.
chmod +x "$VENV"/lib/python3.11/site-packages/pyspark/bin/*          "$VENV"/lib/python3.11/site-packages/pyspark/sbin/* 2>/dev/null || true
"$VENV/bin/pip" install --quiet -e . --no-deps
echo "--- WSL venv ready ---"
"$VENV/bin/python" --version
"$VENV/bin/python" -c "import pyspark; print('pyspark', pyspark.__version__)"
java -version 2>&1 | head -1

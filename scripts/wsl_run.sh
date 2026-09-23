#!/usr/bin/env bash
# Run a Python entrypoint inside WSL, where Spark needs no winutils.
#
# Spark's Hadoop filesystem layer on Windows requires unofficial winutils /
# hadoop.dll binaries. Rather than pull those, every Spark job runs in WSL
# Ubuntu, against the same repository on /mnt/d. The Docker containers publish
# their ports on the Windows side, and WSL reaches them on localhost.
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"
exec "$HOME/.venvs/sentiment/bin/python" "$@"

"""Shared plumbing for the `make evidence-*` commands.

Each evidence script prints to the console *and* writes the same text to
`docs/evidence/<name>.txt`, so the report's claims can be regenerated rather
than asserted (PRD §1.1).

Every script here reads real state -- MongoDB, the lake, the corpus. If the
state it needs does not exist, it says so and exits non-zero. None of them
invent a number to fill a gap.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import REPO_ROOT

EVIDENCE_DIR = REPO_ROOT / "docs" / "evidence"


class Evidence:
    """Collects lines, echoes them, and writes the artifact at the end."""

    def __init__(self, name: str, claim: str, title: str) -> None:
        self.name = name
        self.lines: list[str] = []
        generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        self.line(f"=== {title} ===")
        self.line(f"claim:     {claim}")
        self.line(f"generated: {generated}")
        self.line(f"command:   make evidence-{name.replace('_', '-')}")
        self.line("")

    def line(self, text: str = "") -> None:
        print(text, flush=True)
        self.lines.append(text)

    def table(self, headers: list[str], rows: list[list[str]]) -> None:
        """Fixed-width table, so the artifact is readable as plain text."""
        widths = [
            max(len(str(headers[index])), *(len(str(row[index])) for row in rows))
            if rows
            else len(str(headers[index]))
            for index in range(len(headers))
        ]
        self.line("  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
        self.line("  ".join("-" * width for width in widths))
        for row in rows:
            self.line("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))

    def fail(self, message: str, fix: str | None = None) -> int:
        """Record why the evidence could not be produced, and exit non-zero."""
        self.line("")
        self.line(f"UNAVAILABLE: {message}")
        if fix:
            self.line(f"fix: {fix}")
        self.write()
        return 1

    def write(self) -> None:
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        path = EVIDENCE_DIR / f"{self.name}.txt"
        path.write_text("\n".join(self.lines) + "\n", encoding="utf-8")
        print(f"\nwritten: {path.relative_to(REPO_ROOT)}", file=sys.stderr)

    def done(self) -> int:
        self.write()
        return 0


def format_bytes(value: int) -> str:
    if value == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:,.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:,.1f} TB"


def repository_or_fail(evidence: Evidence):
    """A connected Repository, or None after recording why not."""
    from api.repository import Repository, RepositoryUnavailable

    repository = Repository(timeout_ms=5000)
    try:
        repository.ping()
    except RepositoryUnavailable as exc:
        evidence.line(f"MongoDB unreachable: {exc}")
        return None
    return repository

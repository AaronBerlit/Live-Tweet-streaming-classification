/**
 * Keyboard shortcut sheet (PRD §9.2): `?` opens it, `Esc` closes it.
 * `g` then a letter jumps between pages, so the whole app is usable from the
 * keyboard alone (§12.6 checklist).
 */

import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";

const JUMPS: Record<string, { path: string; label: string }> = {
  o: { path: "/overview", label: "Overview" },
  s: { path: "/stream", label: "Stream Monitor" },
  m: { path: "/model", label: "Model" },
  d: { path: "/storage", label: "Data & Storage" },
  r: { path: "/records", label: "Raw Records" },
  h: { path: "/health", label: "Pipeline Health" },
};

function isTyping(target: EventTarget | null): boolean {
  const element = target as HTMLElement | null;
  return !!element && (["INPUT", "TEXTAREA", "SELECT"].includes(element.tagName) || element.isContentEditable);
}

export function ShortcutSheet() {
  const [open, setOpen] = useState(false);
  const pendingG = useRef(false);
  const navigate = useNavigate();
  const closeRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") { setOpen(false); return; }
      if (isTyping(event.target) || event.ctrlKey || event.metaKey || event.altKey) return;
      if (event.key === "?") { event.preventDefault(); setOpen((value) => !value); return; }
      if (pendingG.current) {
        pendingG.current = false;
        const jump = JUMPS[event.key.toLowerCase()];
        if (jump) { event.preventDefault(); navigate(jump.path); setOpen(false); }
        return;
      }
      if (event.key === "g") {
        pendingG.current = true;
        window.setTimeout(() => { pendingG.current = false; }, 1200);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [navigate]);

  useEffect(() => { if (open) closeRef.current?.focus(); }, [open]);

  if (!open) return null;

  const rows: Array<[string, string]> = [
    ["?", "Show or hide this sheet"],
    ["Esc", "Close sheets and record details"],
    ["/", "Focus search (Raw Records)"],
    ...Object.entries(JUMPS).map(([key, jump]): [string, string] => [`g ${key}`, `Go to ${jump.label}`]),
    ["Tab", "Move between controls; charts have table fallbacks"],
  ];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-bg/80 p-4" onClick={() => setOpen(false)}>
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="shortcut-title"
        className="card w-full max-w-md p-5"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between">
          <h2 id="shortcut-title" className="text-sm font-semibold">Keyboard shortcuts</h2>
          <button ref={closeRef} type="button" onClick={() => setOpen(false)}
            className="rounded px-2 py-1 text-xs text-text-muted hover:bg-raised">Close (Esc)</button>
        </div>
        <dl className="mt-4 space-y-2 text-sm">
          {rows.map(([keys, action]) => (
            <div key={keys} className="flex items-center justify-between gap-4">
              <dt><kbd className="num rounded border border-border bg-raised px-2 py-0.5 text-xs">{keys}</kbd></dt>
              <dd className="text-text-muted">{action}</dd>
            </div>
          ))}
        </dl>
      </div>
    </div>
  );
}

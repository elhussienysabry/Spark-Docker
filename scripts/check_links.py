#!/usr/bin/env python3
"""Check relative links in Markdown files and notebook Markdown cells.

External URLs are not fetched. Only local file links are resolved, so the check is
fast, deterministic, and safe to run in CI. Anchor-only links are skipped; file links
with a ``#fragment`` are checked for file existence.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
SKIP_PARTS = {".git", ".ipynb_checkpoints", "__pycache__", "node_modules"}
MARKDOWN_GLOBS = ("*.md", "docs/**/*.md", ".github/**/*.md")
NOTEBOOK_GLOB = "notebooks/**/*.ipynb"

LINK_RE = re.compile(r"!?\[[^\]]*\]\(\s*(<[^>]+>|[^)\s]+)(?:\s+['\"][^'\"]*['\"])?\s*\)")
FENCE_RE = re.compile(r"```.*?```|~~~.*?~~~", re.DOTALL)
INLINE_CODE_RE = re.compile(r"`[^`\n]*`")
EXTERNAL_PREFIXES = (
    "http://",
    "https://",
    "mailto:",
    "tel:",
    "data:",
    "ftp://",
    "//",
)


def strip_code(text: str) -> str:
    """Remove fenced blocks and inline code so examples are not treated as links."""
    text = FENCE_RE.sub("", text)
    return INLINE_CODE_RE.sub("", text)


def iter_markdown_files() -> list[Path]:
    files: set[Path] = set()
    for pattern in MARKDOWN_GLOBS:
        for path in ROOT.glob(pattern):
            if path.is_file() and not any(part in SKIP_PARTS for part in path.parts):
                files.add(path)
    return sorted(files)


def iter_notebooks() -> list[Path]:
    notebooks = []
    for path in sorted(ROOT.glob(NOTEBOOK_GLOB)):
        if "templates" in path.parts:
            continue
        if path.is_file() and not any(part in SKIP_PARTS for part in path.parts):
            notebooks.append(path)
    return notebooks


def markdown_links(path: Path) -> list[tuple[int, str]]:
    text = path.read_text(encoding="utf-8")
    text = strip_code(text)
    links = []
    for match in LINK_RE.finditer(text):
        target = match.group(1).strip()
        if target.startswith("<") and target.endswith(">"):
            target = target[1:-1].strip()
        line = text.count("\n", 0, match.start()) + 1
        links.append((line, target))
    return links


def notebook_links(path: Path) -> list[tuple[int, str]]:
    try:
        notebook = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    links = []
    for cell_index, cell in enumerate(notebook.get("cells", [])):
        if cell.get("cell_type") != "markdown":
            continue
        source = cell.get("source", "")
        if isinstance(source, list):
            source = "".join(source)
        source = strip_code(source or "")
        for match in LINK_RE.finditer(source):
            target = match.group(1).strip()
            if target.startswith("<") and target.endswith(">"):
                target = target[1:-1].strip()
            line = source.count("\n", 0, match.start()) + 1
            links.append((cell_index, line, target))
    return links


def resolve(path: Path, target: str) -> Path | None:
    target = target.strip()
    if not target or target.startswith("#"):
        return None
    if target.lower().startswith(EXTERNAL_PREFIXES):
        return None
    if "{" in target or "}" in target:
        return None

    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return None
    raw_path = unquote(parsed.path)
    if not raw_path:
        return None

    if raw_path.startswith("/"):
        candidate = (ROOT / raw_path.lstrip("/")).resolve()
    else:
        candidate = (path.parent / raw_path).resolve()
    return candidate


def check() -> int:
    markdown_files = iter_markdown_files()
    notebooks = iter_notebooks()
    checked = 0
    broken: list[tuple[str, int, str, str]] = []

    for path in markdown_files:
        for line, target in markdown_links(path):
            candidate = resolve(path, target)
            if candidate is None:
                continue
            checked += 1
            if not candidate.exists():
                broken.append((str(path.relative_to(ROOT)), line, target, str(candidate)))

    for path in notebooks:
        for cell_index, line, target in notebook_links(path):
            candidate = resolve(path, target)
            if candidate is None:
                continue
            checked += 1
            if not candidate.exists():
                broken.append(
                    (
                        str(path.relative_to(ROOT)),
                        cell_index,
                        target,
                        str(candidate),
                    )
                )

    print("=" * 72)
    print("LOCAL LINK CHECK")
    print("=" * 72)
    print(f"Markdown files scanned: {len(markdown_files)}")
    print(f"Notebooks scanned:      {len(notebooks)}")
    print(f"Local links checked:    {checked}")
    print(f"Broken links:           {len(broken)}")

    if broken:
        print("\nBroken links:")
        for source, line, target, candidate in broken:
            print(f"  {source}:{line}: {target}")
            print(f"    resolved to: {candidate}")
        print("=" * 72)
        print("LINK CHECK FAILED")
        return 1

    print("=" * 72)
    print("LINK CHECK PASSED: all local links resolve.")
    return 0


if __name__ == "__main__":
    sys.exit(check())

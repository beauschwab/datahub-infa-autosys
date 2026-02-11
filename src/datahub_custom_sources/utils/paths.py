"""
Shared file path utilities used across multiple source plugins.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence


def expand_paths(
    explicit_paths: Sequence[str],
    scan_dirs: Sequence[str],
    extensions: Sequence[str],
) -> list[Path]:
    """
    Build a de-duplicated, ordered list of file paths from:
      - explicit paths (files named directly in config)
      - scan directories (recursively searched for files matching extensions)

    Parameters
    ----------
    explicit_paths : directly specified file paths.
    scan_dirs      : directories to walk recursively.
    extensions     : file extensions to match (e.g. ``[".dtsx"]``, ``[".mp", ".g", ".xml"]``).

    Returns
    -------
    De-duplicated list of resolved ``Path`` objects, preserving insertion order.
    """
    result: list[Path] = []
    seen: set[Path] = set()

    def _add(p: Path) -> None:
        resolved = p.resolve()
        if resolved not in seen:
            seen.add(resolved)
            result.append(p)

    for p in explicit_paths:
        _add(Path(p))

    for d in scan_dirs:
        dir_path = Path(d)
        if not dir_path.exists():
            continue
        for ext in extensions:
            for hit in sorted(dir_path.rglob(f"*{ext}")):
                _add(hit)

    return result

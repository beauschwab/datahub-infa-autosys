"""
Lightweight Ab Initio parsing helpers for DML and XFR files.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass
class AbInitioField:
    name: str
    field_type: str


@dataclass
class AbInitioSchema:
    name: str
    fields: list[AbInitioField]


def parse_dml(path: str) -> AbInitioSchema:
    dml_path = Path(path)
    fields: list[AbInitioField] = []

    for line in dml_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^(?P<name>[A-Za-z_][\w\-]*)\s*:\s*(?P<type>[\w\(\)\[\]\,\s]+)", line)
        if not match:
            continue
        fields.append(AbInitioField(name=match.group("name"), field_type=match.group("type").strip()))

    schema_name = dml_path.stem
    return AbInitioSchema(name=schema_name, fields=fields)


def parse_xfr_mappings(path: str) -> list[tuple[str, str]]:
    """
    Extract simple field-to-field mappings from an XFR file.
    Returns list of (source_field, target_field) tuples.
    """
    xfr_path = Path(path)
    mappings: list[tuple[str, str]] = []

    for line in xfr_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^(?P<target>[A-Za-z_][\w\-]*)\s*=\s*(?P<source>[^;]+)", line)
        if match:
            mappings.append((match.group("source").strip(), match.group("target").strip()))

    return mappings


def iter_graph_paths(paths: Iterable[str]) -> Iterable[Path]:
    for p in paths:
        yield Path(p)


def load_io_mapping(paths: Iterable[str]) -> dict[str, dict[str, list[str]]]:
    mapping: dict[str, dict[str, list[str]]] = {}
    for path in paths:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for graph_name, payload in data.items():
                if not isinstance(payload, dict):
                    continue
                inputs = payload.get("inputs", [])
                outputs = payload.get("outputs", [])
                mapping[graph_name] = {
                    "inputs": list(inputs) if isinstance(inputs, (list, tuple, set)) else [],
                    "outputs": list(outputs) if isinstance(outputs, (list, tuple, set)) else [],
                }
    return mapping
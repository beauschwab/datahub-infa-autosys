from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


@dataclass
class AutoSysJob:
    name: str
    job_type: str = "command"
    box_name: Optional[str] = None
    command: Optional[str] = None
    condition: Optional[str] = None
    raw: Dict[str, str] = field(default_factory=dict)

    def upstream_job_names(self) -> Set[str]:
        """
        Parse AutoSys condition strings like:
          s(UPSTREAM1) & s(UPSTREAM2)
          (s(A) | s(B)) & f(C)

        We extract names inside x(NAME) where x is any status function.
        """
        if not self.condition:
            return set()
        return set(re.findall(r"\b[a-zA-Z]\(([^\)]+)\)", self.condition))


@dataclass
class AutoSysBox:
    name: str
    jobs: Set[str] = field(default_factory=set)


@dataclass
class AutoSysExport:
    jobs: Dict[str, AutoSysJob] = field(default_factory=dict)
    boxes: Dict[str, AutoSysBox] = field(default_factory=dict)


def parse_jil_text(text: str) -> AutoSysExport:
    """
    Parse a JIL export (very common format):
      insert_job: NAME
      job_type: command
      command: <...>
      box_name: BOX
      condition: s(UPSTREAM)

    This is not a full JIL parser; it covers the 80/20 needed for orchestration lineage.
    """
    export = AutoSysExport()
    current: Optional[AutoSysJob] = None

    def flush() -> None:
        nonlocal current
        if not current:
            return
        export.jobs[current.name] = current
        if current.box_name:
            export.boxes.setdefault(current.box_name, AutoSysBox(name=current.box_name)).jobs.add(current.name)
        current = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("/*") or line.startswith("//"):
            continue

        if line.lower().startswith("insert_job:"):
            flush()
            name = line.split(":", 1)[1].strip()
            current = AutoSysJob(name=name)
            continue

        if current is None:
            continue

        if ":" not in line:
            # multiline command or condition continuation
            if "command" in current.raw:
                current.raw["command"] += "\n" + line
                current.command = current.raw["command"]
            if "condition" in current.raw:
                current.raw["condition"] += " " + line
                current.condition = current.raw["condition"]
            continue

        k, v = [s.strip() for s in line.split(":", 1)]
        kl = k.lower()

        current.raw[kl] = v
        if kl == "job_type":
            current.job_type = v
        elif kl == "box_name":
            current.box_name = v
        elif kl == "command":
            current.command = v
        elif kl == "condition":
            current.condition = v

    flush()
    return export


def parse_jil_files(paths: Iterable[str | Path]) -> AutoSysExport:
    combined = AutoSysExport()
    for p in paths:
        p = Path(p)
        txt = p.read_text(encoding="utf-8", errors="ignore")
        exp = parse_jil_text(txt)
        combined.jobs.update(exp.jobs)
        for b, box in exp.boxes.items():
            combined.boxes.setdefault(b, AutoSysBox(name=b)).jobs |= box.jobs
    return combined

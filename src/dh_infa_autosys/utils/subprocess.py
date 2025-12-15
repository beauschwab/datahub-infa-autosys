from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from rich.console import Console

console = Console()


@dataclass(frozen=True)
class CmdResult:
    cmd: List[str]
    returncode: int
    stdout: str
    stderr: str


def run_cmd(
    cmd: Sequence[str],
    env: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None,
    timeout_s: Optional[int] = None,
    check: bool = True,
) -> CmdResult:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    console.log(f"[bold cyan]$[/bold cyan] {' '.join(map(str, cmd))}")
    p = subprocess.run(
        list(cmd),
        env=merged_env,
        cwd=cwd,
        timeout=timeout_s,
        capture_output=True,
        text=True,
    )
    res = CmdResult(cmd=list(cmd), returncode=p.returncode, stdout=p.stdout, stderr=p.stderr)
    if check and p.returncode != 0:
        raise RuntimeError(
            f"Command failed ({p.returncode}): {' '.join(cmd)}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}"
        )
    return res

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from dh_infa_autosys.config import AutoSysSourceConfig, InformaticaSourceConfig
from dh_infa_autosys.extractors.autosys_jil import parse_jil_files
from dh_infa_autosys.extractors.informatica_xml import parse_informatica_folder_xml
from dh_infa_autosys.extractors.pmrep import PmrepRunner

app = typer.Typer(add_completion=False, help="Helpers for Informatica + AutoSys + DataHub ingestion")
console = Console()


def _load_json(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


@app.command()
def extract_informatica(config: str = typer.Option(..., help="Path to InformaticaSourceConfig JSON")) -> None:
    cfg = InformaticaSourceConfig.model_validate(_load_json(config))
    runner = PmrepRunner(cfg.pmrep)
    xml_path = runner.export_folder(cfg.export)
    console.log(f"[green]Exported[/green] {xml_path}")


@app.command()
def inspect_informatica(
    xml: str = typer.Option(..., help="Path to pmrep folder export XML"),
    folder: str = typer.Option(..., help="Folder name"),
) -> None:
    exp = parse_informatica_folder_xml(xml, folder=folder)
    console.print(f"Mappings: {len(exp.mappings)}  Workflows: {len(exp.workflows)}")
    for m in list(exp.mappings.values())[:10]:
        console.print(f"- mapping {m.name}  sources={len(m.sources)} targets={len(m.targets)} sql_snippets={len(m.sql_snippets)}")


@app.command()
def inspect_autosys(config: str = typer.Option(..., help="Path to AutoSysSourceConfig JSON")) -> None:
    cfg = AutoSysSourceConfig.model_validate(_load_json(config))
    exp = parse_jil_files(cfg.jil_paths)
    console.print(f"Jobs: {len(exp.jobs)}  Boxes: {len(exp.boxes)}")
    for j in list(exp.jobs.values())[:15]:
        console.print(f"- {j.name} type={j.job_type} box={j.box_name} upstream={sorted(j.upstream_job_names())}")


@app.command()
def dump_autosys_graph(config: str = typer.Option(...), out: Optional[str] = typer.Option(None)) -> None:
    cfg = AutoSysSourceConfig.model_validate(_load_json(config))
    exp = parse_jil_files(cfg.jil_paths)
    payload = {
        "boxes": {b: sorted(list(box.jobs)) for b, box in exp.boxes.items()},
        "jobs": {
            jn: {
                "type": j.job_type,
                "box": j.box_name,
                "upstreams": sorted(list(j.upstream_job_names())),
                "command": j.command,
            }
            for jn, j in exp.jobs.items()
        },
    }
    if out:
        Path(out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
        console.print(f"Wrote {out}")
    else:
        console.print_json(json.dumps(payload))


if __name__ == "__main__":
    app()

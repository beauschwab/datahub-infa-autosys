from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import typer

from datahub_custom_sources.config import (
    AutoSysSourceConfig,
    InformaticaSourceConfig,
    OracleOperationalConfig,
)
from datahub_custom_sources.extractors.autosys_jil import parse_jil_files
from datahub_custom_sources.extractors.informatica_xml import parse_informatica_folder_xml
from datahub_custom_sources.extractors.pmrep import PmrepRunner

logger = logging.getLogger(__name__)

app = typer.Typer(
    add_completion=False,
    help="DataHub ingestion helpers â€” run recipes, extract, and inspect metadata sources.",
)


def _load_json(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Universal recipe runner (main entry-point for all sources)
# ---------------------------------------------------------------------------
@app.command()
def ingest(
    config: str = typer.Option(..., "-c", "--config", help="Path to a YAML recipe file"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Build pipeline but skip emission"),
    report_to: Optional[str] = typer.Option(None, "--report-to", help="Write JSON report to file"),
) -> None:
    """Run a DataHub ingestion pipeline from a YAML recipe (same format as `datahub ingest -c`)."""
    from datahub_custom_sources.runner import run_recipe

    pipeline = run_recipe(config, dry_run=dry_run, report_to=report_to)
    typer.echo(f"âœ” Pipeline complete â€” source={pipeline.config.source.type}")


# ---------------------------------------------------------------------------
# Informatica helpers
# ---------------------------------------------------------------------------
@app.command()
def extract_informatica(
    config: str = typer.Option(..., help="Path to InformaticaSourceConfig JSON"),
) -> None:
    """Export an Informatica folder via pmrep."""
    cfg = InformaticaSourceConfig.model_validate(_load_json(config))
    runner = PmrepRunner(cfg.pmrep)
    xml_path = runner.export_folder(cfg.export)
    typer.echo(f"Exported â†’ {xml_path}")


@app.command()
def inspect_informatica(
    xml: str = typer.Option(..., help="Path to pmrep folder export XML"),
    folder: str = typer.Option(..., help="Folder name"),
) -> None:
    """Print a summary of an Informatica pmrep XML export."""
    exp = parse_informatica_folder_xml(xml, folder=folder)
    typer.echo(f"Mappings: {len(exp.mappings)}  Workflows: {len(exp.workflows)}")
    for m in list(exp.mappings.values())[:10]:
        typer.echo(
            f"  - mapping {m.name}  sources={len(m.sources)}"
            f" targets={len(m.targets)} sql_snippets={len(m.sql_snippets)}"
        )


# ---------------------------------------------------------------------------
# AutoSys helpers
# ---------------------------------------------------------------------------
@app.command()
def inspect_autosys(
    config: str = typer.Option(..., help="Path to AutoSysSourceConfig JSON"),
) -> None:
    """Print a summary of AutoSys JIL parse results."""
    cfg = AutoSysSourceConfig.model_validate(_load_json(config))
    exp = parse_jil_files(cfg.jil_paths)
    typer.echo(f"Jobs: {len(exp.jobs)}  Boxes: {len(exp.boxes)}")
    for j in list(exp.jobs.values())[:15]:
        typer.echo(f"  - {j.name}  type={j.job_type}  box={j.box_name}"
                   f"  upstream={sorted(j.upstream_job_names())}")


@app.command()
def dump_autosys_graph(
    config: str = typer.Option(..., help="Path to AutoSysSourceConfig JSON"),
    out: Optional[str] = typer.Option(None, help="Output JSON path (stdout if omitted)"),
) -> None:
    """Dump the AutoSys job dependency graph as JSON."""
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
    text = json.dumps(payload, indent=2)
    if out:
        Path(out).write_text(text, encoding="utf-8")
        typer.echo(f"Wrote â†’ {out}")
    else:
        typer.echo(text)


# ---------------------------------------------------------------------------
# Oracle helpers
# ---------------------------------------------------------------------------
@app.command()
def extract_oracle_procs(
    config: str = typer.Option(..., help="Path to OracleOperationalConfig JSON"),
    server: str = typer.Option("http://localhost:8080", help="DataHub GMS server URL"),
) -> None:
    """Extract and emit Oracle stored procedures to DataHub."""
    from datahub_custom_sources.operational import emit_stored_procedures

    cfg = OracleOperationalConfig.model_validate(_load_json(config))
    count = emit_stored_procedures(cfg, server)
    typer.echo(f"✓ Emitted {count} stored procedures to DataHub")


if __name__ == "__main__":
    app()

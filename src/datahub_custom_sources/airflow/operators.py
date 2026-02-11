"""
Airflow operators for DataHub ingestion via ``datahub_custom_sources`` recipes.

These operators are thin wrappers around :func:`datahub_custom_sources.runner.run_recipe`
and :func:`datahub_custom_sources.runner.run_recipe_dict`, designed for drop-in use in
Airflow DAGs.

Usage
-----
Option 1 â€” YAML recipe file on disk::

    DataHubIngestionOperator(
        task_id="ingest_informatica",
        recipe_path="/opt/recipes/infa_pmrep.yml",
    )

Option 2 â€” inline recipe dict::

    DataHubIngestionOperator(
        task_id="ingest_autosys",
        recipe={
            "source": {"type": "autosys_jil", "config": {"jil_paths": ["/tmp/export.jil"]}},
            "sink": {"type": "datahub-rest", "config": {"server": "http://datahub:8080"}},
        },
    )

Option 3 â€” Jinja-templated recipe path from Airflow variables/params::

    DataHubIngestionOperator(
        task_id="ingest_dynamic",
        recipe_path="{{ var.value.recipe_dir }}/infa_pmrep.yml",
    )
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Sequence, Union

from airflow.sdk import BaseOperator, Context

logger = logging.getLogger(__name__)


class DataHubIngestionOperator(BaseOperator):
    """
    Run a DataHub ingestion pipeline using a YAML recipe file or an inline dict.

    Exactly one of ``recipe_path`` or ``recipe`` must be provided.

    Parameters
    ----------
    recipe_path : str | None
        Filesystem path to a YAML recipe (supports Jinja templating).
    recipe : dict | None
        Inline recipe dict (same schema as YAML files).
    dry_run : bool
        If True, build the pipeline but do not emit to the sink.
    report_to : str | None
        Optional filesystem path to write a JSON run report.
    """

    # Allow Jinja templating on recipe_path
    template_fields: Sequence[str] = ("recipe_path",)

    def __init__(
        self,
        *,
        recipe_path: Optional[str] = None,
        recipe: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
        report_to: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        if recipe_path and recipe:
            raise ValueError("Provide either 'recipe_path' or 'recipe', not both.")
        if not recipe_path and not recipe:
            raise ValueError("One of 'recipe_path' or 'recipe' must be provided.")

        self.recipe_path = recipe_path
        self.recipe = recipe
        self.dry_run = dry_run
        self.report_to = report_to

    def execute(self, context: Context) -> None:
        # Import at execute-time so the module can be loaded even when
        # ``datahub`` is not yet installed in the DAG-parsing env.
        from datahub_custom_sources.runner import run_recipe, run_recipe_dict

        if self.recipe_path:
            logger.info("Running DataHub recipe from file: %s", self.recipe_path)
            pipeline = run_recipe(
                self.recipe_path,
                dry_run=self.dry_run,
                report_to=self.report_to,
            )
        else:
            assert self.recipe is not None
            logger.info("Running DataHub recipe from inline dict (source=%s)",
                        self.recipe.get("source", {}).get("type", "?"))
            pipeline = run_recipe_dict(
                self.recipe,
                dry_run=self.dry_run,
                report_to=self.report_to,
            )

        # Push a lightweight summary into XCom for downstream tasks.
        try:
            report = pipeline.source.get_report()  # type: ignore[union-attr]
            context["ti"].xcom_push(key="datahub_report", value={
                "workunits": getattr(report, "events_produced", None),
                "warnings": getattr(report, "warnings", []),
                "failures": getattr(report, "failures", []),
            })
        except Exception:
            logger.debug("Could not push report to XCom", exc_info=True)

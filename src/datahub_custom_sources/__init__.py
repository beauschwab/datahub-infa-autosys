"""
datahub_custom_sources â€” Custom DataHub ingestion source plugins.

Quick-start (programmatic)::

    from datahub_custom_sources.runner import run_recipe
    run_recipe("examples/recipes/infa_pmrep.yml")

Airflow integration::

    from datahub_custom_sources.airflow import DataHubIngestionOperator
"""

from __future__ import annotations

# Re-export the runner for convenience â€” usable from Airflow / any Python code.
from datahub_custom_sources.runner import load_recipe as load_recipe
from datahub_custom_sources.runner import run_recipe as run_recipe
from datahub_custom_sources.runner import run_recipe_dict as run_recipe_dict

__all__ = [
    "load_recipe",
    "run_recipe",
    "run_recipe_dict",
]

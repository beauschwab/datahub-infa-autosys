"""
dh_infa_autosys — Custom DataHub ingestion source plugins.

Quick-start (programmatic)::

    from dh_infa_autosys.runner import run_recipe
    run_recipe("examples/recipes/infa_pmrep.yml")

Airflow integration::

    from dh_infa_autosys.airflow import DataHubIngestionOperator
"""

from __future__ import annotations

# Re-export the runner for convenience — usable from Airflow / any Python code.
from dh_infa_autosys.runner import load_recipe as load_recipe
from dh_infa_autosys.runner import run_recipe as run_recipe
from dh_infa_autosys.runner import run_recipe_dict as run_recipe_dict

__all__ = [
    "load_recipe",
    "run_recipe",
    "run_recipe_dict",
]

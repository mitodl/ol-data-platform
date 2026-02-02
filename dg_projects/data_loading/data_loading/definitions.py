"""Data loading assets using dlt (data load tool)."""

from pathlib import Path

from dagster import Definitions, load_from_defs_folder
from dagster_dlt import DagsterDltResource

# Autoload all components from the defs/ directory and merge with resources
defs = Definitions.merge(
    load_from_defs_folder(path_within_project=Path(__file__).parent / "defs"),
    Definitions(
        resources={
            "dlt": DagsterDltResource(),
        }
    ),
)

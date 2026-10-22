"""Where the ingestion inventory lives, for the assets that read it."""

from pathlib import Path

from lakehouse.lib.repo_paths import find_upward

# Returning a path rather than raising when nothing is found keeps a missing
# inventory a findable runtime failure -- airbyte_inventory_drift already fails
# with "No inventory units found under ..." -- rather than an unimportable
# module that no error message can reach.
INVENTORY_FALLBACK = Path("/app/ingestion/inventory")


def find_inventory_dir() -> Path:
    """Locate ``ingestion/inventory`` from either the source tree or the image."""
    return find_upward(Path(__file__), Path("ingestion/inventory"), INVENTORY_FALLBACK)


INVENTORY_DIR = find_inventory_dir()

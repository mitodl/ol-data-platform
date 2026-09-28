"""Where the ingestion inventory lives, for the assets that read it."""

from pathlib import Path

# Anchored on this file rather than the process's cwd, which a run launcher is
# free to change -- but found by searching upward instead of by counting
# parents, because the two layouts this runs in sit at different depths. In the
# source tree the file is four levels below the repo root
# (dg_projects/lakehouse/lakehouse/lib/); in the image it is two, because
# dg_projects/lakehouse/Dockerfile copies the project's *contents* onto /app. No
# fixed index is right for both, and the one that matched the source tree raised
# IndexError at import inside the image, taking the entire code location down
# before dagster could load its definitions.
#
# Returning a path rather than raising when nothing is found keeps a missing
# inventory a findable runtime failure -- airbyte_inventory_drift already fails
# with "No inventory units found under ..." -- rather than an unimportable
# module that no error message can reach.
INVENTORY_FALLBACK = Path("/app/ingestion/inventory")


def find_inventory_dir() -> Path:
    """Locate ``ingestion/inventory`` from either the source tree or the image."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "ingestion" / "inventory"
        if candidate.is_dir():
            return candidate
    return INVENTORY_FALLBACK


INVENTORY_DIR = find_inventory_dir()

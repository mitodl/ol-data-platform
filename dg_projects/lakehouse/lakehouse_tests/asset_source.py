"""Parse the dbt asset modules without importing them.

Importing ``lakehouse.assets.lakehouse.dbt`` or ``dbt_starrocks`` evaluates a
``@dbt_assets`` decorator, which needs a parsed dbt manifest on disk, so tests
that check their structure read the source instead.
"""

import ast
from pathlib import Path

import lakehouse

DBT_ASSET_DIR = Path(lakehouse.__file__).parent / "assets" / "lakehouse"


def parse_asset_module(name: str) -> ast.Module:
    """Return the AST of *name* (e.g. ``dbt.py``) under assets/lakehouse."""
    return ast.parse((DBT_ASSET_DIR / name).read_text())

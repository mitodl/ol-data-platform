#!/usr/bin/env python3
"""Regression guard for override_source.sql on DuckDB targets.

On DuckDB targets, source() must still register a source.* dependency edge
(not just render the Glue view name), or manifest-based lineage/state:modified+
selection goes blind. Run after `dbt parse -t dev` against target/manifest.json.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

manifest = json.loads(Path("target/manifest.json").read_text())
models = [n for n in manifest["nodes"].values() if n["resource_type"] == "model"]
with_source = [
    m
    for m in models
    if any(d.startswith("source.") for d in m.get("depends_on", {}).get("nodes", []))
]
sys.stdout.write(
    f"{len(with_source)}/{len(models)} models have a source.* dependency edge\n"
)
if not with_source:
    sys.exit(
        "No model has a source.* dependency edge — override_source.sql regression?"
    )

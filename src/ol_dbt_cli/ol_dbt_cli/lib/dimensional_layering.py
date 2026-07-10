"""Dimensional-layering rule — enforces the #2072 Definition of Done.

The dimensional layer (``models/dimensional/``) is the architectural boundary
between data preparation and data consumption::

    staging → intermediate → [ DIMENSIONAL LAYER ] → marts → reporting

Marts (``models/marts/``) and reporting (``models/reporting/``) models must
reference ONLY dimensional models or other mart/reporting models — never
``staging/`` (``stg__*``) or ``intermediate/`` (``int__*``) directly. Any data
not yet in the dimensional layer should be added there first. This module
detects direct ``marts``/``reporting`` → ``staging``/``intermediate`` edges.

The check operates over the dbt manifest when one is available (authoritative
parent/child + ``original_file_path``); it falls back to static ``ref()``
extraction (``sql_parser``) mapped to each model's directory when no manifest
has been produced yet — so the lint runs both in CI (after ``dbt parse``) and
locally without one.

Because ~39 violations exist today while the #2072 migration is in flight, the
lint ships with a checked-in **baseline** of known ``child -> parent`` pairs.
Only violations *not* in the baseline fail; the migration shrinks the baseline
over time (regenerate with ``ol-dbt validate --update-baseline``).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ol_dbt_cli.lib.manifest import ManifestRegistry
    from ol_dbt_cli.lib.sql_parser import ParsedModel

CONSUMER_LAYERS = frozenset({"marts", "reporting"})
"""Layers whose models may only reference dimensional or other consumer models."""

FORBIDDEN_PARENT_LAYERS = frozenset({"staging", "intermediate"})
"""Layers a consumer model must not reference directly."""

BASELINE_FILENAME = "dimensional_layering_baseline.txt"
"""Checked-in allowlist of pre-existing violations, at the dbt project root."""


@dataclass(frozen=True)
class LayeringViolation:
    """A single direct consumer → staging/intermediate reference."""

    child: str
    parent: str
    child_layer: str
    parent_layer: str

    @property
    def key(self) -> str:
        """Stable ``child -> parent`` identity used for baseline matching."""
        return f"{self.child} -> {self.parent}"


def classify_layer(file_path: str | Path) -> str | None:
    """Return the dimensional layer for a model file path, or ``None``.

    The layer is the first path segment after ``models/`` — e.g.
    ``models/marts/foo.sql`` → ``"marts"`` and
    ``models/staging/mitxonline/bar.sql`` → ``"staging"``. Works for both
    manifest ``original_file_path`` values and absolute filesystem paths.
    """
    parts = Path(file_path).parts
    try:
        idx = len(parts) - 1 - parts[::-1].index("models")
    except ValueError:
        return None
    if idx + 1 >= len(parts):
        return None
    return parts[idx + 1]


def _dedupe(violations: list[LayeringViolation]) -> list[LayeringViolation]:
    """Drop duplicate ``child -> parent`` pairs, preserving first-seen order."""
    seen: set[str] = set()
    unique: list[LayeringViolation] = []
    for violation in violations:
        if violation.key not in seen:
            seen.add(violation.key)
            unique.append(violation)
    return unique


def find_violations_from_manifest(manifest: ManifestRegistry) -> list[LayeringViolation]:
    """Find layering violations using authoritative manifest parent edges."""
    violations: list[LayeringViolation] = []
    for node in manifest.nodes.values():
        if not node.is_model:
            continue
        child_layer = classify_layer(node.original_file_path)
        if child_layer not in CONSUMER_LAYERS:
            continue
        for parent_uid in node.depends_on:
            parent = manifest.get_node(parent_uid)
            if parent is None or not parent.is_model:
                continue
            parent_layer = classify_layer(parent.original_file_path)
            if parent_layer in FORBIDDEN_PARENT_LAYERS:
                violations.append(
                    LayeringViolation(
                        child=node.name,
                        parent=parent.name,
                        child_layer=child_layer,
                        parent_layer=parent_layer,
                    )
                )
    return _dedupe(violations)


def find_violations_from_sql(
    parsed_by_name: dict[str, ParsedModel],
    layer_by_name: dict[str, str | None],
) -> list[LayeringViolation]:
    """Find layering violations from statically parsed ``ref()`` edges.

    Fallback for when no manifest is available. *layer_by_name* maps every
    model name to its layer (from its file path); *parsed_by_name* supplies the
    ``ref()`` names each model consumes.
    """
    violations: list[LayeringViolation] = []
    for name, parsed in parsed_by_name.items():
        child_layer = layer_by_name.get(name)
        if child_layer not in CONSUMER_LAYERS:
            continue
        for ref_name in parsed.refs:
            parent_layer = layer_by_name.get(ref_name)
            if parent_layer in FORBIDDEN_PARENT_LAYERS:
                violations.append(
                    LayeringViolation(
                        child=name,
                        parent=ref_name,
                        # The guard above narrows child_layer to a non-None str.
                        child_layer=child_layer,
                        parent_layer=parent_layer,
                    )
                )
    return _dedupe(violations)


def default_baseline_path(dbt_dir: Path) -> Path:
    """Return the conventional baseline path at the dbt project root."""
    return dbt_dir / BASELINE_FILENAME


def load_baseline(path: Path) -> set[str]:
    """Load baseline ``child -> parent`` keys, ignoring blanks and ``#`` comments."""
    if not path.exists():
        return set()
    keys: set[str] = set()
    for raw_line in path.read_text().splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if line:
            keys.add(line)
    return keys


def render_baseline(violations: list[LayeringViolation]) -> str:
    """Render a baseline file body: header comment + sorted unique keys."""
    header = [
        "# Dimensional-layering baseline (enforces #2072 Definition of Done).",
        "#",
        "# Each line is a known `child -> parent` reference where a marts/reporting",
        "# model references a staging/intermediate model directly. These are the",
        "# violations that existed when the lint was introduced; the #2072 migration",
        "# shrinks this list over time. The lint fails only on violations NOT listed",
        "# here, so new regressions are blocked while existing debt is tolerated.",
        "#",
        "# Regenerate with: ol-dbt validate --update-baseline",
        "",
    ]
    body = sorted({v.key for v in violations})
    return "\n".join([*header, *body, ""])


def write_baseline(path: Path, violations: list[LayeringViolation]) -> None:
    """Write the baseline file for *violations*, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_baseline(violations))

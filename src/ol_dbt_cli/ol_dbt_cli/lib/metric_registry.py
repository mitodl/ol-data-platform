"""Business metric definitions kept as code in the repository's ``metrics/`` directory.

Each file defines one metric and names the columns that carry it:

    metric:                       # OpenMetadata CreateMetric body
      name: learner_completion_status
      description: ...
      metricType: OTHER
    status: Approved              # entityStatus; sync applies it by PATCH
    implemented_by:
      - dbt_model: afact_learner_courserun_progress
        columns: [completion_status]

The column is the definition. The file says which column that is, so a model
change that drops or renames it fails here instead of leaving the catalog
pointing at nothing.

Bindings resolve against the project's model YAML and SQL files, not manifest
nodes. PR CI parses with the DuckDB target, where the StarRocks-only models
(``b2b_analytics``, ``b2b_learner_records``) are disabled and so absent from
the manifest, and a metric served from one of those views still has to be
checked.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from ol_dbt_cli.lib.data_contracts import EntityBinding, parse_binding, resolved_sql_columns
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from ol_dbt_cli.lib.sql_parser import ParsedModel
    from ol_dbt_cli.lib.yaml_registry import YamlRegistry

METRIC_REGISTRY_CHECK = "metric_registry"
DEFAULT_METRICS_DIR = Path("metrics")

# OpenMetadata's Metric FQN is the bare name, so names are global:
# `<subject>_<measure>` in snake case, which also rules out `__`.
_METRIC_NAME = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)+$")

# From OpenMetadata 2.0.2: api/data/createMetric.json (additionalProperties is
# false, so any other key fails the PUT), entity/data/metric.json for the three
# enums and type/status.json for the status.
CREATE_METRIC_FIELDS = frozenset(
    {
        "name",
        "displayName",
        "description",
        "metricExpression",
        "metricType",
        "unitOfMeasurement",
        "customUnitOfMeasurement",
        "granularity",
        "dimensions",
        "measures",
        "filters",
        "relatedMetrics",
        "assets",
        "owners",
        "reviewers",
        "tags",
        "domains",
        "dataProducts",
        "extension",
        "provider",
    }
)
SYNC_OWNED_FIELDS = frozenset({"id", "fullyQualifiedName"})
METRIC_ENUMS: dict[str, frozenset[str]] = {
    "metricType": frozenset(
        {
            "COUNT",
            "SUM",
            "AVERAGE",
            "RATIO",
            "PERCENTAGE",
            "MIN",
            "MAX",
            "MEDIAN",
            "MODE",
            "STANDARD_DEVIATION",
            "VARIANCE",
            "SIMPLE",
            "CUMULATIVE",
            "DERIVED",
            "CONVERSION",
            "OTHER",
        }
    ),
    "unitOfMeasurement": frozenset(
        {"COUNT", "DOLLARS", "PERCENTAGE", "TIMESTAMP", "SIZE", "REQUESTS", "EVENTS", "TRANSACTIONS", "OTHER"}
    ),
    "granularity": frozenset({"SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR"}),
}
ENTITY_STATUSES = frozenset({"Draft", "In Review", "Approved", "Archived", "Deprecated", "Rejected", "Unprocessed"})


@dataclass(frozen=True)
class MetricImplementation:
    """One place a metric is computed or served from."""

    binding: EntityBinding
    columns: tuple[str, ...]


@dataclass(frozen=True)
class MetricDefinition:
    path: Path
    body: dict[str, Any]
    status: str | None
    implemented_by: tuple[MetricImplementation, ...]
    """The first entry is the defining implementation; later ones serve it."""

    @property
    def name(self) -> str:
        return self.body["name"]


def _parse_implementation(path: Path, raw: Any) -> MetricImplementation:
    label = "an `implemented_by` entry"
    if not isinstance(raw, dict):
        msg = f"{path}: {label} must be a mapping with `dbt_model` or `fqn`, and `columns`"
        raise ValueError(msg)
    columns = raw.get("columns")
    if not isinstance(columns, list) or not columns or not all(isinstance(c, str) for c in columns):
        msg = f"{path}: {label} needs a non-empty `columns` list of column names"
        raise ValueError(msg)
    binding = parse_binding(path, {"type": "table", **{k: v for k, v in raw.items() if k != "columns"}}, label=label)
    if binding.kind == "dbt_source":
        msg = f"{path}: {label} can't be a dbt_source: a metric is implemented by a model, not by raw data"
        raise ValueError(msg)
    return MetricImplementation(binding=binding, columns=tuple(c.lower() for c in columns))


def load_metrics(metrics_dir: Path) -> list[MetricDefinition]:
    """Load every ``*.yaml`` / ``*.yml`` metric in *metrics_dir*, sorted by path.

    :param metrics_dir: directory holding one file per metric.
    :returns: the parsed metrics; empty when the directory does not exist.
    :rtype: list[MetricDefinition]
    :raises ValueError: when a file lacks a ``metric`` mapping with a ``name``,
        or its ``implemented_by`` is empty or holds a malformed entry.
    """
    if not metrics_dir.is_dir():
        return []
    metrics = []
    for path in sorted(p for p in metrics_dir.iterdir() if p.suffix in {".yaml", ".yml"}):
        raw = yaml.safe_load(path.read_text())
        if not isinstance(raw, dict) or not isinstance(raw.get("metric"), dict):
            msg = f"{path}: a metric file needs a top-level `metric` mapping and an `implemented_by` list"
            raise ValueError(msg)
        body = raw["metric"]
        if not isinstance(body.get("name"), str):
            msg = f"{path}: `metric` needs a `name`"
            raise ValueError(msg)
        implementations = raw.get("implemented_by")
        if not isinstance(implementations, list) or not implementations:
            msg = f"{path}: `implemented_by` must list at least one model and the columns that carry the metric"
            raise ValueError(msg)
        metrics.append(
            MetricDefinition(
                path=path,
                body=body,
                status=raw.get("status"),
                implemented_by=tuple(_parse_implementation(path, entry) for entry in implementations),
            )
        )
    return metrics


def check_metric_registry(
    metrics: list[MetricDefinition],
    yaml_registry: YamlRegistry,
    sql_models_by_name: dict[str, ParsedModel],
    report: ValidationReport,
) -> None:
    """Fail when a metric file would not sync, or names a column its model no longer has."""
    paths_by_name: dict[str, list[Path]] = defaultdict(list)
    for metric in metrics:
        paths_by_name[metric.name].append(metric.path)
    for name, paths in paths_by_name.items():
        if len(paths) > 1:
            report.add(
                METRIC_REGISTRY_CHECK,
                Severity.ERROR,
                name,
                f"Metric '{name}' is declared in {len(paths)} files: {', '.join(p.name for p in paths)}",
                "A metric has one definition. Keep one file and list every place it is served under implemented_by.",
            )
    for metric in metrics:
        _check_body(metric, report)
        for implementation in metric.implemented_by:
            _check_implementation(metric, implementation, yaml_registry, sql_models_by_name, report)


def _check_body(metric: MetricDefinition, report: ValidationReport) -> None:
    label = metric.path.name
    if not _METRIC_NAME.match(metric.name):
        report.add(
            METRIC_REGISTRY_CHECK,
            Severity.ERROR,
            label,
            f"Metric name '{metric.name}' breaks the naming convention",
            "Use <subject>_<measure> in snake case with no double underscore, e.g. learner_completion_status.",
        )
    if metric.path.stem != metric.name:
        report.add(
            METRIC_REGISTRY_CHECK,
            Severity.ERROR,
            label,
            f"File {metric.path.name} declares metric '{metric.name}'",
            f"Name the file after the metric: {metric.name}.yaml.",
        )
    for key in sorted(set(metric.body) - CREATE_METRIC_FIELDS):
        report.add(
            METRIC_REGISTRY_CHECK,
            Severity.ERROR,
            label,
            f"`metric.{key}` is not a field the file may set",
            "`ol-dbt metrics sync` resolves it; remove it."
            if key in SYNC_OWNED_FIELDS
            else "OpenMetadata's CreateMetric rejects unknown fields. A glossary term is a `tags` entry "
            "with `source: Glossary`.",
        )
    for key, allowed in METRIC_ENUMS.items():
        if key in metric.body and metric.body[key] not in allowed:
            report.add(
                METRIC_REGISTRY_CHECK,
                Severity.ERROR,
                label,
                f"`metric.{key}` is {metric.body[key]!r}, which OpenMetadata does not accept",
                f"Use one of: {', '.join(sorted(allowed))}.",
            )
    if metric.status is not None and metric.status not in ENTITY_STATUSES:
        report.add(
            METRIC_REGISTRY_CHECK,
            Severity.ERROR,
            label,
            f"`status` is {metric.status!r}, which OpenMetadata does not accept",
            f"Use one of: {', '.join(sorted(ENTITY_STATUSES))}.",
        )


def _check_implementation(
    metric: MetricDefinition,
    implementation: MetricImplementation,
    yaml_registry: YamlRegistry,
    sql_models_by_name: dict[str, ParsedModel],
    report: ValidationReport,
) -> None:
    binding = implementation.binding
    if binding.kind == "fqn":
        return
    model = binding.target
    if model not in sql_models_by_name:
        report.add(
            METRIC_REGISTRY_CHECK,
            Severity.ERROR,
            model,
            f"Metric {metric.path.name} names a dbt_model that does not exist",
            "Rename the binding, or remove the entry in the PR that removes the model.",
        )
        return
    yaml_model = yaml_registry.get_model(model)
    declared = yaml_model.column_names if yaml_model is not None else set()
    sql_columns = resolved_sql_columns(sql_models_by_name[model])
    for column in implementation.columns:
        if column not in declared:
            problem = "is not declared in the YAML"
        elif sql_columns is not None and column not in sql_columns:
            problem = "is not selected by the model SQL"
        else:
            continue
        report.add(
            METRIC_REGISTRY_CHECK,
            Severity.ERROR,
            model,
            f"Column '{column}' {problem}",
            f"{metric.path.name} says it carries metric '{metric.name}'. Restore the column, or change the "
            "metric file in the same PR so the definition change is reviewed.",
        )

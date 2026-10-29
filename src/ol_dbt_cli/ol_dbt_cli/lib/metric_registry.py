"""Business metric definitions kept as code in the repository's ``metrics/`` directory.

Each file defines one metric and names the columns that carry it:

    metric:                       # OpenMetadata CreateMetric body
      name: learner_completion_status
      description: ...
      metricType: OTHER
    status: Approved              # required: Draft or Approved; sync applies it by PATCH
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

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from jsonschema import Draft4Validator

from ol_dbt_cli.lib.data_contracts import EntityBinding, parse_binding, resolved_sql_columns
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ol_dbt_cli.lib.sql_parser import ParsedModel
    from ol_dbt_cli.lib.yaml_registry import YamlRegistry

METRIC_REGISTRY_CHECK = "metric_registry"
DEFAULT_METRICS_DIR = Path("metrics")

# OpenMetadata's Metric FQN is the bare name, so names are global:
# `<subject>_<measure>` in snake case, which also rules out `__`.
_METRIC_NAME = re.compile(r"[a-z][a-z0-9]*(_[a-z0-9]+)+")

# The server's own request schema, cut out of its OpenAPI document by
# `ol-dbt metrics refresh-schema` and committed so the check runs offline. PR CI
# must not depend on the catalog being reachable, and an OpenMetadata upgrade
# should change what passes in a reviewed commit, not between two CI runs.
SCHEMA_SNAPSHOT = Path(__file__).with_name("openmetadata_metric_schema.json")
SCHEMA_VERSION_KEY = "x-openmetadata-version"
_SCHEMA_REF_PREFIX = "#/components/schemas/"
_CREATE_METRIC = "CreateMetric"
_SYNC_OWNED = "`ol-dbt metrics sync` resolves it; remove it."
# A bot's PUT keeps existing domains and data products and ignores extension
# changes (EntityUpdater.updateDomains, updateDataProducts, updateExtension in
# 2.0.5), and sync PATCHes none of them.
_BOT_PUT_KEEPS = "A bot's PUT does not change it on an existing metric, so a change to it in a file would never land."
# Fields a file may not set, and why. `assets` is a CreateMetric field, but it
# takes entity ids, which only exist in the live catalog.
EXCLUDED_FIELDS = {
    "id": _SYNC_OWNED,
    "fullyQualifiedName": _SYNC_OWNED,
    "assets": "It takes catalog ids a file cannot know. List the tables under `implemented_by`; sync links them "
    "by lineage.",
    "domains": _BOT_PUT_KEEPS,
    "dataProducts": _BOT_PUT_KEEPS,
    "extension": _BOT_PUT_KEEPS,
    # A PUT keeps each live lifeCycle timestamp newer than the body's
    # (EntityUpdater.updateLifeCycle in 2.0.5), so a file's would stop matching
    # and be PUT on every sync.
    "lifeCycle": "It records when the metric was created, updated and accessed, which OpenMetadata keeps up to "
    "date itself; remove it.",
}

# OpenMetadata references an owner or reviewer by id, which only exists in the
# live catalog. The file names them and sync resolves the names, as it does for
# a data contract's owners.
_NAMED_REFERENCES = {
    "type": "array",
    "items": {
        "type": "object",
        "required": ["type", "name"],
        "properties": {"type": {"enum": ["team", "user"]}, "name": {"type": "string", "minLength": 1}},
        "additionalProperties": False,
    },
}
_NAMED_REFERENCE_FIELDS = ("owners", "reviewers")

# The pull request that changes a file is the approval, so a file says only
# whether its metric is approved. The sync bot is not a reviewer: when a metric
# has reviewers, OpenMetadata 2.0.5 lets only one of them move it from In Review
# to Approved or Rejected (EntityUpdater.updateEntityStatus), and reviewers are
# all users or exactly one team (validateReviewers), so the bot cannot sit
# beside a team.
FILE_STATUSES = ("Draft", "Approved")


def file_status_problem(status: Any) -> tuple[str, str] | None:
    """Say why a file may not set *status*.

    :returns: the problem and how to fix it, or ``None`` when the file may set it.
    :rtype: tuple[str, str] | None
    """
    if status in FILE_STATUSES:
        return None
    if status is None:
        return (
            f"`status` is missing; a metric file sets {' or '.join(FILE_STATUSES)}",
            "Add `status: Draft`, or `status: Approved` once the definition is approved. Every PUT resets the "
            "status in OpenMetadata, and sync restores it only from the file.",
        )
    return (
        f"`status` is {status!r}; a metric file sets only {' or '.join(FILE_STATUSES)}",
        "Approval happens on the pull request that changes this file. In Review, Rejected, Deprecated and "
        "OpenMetadata's other statuses are not set from files: the sync bot is not a metric reviewer, and when "
        "a metric has reviewers OpenMetadata lets only one of them approve or reject it while it is In Review.",
    )


_TOP_LEVEL_KEYS = frozenset({"metric", "status", "implemented_by"})
_IMPLEMENTATION_KEYS = frozenset({"dbt_model", "dbt_source", "fqn", "type", "columns"})


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


def _reject_unknown_keys(path: Path, label: str, raw: dict[str, Any], allowed: frozenset[str]) -> None:
    unknown = sorted(str(k) for k in set(raw) - allowed)
    if unknown:
        msg = f"{path}: {label} has unknown key(s) {', '.join(unknown)}; it takes {', '.join(sorted(allowed))}"
        raise ValueError(msg)


def _parse_implementation(path: Path, raw: Any) -> MetricImplementation:
    label = "an `implemented_by` entry"
    if not isinstance(raw, dict):
        msg = f"{path}: {label} must be a mapping with `dbt_model` or `fqn`, and `columns`"
        raise ValueError(msg)
    _reject_unknown_keys(path, label, raw, _IMPLEMENTATION_KEYS)
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
        _reject_unknown_keys(path, "a metric file", raw, _TOP_LEVEL_KEYS)
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


def _schema_refs(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            yield ref.removeprefix(_SCHEMA_REF_PREFIX)
        for value in node.values():
            yield from _schema_refs(value)
    elif isinstance(node, list):
        for value in node:
            yield from _schema_refs(value)


def build_schema_snapshot(openapi: dict[str, Any]) -> dict[str, Any]:
    """Cut the schema for a metric file out of an OpenMetadata server's OpenAPI document.

    :param openapi: the parsed ``swagger.json`` the server publishes.
    :returns: a JSON Schema for ``{"metric": <CreateMetric body>, "status": <entityStatus>}``
        holding only the component schemas ``CreateMetric`` reaches, stamped
        with the server's version.
    :rtype: dict[str, Any]
    """
    schemas = openapi["components"]["schemas"]
    reached: dict[str, Any] = {}
    pending = [_CREATE_METRIC]
    while pending:
        name = pending.pop()
        if name not in reached:
            reached[name] = schemas[name]
            pending.extend(_schema_refs(schemas[name]))
    return {
        SCHEMA_VERSION_KEY: openapi["info"]["version"],
        "type": "object",
        "properties": {
            "metric": {"$ref": f"{_SCHEMA_REF_PREFIX}{_CREATE_METRIC}"},
            "status": schemas["Metric"]["properties"]["entityStatus"],
        },
        "components": {"schemas": dict(sorted(reached.items()))},
    }


@cache
def metric_file_schema() -> dict[str, Any]:
    """Return the committed schema snapshot with owners and reviewers taken by name."""
    schema = json.loads(SCHEMA_SNAPSHOT.read_text())
    properties = schema["components"]["schemas"][_CREATE_METRIC]["properties"]
    for field in _NAMED_REFERENCE_FIELDS:
        properties[field] = _NAMED_REFERENCES
    return schema


def _check_body(metric: MetricDefinition, report: ValidationReport) -> None:
    label = metric.path.name
    if not _METRIC_NAME.fullmatch(metric.name):
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
    schema = metric_file_schema()
    version = schema[SCHEMA_VERSION_KEY]
    # The OpenAPI document does not mark CreateMetric as closed, but the server
    # rejects a field it does not know, so the schema alone would let one through.
    known = set(schema["components"]["schemas"][_CREATE_METRIC]["properties"]) - EXCLUDED_FIELDS.keys()
    unknown_hint = (
        f"OpenMetadata {version}'s CreateMetric has no such field. A glossary term is a `tags` entry "
        "with `source: Glossary`."
    )
    for key in sorted(str(k) for k in set(metric.body) - known):
        report.add(
            METRIC_REGISTRY_CHECK,
            Severity.ERROR,
            label,
            f"`metric.{key}` is not a field the file may set",
            EXCLUDED_FIELDS.get(key, unknown_hint),
        )
    status_problem = file_status_problem(metric.status)
    if status_problem is not None:
        report.add(METRIC_REGISTRY_CHECK, Severity.ERROR, label, *status_problem)
    document = {"metric": metric.body}
    # Draft 4 is the JSON Schema dialect an OpenAPI 3.0 schema object is closest to.
    # A value of the wrong type fails several keywords at once; one finding per value says enough.
    errors: dict[str, str] = {}
    for error in Draft4Validator(schema).iter_errors(document):
        errors.setdefault(".".join(str(p) for p in error.absolute_path), error.message)
    for location, message in sorted(errors.items()):
        report.add(
            METRIC_REGISTRY_CHECK,
            Severity.ERROR,
            label,
            f"`{location}`: {message}",
            f"OpenMetadata {version} would reject this value. See metrics/README.md for the format.",
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

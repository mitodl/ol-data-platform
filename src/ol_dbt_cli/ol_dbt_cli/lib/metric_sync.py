"""What `ol-dbt metrics sync` sends to OpenMetadata for a metric file, and when.

The file is the source of truth, but a PUT from a bot does not make the
server match it. Read from the OpenMetadata 2.0.5 source (EntityRepository
and MetricRepository), a PUT to an existing Metric:

- keeps a stored non-empty description (``keepsStoredDescription``) and any
  existing owners (``updateOwners``), so those land only by PATCH;
- resets ``entityStatus`` to Unprocessed, because CreateMetric has no status
  and ``setDefaultStatus`` fills it before the update is compared;
- replaces reviewers with the body's, so a body without them clears them;
- merges tags and never removes one (``updateTags``);
- replaces the metric's own fields (``MetricUpdater.entitySpecificUpdate``).

So sync PUTs only to create the metric or change one of its own fields, sends
the live reviewers when the file does not set any, and then PATCHes
description, owners, reviewers and status wherever they still differ. An
unchanged file writes no Metric field; it re-PUTs its lineage edges, which
the server upserts.

The server stores ``DescriptionSanitizer.sanitize(description)`` on create,
PUT and PATCH. That is the OWASP HTML sanitizer, which decodes the text and
re-encodes ``& < > " ' + = @ ` `` as entities (``learner's`` is stored as
``learner&#39;s``), so descriptions are compared after unescaping both sides.

Drift is a field sync owns that someone else changed since the bot last wrote
the metric: the live value is compared with the newest version in
``GET /v1/metrics/{id}/versions`` whose ``updatedBy`` is the bot. Each version
there is a full snapshot of the entity, reviewers included
(``EntityRepository.listVersions``, ``storeEntityHistory``).
"""

from __future__ import annotations

import copy
import html
import json
import operator
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ol_dbt_cli.lib.data_contracts import om_fqn

if TYPE_CHECKING:
    from ol_dbt_cli.lib.manifest import ManifestRegistry
    from ol_dbt_cli.lib.metric_registry import MetricDefinition

METRIC_ENTITY_TYPE = "metric"
STATUS_FIELD = "entityStatus"
# Status CreateMetric cannot carry, so a PUT leaves it on the server
# (EntityRepository.setDefaultStatus).
PUT_DEFAULT_STATUS = "Unprocessed"
# Fields GET /v1/metrics/name/{fqn} returns only when asked for.
METRIC_FIELDS = "owners,reviewers,tags,domains,dataProducts,relatedMetrics,extension"
STARROCKS_TAG = "starrocks"
TAG_COLLECTIONS = {"Glossary": "glossaryTerms", "Classification": "tags"}

_PATCHED_FIELDS = ("description", "owners", "reviewers")
# A PUT sets these to whatever the body holds, so one the file dropped is
# cleared on the server and counts as a change.
_REPLACED_FIELDS = (
    "displayName",
    "metricExpression",
    "metricType",
    "unitOfMeasurement",
    "customUnitOfMeasurement",
    "granularity",
    "dimensions",
    "measures",
    "filters",
)
# CreateMetric names these by FQN; the entity holds references.
_FQN_LIST_FIELDS = ("relatedMetrics", "domains", "dataProducts")


@dataclass(frozen=True)
class LineageSource:
    """An OpenMetadata entity that implements a metric, the upstream end of a lineage edge."""

    entity_type: str
    fqn: str


def lineage_sources(
    metric: MetricDefinition, manifest: ManifestRegistry, service: str
) -> tuple[list[LineageSource], list[str]]:
    """Resolve each ``implemented_by`` entry to the entity OpenMetadata catalogs.

    :returns: the entities to link, and the StarRocks-only models skipped
        because OpenMetadata has no StarRocks service yet.
    :rtype: tuple[list[LineageSource], list[str]]
    :raises ValueError: when a dbt binding is in neither the manifest's nodes
        nor its StarRocks-only disabled models.
    """
    sources: list[LineageSource] = []
    skipped: list[str] = []
    for implementation in metric.implemented_by:
        binding = implementation.binding
        if binding.kind == "dbt_model":
            node = manifest.get_model(binding.target) or manifest.disabled.get(binding.target)
            if node is not None and STARROCKS_TAG in node.tags:
                skipped.append(binding.target)
                continue
        sources.append(LineageSource(binding.entity_type, om_fqn(binding, manifest, service)))
    return sources, skipped


def _live_refs(refs: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    return [{"id": r["id"], "type": r["type"]} for r in refs or [] if not r.get("inherited")]


def _ref_set(refs: list[dict[str, Any]] | None) -> set[tuple[str, str]]:
    return {(r["type"], r["id"]) for r in _live_refs(refs)}


def same_description(a: str | None, b: str | None) -> bool:
    """Whether two descriptions read the same once the server's entity encoding is undone.

    Markup the sanitizer strips (e.g. a ``<script>`` or an unknown tag) is not
    restored, so a description holding it never compares equal.
    """
    return html.unescape(a or "") == html.unescape(b or "")


def last_synced_version(history: dict[str, Any], bot: str) -> dict[str, Any] | None:
    """Return the newest version of a metric that *bot* wrote, or ``None`` when it wrote none.

    :param history: the body of ``GET /v1/metrics/{id}/versions``, whose
        ``versions`` are entity JSON strings, newest first.
    """
    for raw in history["versions"]:
        version = json.loads(raw)
        if version.get("updatedBy") == bot:
            return version
    return None


def create_body(
    metric: MetricDefinition,
    owners: list[dict[str, str]] | None,
    reviewers: list[dict[str, str]] | None,
    live: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the CreateMetric body for PUT /v1/metrics.

    :param owners: resolved owner references; ``None`` when the file does not
        set ``owners``.
    :param reviewers: resolved reviewer references, ``[]`` to clear them;
        ``None`` when the file does not set ``reviewers``, in which case the
        live reviewers are sent so the PUT keeps them.
    :param live: the metric as OpenMetadata has it, or ``None``.
    """
    body = copy.deepcopy(metric.body)
    if owners is not None:
        body["owners"] = owners
    if reviewers is not None:
        body["reviewers"] = reviewers
    elif live is not None and _live_refs(live.get("reviewers")):
        body["reviewers"] = _live_refs(live.get("reviewers"))
    return body


def _tag_key(tag: dict[str, Any]) -> tuple[str, str]:
    return (tag.get("source", "Classification"), tag["tagFQN"])


def _matches(want: Any, have: Any) -> bool:
    """Whether the live value *have* already says what the file's *want* says.

    The server adds keys of its own (e.g. a measure's fullyQualifiedName), so a
    mapping matches when every key the file sets matches; a list matches when
    it pairs off item for item, in any order.
    """
    if isinstance(want, dict):
        return isinstance(have, dict) and all(k in have and _matches(v, have[k]) for k, v in want.items())
    if isinstance(want, list):
        if not isinstance(have, list) or len(want) != len(have):
            return False
        remaining = list(have)
        for item in want:
            match = next((i for i, candidate in enumerate(remaining) if _matches(item, candidate)), None)
            if match is None:
                return False
            remaining.pop(match)
        return True
    return want == have


def structural_changes(body: dict[str, Any], live: dict[str, Any]) -> list[str]:
    """List the fields a PUT has to change on *live* to match *body*, leaving out those sync PATCHes."""
    changed = set()
    for key, want in body.items():
        if key == "name" or key in _PATCHED_FIELDS:
            continue
        have = live.get(key)
        if key == "tags":
            # A PUT merges tags and never removes one, so only a missing tag is a change.
            if not {_tag_key(t) for t in want} <= {_tag_key(t) for t in have or []}:
                changed.add(key)
        elif key in _FQN_LIST_FIELDS:
            if set(want) != {r["fullyQualifiedName"] for r in have or []}:
                changed.add(key)
        elif not _matches(want, have):
            changed.add(key)
    changed.update(key for key in _REPLACED_FIELDS if key not in body and live.get(key) not in (None, "", []))
    return sorted(changed)


def after_put(body: dict[str, Any], live: dict[str, Any] | None) -> dict[str, Any]:
    """Predict the PATCHed fields of the metric a bot PUT of *body* leaves (see the module docstring)."""
    live = live or {}
    return {
        "description": live.get("description") or body.get("description"),
        "owners": live.get("owners") or body.get("owners"),
        "reviewers": body.get("reviewers"),
        STATUS_FIELD: PUT_DEFAULT_STATUS,
    }


def drift(
    metric: MetricDefinition, body: dict[str, Any], live: dict[str, Any] | None, synced: dict[str, Any] | None
) -> list[str]:
    """List the fields someone other than the sync bot changed since the bot last wrote them.

    Only description, reviewers and status are compared, and only where the
    file sets them. A field is drift when its live value differs both from
    the file's and from *synced*, so a file change to a field nobody touched
    in OpenMetadata is not drift, and neither is an edit that already matches
    the file.

    :param synced: the newest version the bot wrote (*live* itself when the
        bot made the last change), or ``None`` when it never wrote the metric,
        in which case every difference from the file counts.
    """
    if live is None:
        return []

    def edited(field: str, want: Any, same: Callable[[Any, Any], bool]) -> bool:
        have = live.get(field)
        return not same(have, want) and (synced is None or not same(have, synced.get(field)))

    found = []
    if "description" in body and edited("description", body["description"], same_description):
        found.append("description")
    if "reviewers" in metric.body and edited("reviewers", body["reviewers"], lambda a, b: _ref_set(a) == _ref_set(b)):
        found.append("reviewers")
    if metric.status is not None and edited(STATUS_FIELD, metric.status, operator.eq):
        found.append(f"{STATUS_FIELD} ({live.get(STATUS_FIELD)} in OpenMetadata, {metric.status} in the file)")
    return found


def patch_ops(
    metric: MetricDefinition, body: dict[str, Any], current: dict[str, Any], status: str | None
) -> list[dict[str, Any]]:
    """JSON Patch operations that make *current* match the file.

    :param current: the metric as it stands before the PATCH.
    :param status: the status to leave, the file's or, when it sets none, the
        one the metric had before a PUT reset it; ``None`` leaves it alone.
    """
    ops: list[dict[str, Any]] = []
    if "description" in body and not same_description(current.get("description"), body["description"]):
        ops.append({"op": "add", "path": "/description", "value": body["description"]})
    for field in ("owners", "reviewers"):
        if field in metric.body and _ref_set(current.get(field)) != _ref_set(body[field]):
            ops.append({"op": "add", "path": f"/{field}", "value": body[field]})
    if status is not None and current.get(STATUS_FIELD) != status:
        ops.append({"op": "add", "path": f"/{STATUS_FIELD}", "value": status})
    return ops


def lineage_edge(source_id: str, source: LineageSource, metric_id: str, file_name: str) -> dict[str, Any]:
    """Build the AddLineage body for one table-to-metric edge.

    No column lineage: OpenMetadata stores none for a Metric
    (LineageRepository, "Metric column level lineage is not supported").
    """
    return {
        "edge": {
            "fromEntity": {"id": source_id, "type": source.entity_type},
            "toEntity": {"id": metric_id, "type": METRIC_ENTITY_TYPE},
            "lineageDetails": {"description": f"implemented_by in metrics/{file_name}"},
        }
    }

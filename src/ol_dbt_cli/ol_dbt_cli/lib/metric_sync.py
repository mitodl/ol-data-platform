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
- replaces the metric's own fields and its related metrics and assets
  (``MetricUpdater.entitySpecificUpdate``), so a body without them clears them.

So sync PUTs only to create the metric or change one of its own fields, sends
the live reviewers and related metrics when the file does not set them and the
live assets always, and then PATCHes
description, owners, reviewers and status wherever they still differ, with
the file's digest (below) when it changed. A PATCH also removes the tags the
bot applied that the file has dropped (``stale_tags``); tags applied by anyone
else stay. An unchanged file writes no Metric
field; it re-PUTs its lineage edges, which the server upserts.

A file's status is Draft or Approved, and the bot is not a metric reviewer.
OpenMetadata checks the updater only on a move from In Review to Approved or
Rejected (``EntityUpdater.updateEntityStatus``). A PUT would still end a
review: it resets In Review to Unprocessed, and a move out of Unprocessed is
not checked. So sync writes nothing for a metric that is In Review, lineage
included, with or without ``--force``, until a reviewer approves or rejects it
or someone moves it out of In Review. A Rejected metric is held the same way:
a reviewer turned the definition down, and any PUT resets the status to
Unprocessed, as would the status PATCH. The guard
reads only the live status, so moving the metric out of Rejected releases it;
the expected process is to change the file too (e.g. back to Draft with the
definition fixed). Moved out with no file change, a file that says Approved
shows status drift, and ``--force`` would approve it again.

The server stores ``DescriptionSanitizer.sanitize(description)`` on create,
PUT and PATCH. That is the OWASP HTML sanitizer, which decodes the text and
re-encodes ``& < > " ' + = @ ` `` as entities (``learner's`` is stored as
``learner&#39;s``), so descriptions are compared after unescaping both sides.

Drift is a field sync owns that someone else changed since the bot last wrote
the metric: the live value is compared with the newest version in
``GET /v1/metrics/{id}/versions`` whose ``updatedBy`` is the bot. Each version
there is a full snapshot of the entity, owners and reviewers included
(``EntityRepository.listVersions``, ``storeEntityHistory``).

A file that adopts an edit made in OpenMetadata changes no field, so without
a write the bot's newest version would keep the old value, and the next file
change to that field would read as drift. Sync therefore keeps a SHA-256 of
what the file declares in the metric's ``metricFileSha256`` custom property
(defined on the metric type by ol-infrastructure) and PATCHes it whenever the
file changed. A PATCH that changes only ``extension`` records a new version
with the caller as ``updatedBy`` (``updateExtension``, ``updateVersion``), and
that version holds the adopted value. When the bot also wrote the version
before it within the session timeout, OpenMetadata folds the two into one
version (``EntityUpdater.consolidateChanges``); either way the newest bot
version is the one just written. A bot's PUT keeps the stored extension
(``updateExtension``), so the marker lands only by PATCH.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
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
# Live statuses sync never writes over, even with --force: a review in
# progress, which only a reviewer may end, and a reviewer's rejection. Each maps
# to the sync label and what clears it.
REVIEW_HOLD_STATUSES = {
    "In Review": (
        "in review",
        "In Review in OpenMetadata, and sync does not override a review in progress, even with --force. A "
        "reviewer approves or rejects it in OpenMetadata, or someone moves it out of In Review; then sync again.",
    ),
    "Rejected": (
        "rejected",
        "Rejected by a reviewer in OpenMetadata, and sync does not override a rejection, even with --force. Change "
        "the metric file (e.g. back to Draft with the definition fixed) and move the metric out of Rejected in "
        "OpenMetadata; then sync again.",
    ),
}
# Fields GET /v1/metrics/name/{fqn} returns only when asked for.
METRIC_FIELDS = "owners,reviewers,tags,relatedMetrics,assets,extension"
EXTENSION_FIELD = "extension"
# The custom property holding file_digest(), a string property on the metric
# type that ol-infrastructure defines (om_server_config).
FILE_DIGEST_PROPERTY = "metricFileSha256"
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
# Keys the server derives from others, e.g. a measure's fullyQualifiedName
# (MetricRepository.setFullyQualifiedName).
_GENERATED_KEYS = frozenset({"fullyQualifiedName"})


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


def file_digest(metric: MetricDefinition) -> str:
    """SHA-256 of what the metric file declares: its ``metric`` body, ``status`` and ``implemented_by``."""
    declared = {
        "metric": metric.body,
        "status": metric.status,
        "implemented_by": [dataclasses.asdict(i) for i in metric.implemented_by],
    }
    return hashlib.sha256(json.dumps(declared, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def create_body(
    metric: MetricDefinition,
    owners: list[dict[str, str]] | None,
    reviewers: list[dict[str, str]] | None,
    live: dict[str, Any] | None,
    bot: str,
) -> dict[str, Any]:
    """Build the CreateMetric body for PUT /v1/metrics.

    :param owners: resolved owner references; ``None`` when the file does not
        set ``owners``.
    :param reviewers: resolved reviewer references, ``[]`` to clear them;
        ``None`` when the file does not set ``reviewers``, in which case the
        live reviewers are sent so the PUT keeps them.
    :param live: the metric as OpenMetadata has it, or ``None``. Its assets,
        and its related metrics when the file sets none, are sent so a PUT
        keeps them.
    :param bot: the sync bot's user name, sent as each tag's ``appliedBy``.
        Creating a metric stores the request's ``appliedBy``; a PUT or PATCH
        to an existing one stamps the caller on the tags it adds
        (``EntityUpdater.updateTags``). Either way the tags sync added carry
        the bot's name, which is how ``stale_tags`` tells them apart.
    """
    body = copy.deepcopy(metric.body)
    for tag in body.get("tags", []):
        tag["appliedBy"] = bot
    if owners is not None:
        body["owners"] = owners
    if reviewers is not None:
        body["reviewers"] = reviewers
    elif live is not None and _live_refs(live.get("reviewers")):
        body["reviewers"] = _live_refs(live.get("reviewers"))
    if live is not None:
        if live.get("assets"):
            body["assets"] = _live_refs(live["assets"])
        if "relatedMetrics" not in body and live.get("relatedMetrics"):
            body["relatedMetrics"] = [r["fullyQualifiedName"] for r in live["relatedMetrics"]]
    return body


def _tag_key(tag: dict[str, Any]) -> tuple[str, str]:
    return (tag.get("source", "Classification"), tag["tagFQN"])


def stale_tags(metric: MetricDefinition, current: dict[str, Any], bot: str) -> list[dict[str, Any]]:
    """List the tags on *current* that the bot applied and the file no longer declares.

    A tag someone applied in OpenMetadata is never stale, even when the file
    once listed it too: the server keeps the first ``appliedBy`` of a tag.
    """
    declared = {_tag_key(t) for t in metric.body.get("tags", [])}
    return [t for t in current.get("tags") or [] if t.get("appliedBy") == bot and _tag_key(t) not in declared]


def _matches(want: Any, have: Any) -> bool:
    """Whether the live value *have* already says what the file's *want* says.

    A mapping matches when every key the file sets matches and the live value
    has no other key with a value, except those the server derives; a key the
    file dropped would otherwise stay on the server. A list matches when it
    pairs off item for item, in any order.
    """
    if isinstance(want, dict):
        if not isinstance(have, dict):
            return False
        dropped = have.keys() - want.keys() - _GENERATED_KEYS
        return all(have[k] in (None, "", []) for k in dropped) and all(
            k in have and _matches(v, have[k]) for k, v in want.items()
        )
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
        # assets is always the live list (create_body).
        if key in ("name", "assets") or key in _PATCHED_FIELDS:
            continue
        have = live.get(key)
        if key == "tags":
            # A PUT merges tags and never removes one, so only a missing tag is
            # a change; patch_ops removes the stale ones.
            if not {_tag_key(t) for t in want} <= {_tag_key(t) for t in have or []}:
                changed.add(key)
        elif key == "relatedMetrics":
            # CreateMetric names them by FQN; the entity holds references.
            if set(want) != {r["fullyQualifiedName"] for r in have or []}:
                changed.add(key)
        elif not _matches(want, have):
            changed.add(key)
    changed.update(key for key in _REPLACED_FIELDS if key not in body and live.get(key) not in (None, "", []))
    return sorted(changed)


def after_put(body: dict[str, Any], live: dict[str, Any] | None) -> dict[str, Any]:
    """Predict the PATCHed fields and extension of the metric a bot PUT of *body* leaves (see the module docstring)."""
    live = live or {}
    live_tags = live.get("tags") or []
    live_keys = {_tag_key(t) for t in live_tags}
    return {
        "description": live.get("description") or body.get("description"),
        "owners": live.get("owners") or body.get("owners"),
        "reviewers": body.get("reviewers"),
        "tags": live_tags + [t for t in body.get("tags", []) if _tag_key(t) not in live_keys],
        STATUS_FIELD: PUT_DEFAULT_STATUS,
        EXTENSION_FIELD: live.get(EXTENSION_FIELD),
    }


def drift(
    metric: MetricDefinition, body: dict[str, Any], live: dict[str, Any] | None, synced: dict[str, Any] | None
) -> list[str]:
    """List the fields someone other than the sync bot changed since the bot last wrote them.

    Only description, owners, reviewers and status are compared, the first
    three only where the file sets them. Owners and reviewers compare as sets of entity
    references. A field is drift when its live value differs both from
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
    for field in ("owners", "reviewers"):
        if field in metric.body and edited(field, body[field], lambda a, b: _ref_set(a) == _ref_set(b)):
            found.append(field)
    if edited(STATUS_FIELD, metric.status, operator.eq):
        found.append(f"{STATUS_FIELD} ({live.get(STATUS_FIELD)} in OpenMetadata, {metric.status} in the file)")
    return found


def patch_ops(
    metric: MetricDefinition, body: dict[str, Any], current: dict[str, Any], bot: str
) -> list[dict[str, Any]]:
    """JSON Patch operations that make *current* match the file and record its digest.

    A PATCH replaces the tag list, deleting each tag it leaves out
    (``EntityUpdater.updateTags``), so stale tags go by sending the rest.

    :param current: the metric as it stands before the PATCH.
    :param bot: the sync bot's user name (see ``stale_tags``).
    """
    ops: list[dict[str, Any]] = []
    if "description" in body and not same_description(current.get("description"), body["description"]):
        ops.append({"op": "add", "path": "/description", "value": body["description"]})
    for field in ("owners", "reviewers"):
        if field in metric.body and _ref_set(current.get(field)) != _ref_set(body[field]):
            ops.append({"op": "add", "path": f"/{field}", "value": body[field]})
    stale = {_tag_key(t) for t in stale_tags(metric, current, bot)}
    if stale:
        kept = [t for t in current["tags"] if _tag_key(t) not in stale]
        ops.append({"op": "add", "path": "/tags", "value": kept})
    if current.get(STATUS_FIELD) != metric.status:
        ops.append({"op": "add", "path": f"/{STATUS_FIELD}", "value": metric.status})
    digest = file_digest(metric)
    extension = current.get(EXTENSION_FIELD)
    if extension is None:
        ops.append({"op": "add", "path": f"/{EXTENSION_FIELD}", "value": {FILE_DIGEST_PROPERTY: digest}})
    elif extension.get(FILE_DIGEST_PROPERTY) != digest:
        # JSON Patch replaces only a member that exists.
        op = "replace" if FILE_DIGEST_PROPERTY in extension else "add"
        ops.append({"op": op, "path": f"/{EXTENSION_FIELD}/{FILE_DIGEST_PROPERTY}", "value": digest})
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

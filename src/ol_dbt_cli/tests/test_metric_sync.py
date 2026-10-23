"""Tests for `ol-dbt metrics sync` against an in-memory stand-in for the OpenMetadata API.

The stand-in replaces only ``OpenMetadataClient.request``, so name lookups,
404 handling and the command's ordering run as they would against a server.
Its PUT and PATCH follow what the OpenMetadata 2.0.5 source does for a bot
(see ``ol_dbt_cli.lib.metric_sync``); that behaviour is not checked against a
live server here. A PATCH that changes nothing records no version, and a PATCH
by whoever wrote the current version folds into it, as the server does for one
user within its session timeout (``EntityUpdater.consolidateChanges``, every
call here being within it).
"""

from __future__ import annotations

import copy
import dataclasses
import html
import io
import json
import shutil
import urllib.error
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pytest
import yaml

from ol_dbt_cli.commands import metrics as metrics_command
from ol_dbt_cli.lib import openmetadata
from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry
from ol_dbt_cli.lib.metric_registry import SCHEMA_SNAPSHOT, SCHEMA_VERSION_KEY, load_metrics
from ol_dbt_cli.lib.metric_sync import (
    FILE_DIGEST_PROPERTY,
    display_name_conflict,
    file_digest,
    lineage_sources,
    same_description,
    structural_changes,
)
from ol_dbt_cli.lib.openmetadata import OpenMetadataClient, OpenMetadataError

FIXTURES = Path(__file__).parent / "fixtures" / "metrics"
SERVICE = "Starburst Galaxy"
BOT = "metrics-sync-bot"
PROGRESS = "afact_learner_courserun_progress"
PROGRESS_FQN = f"{SERVICE}.ol_data_lake_production.ol_warehouse_production_dimensional.{PROGRESS}"
# Every other model the fixtures bind is a b2b StarRocks view.
STARROCKS_VIEWS = sorted({i.binding.target for m in load_metrics(FIXTURES) for i in m.implemented_by} - {PROGRESS})
SNAPSHOT_VERSION = json.loads(SCHEMA_SNAPSHOT.read_text())[SCHEMA_VERSION_KEY]
# What the OWASP sanitizer behind DescriptionSanitizer encodes in text
# (Encoding.REPLACEMENTS in owasp-java-html-sanitizer 20260101.1).
_SANITIZER_ENTITIES = {
    '"': "&#34;",
    "&": "&amp;",
    "'": "&#39;",
    "+": "&#43;",
    "<": "&lt;",
    "=": "&#61;",
    ">": "&gt;",
    "@": "&#64;",
    "`": "&#96;",
}
TIER1 = {"tagFQN": "Tier.Tier1", "source": "Classification", "labelType": "Manual", "state": "Confirmed"}
COMPLETION_TERM = {
    "tagFQN": "Learning.Course completion",
    "source": "Glossary",
    "labelType": "Manual",
    "state": "Confirmed",
}
TEAM = [{"type": "team", "name": "data-platform"}]
TEAM_REF = [{"id": "team-1", "type": "team"}]


def _sanitize(description: str | None) -> str | None:
    """Store a plain-text description the way DescriptionSanitizer.sanitize does: decode, then re-encode."""
    if description is None:
        return None
    return "".join(_SANITIZER_ENTITIES.get(c, c) for c in html.unescape(description))


def _stamp_tags(before: list[dict[str, Any]], after: list[dict[str, Any]], user: str) -> list[dict[str, Any]]:
    """Store *after* the way EntityUpdater.updateTags does: a kept tag keeps its appliedBy, an added one gets *user*."""
    stored = {(t["source"], t["tagFQN"]): t for t in before}
    return [stored.get((t["source"], t["tagFQN"])) or {**t, "appliedBy": user} for t in after]


def _manifest() -> ManifestRegistry:
    """Build a production manifest: the progress model on Trino, the b2b views disabled and tagged starrocks."""
    registry = ManifestRegistry()
    registry.by_name[PROGRESS] = ManifestModel(
        unique_id=f"model.open_learning.{PROGRESS}",
        name=PROGRESS,
        resource_type="model",
        original_file_path="",
        schema="ol_warehouse_production_dimensional",
        database="ol_data_lake_production",
    )
    for view in STARROCKS_VIEWS:
        registry.disabled[view] = ManifestModel(
            unique_id=f"model.open_learning.{view}",
            name=view,
            resource_type="model",
            original_file_path="",
            schema="ol_warehouse_production_b2b_analytics",
            database="ol_data_lake_production",
            tags=["starrocks"],
        )
    return registry


class FakeOpenMetadata(OpenMetadataClient):
    """Holds metrics in memory and applies a bot's PUT and PATCH the way OpenMetadata 2.0.5 does."""

    def __init__(self) -> None:
        """Start with no metrics, one team, one glossary term and the progress model's table."""
        super().__init__("https://om.example/api", "token")
        self.version = SNAPSHOT_VERSION
        self.teams = {"data-platform": "team-1"}
        self.glossary_terms = {"Learning.Course completion"}
        self.tables = {PROGRESS_FQN: "table-1"}
        self.metrics: dict[str, dict[str, Any]] = {}
        # Every version of each metric, oldest first.
        self.history: dict[str, list[dict[str, Any]]] = {}
        self.lineage: list[dict[str, Any]] = []
        self.calls: list[tuple[str, str]] = []
        # Metrics whose GET answers 500.
        self.broken: set[str] = set()
        # String custom properties defined on the metric type.
        self.custom_properties = {FILE_DIGEST_PROPERTY}

    def writes(self) -> list[tuple[str, str]]:
        return [c for c in self.calls if c[0] != "GET"]

    def edit_in_ui(self, name: str, user: str, **fields: Any) -> None:
        """Apply a person's PATCH, which the server sanitizes like any other."""
        updated = copy.deepcopy(self.metrics[name])
        if "description" in fields:
            fields["description"] = _sanitize(fields["description"])
        if "tags" in fields:
            fields["tags"] = _stamp_tags(updated.get("tags", []), fields["tags"], user)
        updated.update(fields)
        self._store_patch(updated, user)

    def _record(self, metric: dict[str, Any]) -> None:
        self.history.setdefault(metric["name"], []).append(copy.deepcopy(metric))

    def _store_patch(self, updated: dict[str, Any], user: str) -> dict[str, Any]:
        """Store a PATCH the way EntityUpdater.storeUpdate does."""
        current = self.metrics[updated["name"]]
        bookkeeping = ("updatedBy", "version")
        if {k: v for k, v in updated.items() if k not in bookkeeping} == {
            k: v for k, v in current.items() if k not in bookkeeping
        }:
            return copy.deepcopy(current)
        history = self.history[updated["name"]]
        if current["version"] > 0.1 and current["updatedBy"] == user:
            updated.update(updatedBy=user, version=current["version"])
            history[-1] = copy.deepcopy(updated)
        else:
            updated.update(updatedBy=user, version=round(current["version"] + 0.1, 1))
            history.append(copy.deepcopy(updated))
        self.metrics[updated["name"]] = updated
        return copy.deepcopy(updated)

    def _patch(self, metric: dict[str, Any], op: dict[str, Any]) -> None:
        """Apply one JSON Patch operation as RFC 6902 has it: a parent must exist, and replace needs the member."""
        *parents, key = op["path"].lstrip("/").split("/")
        target = metric
        for parent in parents:
            if not isinstance(target.get(parent), dict):
                raise OpenMetadataError(f"PATCH -> 400: no {parent} to apply {op['path']} to", 400)
            target = target[parent]
        assert op["op"] in ("add", "replace")
        if op["op"] == "replace" and key not in target:
            raise OpenMetadataError(f"PATCH -> 400: no {op['path']} to replace", 400)
        target[key] = _sanitize(op["value"]) if op["path"] == "/description" else copy.deepcopy(op["value"])

    def _check_extension(self, path: str, original: dict[str, Any], updated: dict[str, Any]) -> None:
        """Validate each added or changed custom property (EntityUpdater.updateExtension)."""
        before = original.get("extension") or {}
        for key, value in (updated.get("extension") or {}).items():
            if before.get(key) == value:
                continue
            if key not in self.custom_properties:
                message = f"Unknown custom field {key}"
            elif not isinstance(value, str):
                message = f"Custom field {key} has invalid JSON [$: {type(value).__name__} found, string expected]"
            else:
                continue
            raise OpenMetadataError(f"PATCH {path} -> 400: {json.dumps({'code': 400, 'message': message})}", 400)

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        query: dict[str, str] | None = None,
        content_type: str = "application/json",
    ) -> Any:
        self.calls.append((method, path))
        route = (method, *path.strip("/").split("/")[1:])
        match route:
            case ("GET", "system", "version"):
                return {"version": self.version}
            case ("GET", "users", "loggedInUser"):
                return {"name": BOT, "isBot": True}
            case ("GET", "teams", "name", team) if team in self.teams:
                return {"id": self.teams[team]}
            case ("GET", "glossaryTerms", "name", term) if unquote(term) in self.glossary_terms:
                return {"id": "term-1"}
            case ("GET", "tables", "name", fqn) if unquote(fqn) in self.tables:
                return {"id": self.tables[unquote(fqn)]}
            case ("GET", "metrics", "name", name) if name in self.broken:
                raise OpenMetadataError(f"{method} {path} -> 500: boom", 500)
            case ("GET", "metrics", "name", name) if name in self.metrics:
                return copy.deepcopy(self.metrics[name])
            case ("GET", "metrics", metric_id, "versions"):
                [name] = [n for n, m in self.metrics.items() if m["id"] == metric_id]
                # EntityRepository.listVersions: entity JSON strings, newest first.
                return {"entityType": "metric", "versions": [json.dumps(v) for v in reversed(self.history[name])]}
            case ("PUT", "metrics"):
                return self._put_metric(body)
            case ("PATCH", "metrics", metric_id):
                assert content_type == openmetadata.JSON_PATCH_CONTENT_TYPE
                [original] = [m for m in self.metrics.values() if m["id"] == metric_id]
                updated = copy.deepcopy(original)
                for op in body:
                    self._patch(updated, op)
                if "tags" in updated:
                    updated["tags"] = _stamp_tags(original.get("tags", []), updated["tags"], BOT)
                self._check_extension(path, original, updated)
                return self._store_patch(updated, BOT)
            case ("PUT", "lineage"):
                self.lineage.append(body)
                # LineageResource.addLineage answers 200 with no body.
                return None
        raise OpenMetadataError(f"{method} {path} -> 404: not found", 404)

    def _related_refs(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        """Turn CreateMetric's relatedMetrics FQNs into the references the entity holds."""
        return [
            {"id": self.metrics[fqn]["id"], "type": "metric", "fullyQualifiedName": fqn}
            for fqn in body.get("relatedMetrics", [])
        ]

    def _put_metric(self, body: dict[str, Any]) -> dict[str, Any]:
        live = self.metrics.get(body["name"])
        if live is None:
            created = {**copy.deepcopy(body), "id": f"metric-{len(self.metrics) + 1}", "version": 0.1}
            created["relatedMetrics"] = self._related_refs(body)
            created.update(fullyQualifiedName=body["name"], entityStatus="Unprocessed", updatedBy=BOT)
            created["description"] = _sanitize(body.get("description"))
            self.metrics[body["name"]] = created
            self._record(created)
            return copy.deepcopy(created)
        updated = {**copy.deepcopy(body), "id": live["id"], "fullyQualifiedName": live["fullyQualifiedName"]}
        # MetricUpdater replaces both relationship lists with the body's.
        updated["relatedMetrics"] = self._related_refs(body)
        updated["assets"] = body.get("assets", [])
        # keepsStoredDescription for a bot PUT; otherwise updateDescription sanitizes.
        updated["description"] = live.get("description") or _sanitize(body.get("description"))
        # DefaultBotPolicy denies a bot EditDisplayName, so updateDisplayName keeps a stored one.
        if live.get("displayName"):
            updated["displayName"] = live["displayName"]
        if live.get("owners"):
            updated["owners"] = live["owners"]
        updated["reviewers"] = body.get("reviewers") or []
        # A PUT merges tags and never removes one.
        live_tags = live.get("tags", [])
        live_keys = {(t["source"], t["tagFQN"]) for t in live_tags}
        added = [t for t in body.get("tags", []) if (t["source"], t["tagFQN"]) not in live_keys]
        updated["tags"] = _stamp_tags(live_tags, live_tags + added, BOT)
        # A bot's PUT keeps the stored extension (updateExtension).
        if "extension" in live:
            updated["extension"] = live["extension"]
        updated.update(entityStatus="Unprocessed", updatedBy=BOT, version=round(live["version"] + 0.1, 1))
        self.metrics[body["name"]] = updated
        self._record(updated)
        return copy.deepcopy(updated)


@pytest.fixture
def metrics_dir(tmp_path: Path) -> Path:
    shutil.copytree(FIXTURES, tmp_path / "metrics")
    return tmp_path / "metrics"


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch, metrics_dir: Path) -> FakeOpenMetadata:
    fake = FakeOpenMetadata()
    monkeypatch.setattr(OpenMetadataClient, "from_env", classmethod(lambda cls: fake))
    monkeypatch.setattr(metrics_command, "get_repo_root", lambda: metrics_dir.parent)
    monkeypatch.setattr(metrics_command, "load_catalog_manifest", lambda dbt_dir, path: _manifest())
    return fake


def _sync(metrics_dir: Path, **kwargs: Any) -> bool:
    """Run sync; return whether it exited non-zero."""
    try:
        metrics_command.sync(service=SERVICE, metrics_dir_path=str(metrics_dir), **kwargs)
    except SystemExit as exc:
        return exc.code != 0
    return False


def _edit_file(metrics_dir: Path, name: str, **metric_fields: Any) -> None:
    path = metrics_dir / f"{name}.yaml"
    raw = yaml.safe_load(path.read_text())
    raw["metric"].update(metric_fields)
    path.write_text(yaml.safe_dump(raw))


def test_dry_run_writes_nothing(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _sync(metrics_dir, dry_run=True) is False
    assert server.writes() == []
    out = capsys.readouterr().out
    assert out.count("would") == 2
    assert "then PATCH entityStatus, extension" in out
    assert "skipped" in out
    assert "mv_b2b_learner_enrollment" in out


def test_first_sync_creates_each_metric_approved_with_lineage_from_its_table(
    server: FakeOpenMetadata, metrics_dir: Path
) -> None:
    assert _sync(metrics_dir) is False
    for name in ("certified_learners", "learner_completion_status"):
        metric = server.metrics[name]
        # Created Unprocessed by the PUT, set by the PATCH.
        assert metric["entityStatus"] == "Approved"
        assert metric["description"].startswith(("Distinct learners", "Where a learner"))
    # The StarRocks views are skipped, so each metric gets one edge, from the progress model.
    assert [(e["edge"]["fromEntity"], e["edge"]["toEntity"]["type"]) for e in server.lineage] == [
        ({"id": "table-1", "type": "table"}, "metric")
    ] * 2
    assert all("columnsLineage" not in e["edge"]["lineageDetails"] for e in server.lineage)


def test_unchanged_files_write_nothing_but_lineage(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    versions = {name: m["version"] for name, m in server.metrics.items()}
    server.calls.clear()
    assert _sync(metrics_dir) is False
    assert {c for c in server.writes()} == {("PUT", "/v1/lineage")}
    assert {name: m["version"] for name, m in server.metrics.items()} == versions


def test_description_change_lands_by_patch_without_a_put(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    _edit_file(metrics_dir, "certified_learners", description="Learners with a certificate.")
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert ("PUT", "/v1/metrics") not in server.calls
    assert server.metrics["certified_learners"]["description"] == "Learners with a certificate."


def test_structural_change_puts_then_patches_what_the_put_kept(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    _edit_file(metrics_dir, "certified_learners", metricType="SUM", description="Learners with a certificate.")
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    metric = server.metrics["certified_learners"]
    assert metric["metricType"] == "SUM"
    # The bot PUT kept the stored description and reset the status; the PATCH restores both from the file.
    assert metric["description"] == "Learners with a certificate."
    assert metric["entityStatus"] == "Approved"


STALE_NAME = "stale certified_learners: OpenMetadata has displayName 'Certified learners' and the file has"


def test_a_display_name_change_in_the_file_is_reported_and_fails_the_metric(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir)
    _edit_file(
        metrics_dir, "certified_learners", displayName="Learners certified", description="Learners with a certificate."
    )
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    # No PUT for a name the server would keep; the rest of the file still lands.
    assert ("PUT", "/v1/metrics") not in server.calls
    metric = server.metrics["certified_learners"]
    assert metric["displayName"] == "Certified learners"
    assert metric["description"] == "Learners with a certificate."
    assert f"{STALE_NAME} 'Learners certified'" in " ".join(capsys.readouterr().out.split())
    # The next run neither PUTs again nor passes.
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    assert server.writes() == [("PUT", "/v1/lineage")]


def test_a_display_name_renamed_in_openmetadata_is_reported_not_put_back(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", displayName="Certified")
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    assert server.writes() == [("PUT", "/v1/lineage")]
    out = " ".join(capsys.readouterr().out.split())
    assert "OpenMetadata has displayName 'Certified' and the file has 'Certified learners'" in out


def test_a_display_name_the_file_dropped_is_reported(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir)
    path = metrics_dir / "certified_learners.yaml"
    raw = yaml.safe_load(path.read_text())
    del raw["metric"]["displayName"]
    path.write_text(yaml.safe_dump(raw))
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    assert ("PUT", "/v1/metrics") not in server.calls
    assert server.metrics["certified_learners"]["displayName"] == "Certified learners"
    assert f"{STALE_NAME} none" in " ".join(capsys.readouterr().out.split())


def test_a_structural_change_puts_once_and_keeps_the_stored_display_name(
    server: FakeOpenMetadata, metrics_dir: Path
) -> None:
    _sync(metrics_dir)
    _edit_file(metrics_dir, "certified_learners", metricType="SUM", displayName="Learners certified")
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    assert server.calls.count(("PUT", "/v1/metrics")) == 1
    metric = server.metrics["certified_learners"]
    assert (metric["metricType"], metric["displayName"]) == ("SUM", "Certified learners")
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    assert ("PUT", "/v1/metrics") not in server.calls


def test_dry_run_reports_a_stale_display_name(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir)
    _edit_file(metrics_dir, "certified_learners", displayName="Learners certified")
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir, names=("certified_learners",), dry_run=True) is True
    assert server.writes() == []
    assert STALE_NAME in " ".join(capsys.readouterr().out.split())


def test_a_metric_without_a_display_name_gets_the_files(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", displayName=None)
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert ("PUT", "/v1/metrics") in server.calls
    assert server.metrics["certified_learners"]["displayName"] == "Certified learners"


QUOTED_DESCRIPTION = 'A learner\'s "active" enrollments, counted once per run.'


def test_description_with_quotes_is_stored_encoded_and_not_patched_again(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _edit_file(metrics_dir, "certified_learners", description=QUOTED_DESCRIPTION)
    _sync(metrics_dir, names=("certified_learners",))
    stored = server.metrics["certified_learners"]["description"]
    assert stored == "A learner&#39;s &#34;active&#34; enrollments, counted once per run."
    version = server.metrics["certified_learners"]["version"]
    server.calls.clear()
    capsys.readouterr()

    assert _sync(metrics_dir, names=("certified_learners",), dry_run=True) is False
    assert "then PATCH" not in capsys.readouterr().out
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.writes() == [("PUT", "/v1/lineage")]
    assert server.metrics["certified_learners"]["version"] == version


def test_ui_tag_edit_after_a_quoted_description_is_not_description_drift(
    server: FakeOpenMetadata, metrics_dir: Path
) -> None:
    _edit_file(metrics_dir, "certified_learners", description=QUOTED_DESCRIPTION)
    _sync(metrics_dir, names=("certified_learners",))
    server.edit_in_ui("certified_learners", "jane", tags=[TIER1])
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.writes() == [("PUT", "/v1/lineage")]


@pytest.mark.parametrize(
    ("field", "ui_value"),
    [
        ("description", "Edited in the UI."),
        ("entityStatus", "Deprecated"),
        ("reviewers", []),
        ("owners", [{"id": "user-9", "type": "user"}]),
    ],
)
def test_ui_edit_to_a_synced_field_is_drift_until_forced(
    server: FakeOpenMetadata, metrics_dir: Path, field: str, ui_value: Any
) -> None:
    _edit_file(metrics_dir, "certified_learners", owners=TEAM, reviewers=TEAM)
    _sync(metrics_dir, names=("certified_learners",))
    synced = copy.deepcopy(server.metrics["certified_learners"][field])
    server.edit_in_ui("certified_learners", "jane", **{field: ui_value})
    # A later edit by someone else to a field sync does not own does not hide it.
    server.edit_in_ui("certified_learners", "bob", tags=[TIER1])
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    assert server.writes() == []
    assert server.metrics["certified_learners"][field] == ui_value

    assert _sync(metrics_dir, names=("certified_learners",), force=True) is False
    assert server.metrics["certified_learners"][field] == synced


# The status a person sets in the UI, the sync line (label padded to 10 columns) and how it continues.
REVIEW_HOLDS = {
    "In Review": "in review certified_learners: In Review in OpenMetadata, and sync does not override a review in "
    "progress, even with --force. A reviewer approves or rejects it",
    "Rejected": "rejected  certified_learners: Rejected by a reviewer in OpenMetadata, and sync does not override a "
    "rejection, even with --force. Change the metric file (e.g. back to Draft with the definition fixed) and move "
    "the metric out of Rejected",
}


@pytest.mark.parametrize("status", sorted(REVIEW_HOLDS))
@pytest.mark.parametrize("structural", [False, True])
@pytest.mark.parametrize("force", [False, True])
def test_a_metric_in_review_or_rejected_is_left_alone_and_fails_while_the_others_sync(
    server: FakeOpenMetadata,
    metrics_dir: Path,
    capsys: pytest.CaptureFixture[str],
    force: bool,
    structural: bool,
    status: str,
) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", reviewers=TEAM_REF, entityStatus=status)
    if structural:
        # A PUT alone would reset the status to Unprocessed.
        _edit_file(metrics_dir, "certified_learners", metricType="SUM")
    metric_id = server.metrics["certified_learners"]["id"]
    version = server.metrics["certified_learners"]["version"]
    server.calls.clear()
    server.lineage.clear()
    capsys.readouterr()

    assert _sync(metrics_dir, force=force) is True
    raw = capsys.readouterr().out
    # Rich wraps lines at the console width.
    out = " ".join(raw.split())
    assert " ".join(REVIEW_HOLDS[status].split()) in out
    # One line for the metric, besides the StarRocks lineage it skips, with its padded label.
    lines = [line for line in raw.splitlines() if "certified_learners:" in line and not line.startswith("skipped")]
    assert len(lines) == 1
    assert lines[0].startswith(REVIEW_HOLDS[status].split(":")[0] + ":")
    assert "drift" not in out
    assert "overriding" not in out
    assert ("PUT", "/v1/metrics") not in server.writes()
    assert ("PATCH", f"/v1/metrics/{metric_id}") not in server.writes()
    assert all(e["edge"]["toEntity"]["id"] != metric_id for e in server.lineage)
    metric = server.metrics["certified_learners"]
    assert (metric["entityStatus"], metric["version"]) == (status, version)
    # The other metric still synced, lineage included.
    assert "synced learner_completion_status" in out
    assert [e["edge"]["toEntity"]["id"] for e in server.lineage] == [server.metrics["learner_completion_status"]["id"]]


@pytest.mark.parametrize("file_status", ["Approved", "Draft"])
def test_moving_a_rejected_metric_to_draft_releases_it(
    server: FakeOpenMetadata, metrics_dir: Path, file_status: str
) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", entityStatus="Rejected")
    path = metrics_dir / "certified_learners.yaml"
    path.write_text(path.read_text().replace("status: Approved", f"status: {file_status}"))
    server.edit_in_ui("certified_learners", "jane", entityStatus="Draft")
    if file_status == "Approved":
        # The file was not changed, so the move reads as status drift.
        assert _sync(metrics_dir, names=("certified_learners",)) is True
        assert server.metrics["certified_learners"]["entityStatus"] == "Draft"
        assert _sync(metrics_dir, names=("certified_learners",), force=True) is False
        assert server.metrics["certified_learners"]["entityStatus"] == "Approved"
    else:
        assert _sync(metrics_dir, names=("certified_learners",)) is False
        assert server.metrics["certified_learners"]["entityStatus"] == "Draft"


@pytest.mark.parametrize("status", sorted(REVIEW_HOLDS))
def test_dry_run_reports_a_metric_in_review_or_rejected(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str], status: str
) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", entityStatus=status)
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir, dry_run=True, force=True) is True
    out = " ".join(capsys.readouterr().out.split())
    assert " ".join(REVIEW_HOLDS[status].split()) in out
    assert "would certified_learners" not in out
    assert "would learner_completion_status" in out
    assert server.writes() == []


def _digest(metrics_dir: Path, name: str) -> str:
    [metric] = [m for m in load_metrics(metrics_dir) if m.name == name]
    return file_digest(metric)


def test_a_ui_edit_the_file_adopts_is_not_drift_when_the_file_changes_again(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir, names=("certified_learners",))
    server.edit_in_ui("certified_learners", "jane", description="Edited in the UI.")
    _edit_file(metrics_dir, "certified_learners", description="Edited in the UI.")
    capsys.readouterr()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    # Only the digest changed, in a version by the bot that holds jane's description.
    assert "no field changes; patched extension/metricFileSha256" in " ".join(capsys.readouterr().out.split())
    metric = server.metrics["certified_learners"]
    assert (metric["updatedBy"], metric["description"]) == (BOT, "Edited in the UI.")

    _edit_file(metrics_dir, "certified_learners", description="Changed again in a later PR.")
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert "drift" not in capsys.readouterr().out
    assert server.metrics["certified_learners"]["description"] == "Changed again in a later PR."


@pytest.mark.parametrize(
    ("extension", "patched"),
    [
        (None, "extension"),
        ({"tier": "gold"}, "extension/metricFileSha256"),
        ({FILE_DIGEST_PROPERTY: "0" * 64}, "extension/metricFileSha256"),
    ],
)
def test_a_missing_or_stale_digest_is_written_alone(
    server: FakeOpenMetadata,
    metrics_dir: Path,
    capsys: pytest.CaptureFixture[str],
    extension: dict[str, str] | None,
    patched: str,
) -> None:
    server.custom_properties.add("tier")
    _sync(metrics_dir, names=("certified_learners",))
    # As the bot left a metric synced before the digest, or for an older file.
    metric = server.metrics["certified_learners"]
    metric.pop("extension")
    if extension is not None:
        metric["extension"] = extension
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert f"no field changes; patched {patched}" in " ".join(capsys.readouterr().out.split())
    assert ("PUT", "/v1/metrics") not in server.calls
    digest = _digest(metrics_dir, "certified_learners")
    assert server.metrics["certified_learners"]["extension"] == {**(extension or {}), FILE_DIGEST_PROPERTY: digest}


def test_a_digest_that_matches_the_file_is_not_written_again(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir)
    assert {n: m["extension"] for n, m in server.metrics.items()} == {
        n: {FILE_DIGEST_PROPERTY: _digest(metrics_dir, n)} for n in server.metrics
    }
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir, dry_run=True) is False
    assert "then PATCH" not in capsys.readouterr().out
    assert _sync(metrics_dir) is False
    assert set(server.writes()) == {("PUT", "/v1/lineage")}


def test_an_undefined_digest_property_fails_each_metric_loudly(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    server.custom_properties.clear()
    assert _sync(metrics_dir) is True
    out = " ".join(capsys.readouterr().out.split())
    for name in ("certified_learners", "learner_completion_status"):
        assert (
            f"failed {name}: OpenMetadata has no `metricFileSha256` custom property on the metric type, where sync "
            "records the digest of each metric file. It must be defined there (ol-infrastructure om_server_config)"
        ) in out
    assert "Unknown custom field metricFileSha256" in out
    assert "synced" not in out
    # The server refuses the whole PATCH, so the status the PUT reset stays reset.
    assert all(m["entityStatus"] == "Unprocessed" and "extension" not in m for m in server.metrics.values())


def test_the_digest_covers_what_the_file_declares_in_any_key_order(metrics_dir: Path) -> None:
    [metric] = [m for m in load_metrics(metrics_dir) if m.name == "certified_learners"]
    reordered = dataclasses.replace(metric, body=dict(reversed(list(metric.body.items()))))
    assert file_digest(reordered) == file_digest(metric)
    for changed in (
        dataclasses.replace(metric, status="Draft"),
        dataclasses.replace(metric, implemented_by=metric.implemented_by[:1]),
        dataclasses.replace(metric, body={**metric.body, "displayName": "Learners certified"}),
    ):
        assert file_digest(changed) != file_digest(metric)


def test_ui_owners_naming_the_same_team_are_not_drift(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _edit_file(metrics_dir, "certified_learners", owners=TEAM)
    _sync(metrics_dir, names=("certified_learners",))
    # The same reference, in the shape the server returns it.
    server.edit_in_ui("certified_learners", "jane", owners=[{**TEAM_REF[0], "name": "data-platform"}])
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.writes() == [("PUT", "/v1/lineage")]


def test_ui_edit_to_a_field_sync_does_not_compare_is_not_drift(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", tags=[TIER1])
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.writes() == [("PUT", "/v1/lineage")]


def test_file_change_after_an_unrelated_ui_edit_syncs_without_force(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", tags=[TIER1])
    _edit_file(
        metrics_dir, "certified_learners", description="Learners with a certificate.", owners=TEAM, reviewers=TEAM
    )
    capsys.readouterr()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert "drift" not in capsys.readouterr().out
    metric = server.metrics["certified_learners"]
    assert metric["description"] == "Learners with a certificate."
    assert metric["owners"] == metric["reviewers"] == TEAM_REF
    assert metric["tags"] == [{**TIER1, "appliedBy": "jane"}]


@pytest.mark.parametrize("structural", [False, True])
def test_a_tag_sync_applied_is_removed_when_the_file_drops_it(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str], structural: bool
) -> None:
    _edit_file(metrics_dir, "certified_learners", tags=[COMPLETION_TERM])
    _sync(metrics_dir, names=("certified_learners",))
    assert server.metrics["certified_learners"]["tags"] == [{**COMPLETION_TERM, "appliedBy": BOT}]
    server.edit_in_ui("certified_learners", "jane", tags=[*server.metrics["certified_learners"]["tags"], TIER1])
    _edit_file(metrics_dir, "certified_learners", tags=[], **({"metricType": "SUM"} if structural else {}))
    capsys.readouterr()
    assert _sync(metrics_dir, names=("certified_learners",), dry_run=True) is False
    # After a PUT the PATCH also restores the status the PUT reset.
    patched = "tags, entityStatus, extension" if structural else "tags, extension"
    assert f"then PATCH {patched}" in " ".join(capsys.readouterr().out.split())

    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.metrics["certified_learners"]["tags"] == [{**TIER1, "appliedBy": "jane"}]


def test_a_tag_a_person_applied_stays_when_the_file_drops_it(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir, names=("certified_learners",))
    server.edit_in_ui("certified_learners", "jane", tags=[COMPLETION_TERM])
    # The file adopts the term, then drops it again.
    _edit_file(metrics_dir, "certified_learners", tags=[COMPLETION_TERM])
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    _edit_file(metrics_dir, "certified_learners", tags=[])
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.metrics["certified_learners"]["tags"] == [{**COMPLETION_TERM, "appliedBy": "jane"}]


def test_drift_is_the_file_against_live_when_the_bot_never_wrote_the_metric(
    server: FakeOpenMetadata, metrics_dir: Path
) -> None:
    _sync(metrics_dir)
    server.history["certified_learners"].clear()
    server.edit_in_ui("certified_learners", "jane", description="Created by hand.")
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    assert server.metrics["certified_learners"]["description"] == "Created by hand."


def test_reviewers_the_file_does_not_set_survive_a_put(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    server.metrics["certified_learners"]["reviewers"] = [{**TEAM_REF[0], "name": "data-platform"}]
    _edit_file(metrics_dir, "certified_learners", metricType="SUM")
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.metrics["certified_learners"]["reviewers"] == TEAM_REF


@pytest.mark.parametrize("structural", [False, True])
def test_an_empty_reviewers_list_clears_them(server: FakeOpenMetadata, metrics_dir: Path, structural: bool) -> None:
    _edit_file(metrics_dir, "certified_learners", reviewers=TEAM)
    _sync(metrics_dir, names=("certified_learners",))
    assert server.metrics["certified_learners"]["reviewers"] == TEAM_REF
    _edit_file(metrics_dir, "certified_learners", reviewers=[], **({"metricType": "SUM"} if structural else {}))
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert (("PUT", "/v1/metrics") in server.calls) is structural
    assert server.metrics["certified_learners"]["reviewers"] == []


def test_owners_reviewers_and_glossary_terms_resolve_by_name(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _edit_file(metrics_dir, "certified_learners", owners=TEAM, reviewers=TEAM, tags=[COMPLETION_TERM])
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    metric = server.metrics["certified_learners"]
    assert metric["owners"] == metric["reviewers"] == TEAM_REF
    assert metric["tags"] == [{**COMPLETION_TERM, "appliedBy": BOT}]


@pytest.mark.parametrize(
    "fields",
    [
        {"reviewers": [{"type": "team", "name": "nobody"}]},
        {"assets": [{"id": "table-1", "type": "table"}]},
        {"lifeCycle": {"created": {"timestamp": 1760000000000}}},
        {"domains": ["Learning"]},
        {"tags": [{"tagFQN": "Tier.Tier1", "source": "Tier", "labelType": "Manual", "state": "Confirmed"}]},
        {"tags": [{"tagFQN": "Learning.Missing", "source": "Glossary", "labelType": "Manual", "state": "Confirmed"}]},
    ],
)
def test_unresolved_name_skips_only_that_metric(
    server: FakeOpenMetadata, metrics_dir: Path, fields: dict[str, Any]
) -> None:
    _edit_file(metrics_dir, "certified_learners", **fields)
    assert _sync(metrics_dir) is True
    assert set(server.metrics) == {"learner_completion_status"}


@pytest.mark.parametrize("status", ["In Review", "Rejected", "Deprecated"])
def test_a_status_a_file_may_not_set_is_unresolved_with_no_writes(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str], status: str
) -> None:
    _sync(metrics_dir)
    path = metrics_dir / "certified_learners.yaml"
    path.write_text(path.read_text().replace("status: Approved", f"status: {status}"))
    metric_id = server.metrics["certified_learners"]["id"]
    server.calls.clear()
    server.lineage.clear()
    capsys.readouterr()
    assert _sync(metrics_dir) is True
    out = " ".join(capsys.readouterr().out.split())
    assert (
        f"unresolved certified_learners: certified_learners.yaml: `status` is '{status}'; a metric file sets only "
        "Draft or Approved" in out
    )
    assert ("GET", "/v1/metrics/name/certified_learners") not in server.calls
    assert all(e["edge"]["toEntity"]["id"] != metric_id for e in server.lineage)
    assert server.metrics["certified_learners"]["entityStatus"] == "Approved"
    assert "synced learner_completion_status" in out


def test_a_file_without_status_is_unresolved_with_no_writes(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", entityStatus="Draft")
    path = metrics_dir / "certified_learners.yaml"
    path.write_text(path.read_text().replace("status: Approved\n", ""))
    # A PUT resets the status, so a file that took it from OpenMetadata would lose it to a failed PATCH.
    _edit_file(metrics_dir, "certified_learners", metricType="SUM")
    metric_id = server.metrics["certified_learners"]["id"]
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir) is True
    out = " ".join(capsys.readouterr().out.split())
    assert (
        "unresolved certified_learners: certified_learners.yaml: `status` is missing; a metric file sets Draft or "
        "Approved" in out
    )
    assert ("PUT", "/v1/metrics") not in server.writes()
    assert ("PATCH", f"/v1/metrics/{metric_id}") not in server.writes()
    assert server.metrics["certified_learners"]["entityStatus"] == "Draft"
    assert "synced learner_completion_status" in out


def test_a_failed_patch_after_a_put_is_repaired_by_the_next_sync(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir, names=("certified_learners",))
    _edit_file(metrics_dir, "certified_learners", metricType="SUM", description="Learners with a certificate.")
    request = server.request

    def failing_patch(method: str, path: str, *args: Any, **kwargs: Any) -> Any:
        if method == "PATCH":
            raise OpenMetadataError(f"{method} {path} -> 503: unavailable", 503)
        return request(method, path, *args, **kwargs)

    server.request = failing_patch  # type: ignore[method-assign]
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    del server.request
    metric = server.metrics["certified_learners"]
    assert (metric["metricType"], metric["entityStatus"]) == ("SUM", "Unprocessed")

    assert _sync(metrics_dir, names=("certified_learners",)) is False
    metric = server.metrics["certified_learners"]
    assert (metric["entityStatus"], metric["description"]) == ("Approved", "Learners with a certificate.")


def test_a_failed_read_skips_only_that_metric(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    server.broken.add("certified_learners")
    assert _sync(metrics_dir) is True
    assert set(server.metrics) == {"learner_completion_status"}


@pytest.mark.parametrize("file_related", [None, []])
def test_a_put_keeps_assets_and_the_related_metrics_the_file_does_not_set(
    server: FakeOpenMetadata, metrics_dir: Path, file_related: list[str] | None
) -> None:
    _sync(metrics_dir)
    metric = server.metrics["certified_learners"]
    other = server.metrics["learner_completion_status"]
    metric["assets"] = [{"id": "dashboard-1", "type": "dashboard", "name": "Contract health"}]
    metric["relatedMetrics"] = [{"id": other["id"], "type": "metric", "fullyQualifiedName": other["name"]}]
    fields: dict[str, Any] = {"metricType": "SUM"}
    if file_related is not None:
        fields["relatedMetrics"] = file_related
    _edit_file(metrics_dir, "certified_learners", **fields)
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    metric = server.metrics["certified_learners"]
    assert metric["metricType"] == "SUM"
    assert metric["assets"] == [{"id": "dashboard-1", "type": "dashboard"}]
    # An explicit empty list in the file clears them.
    assert [r["fullyQualifiedName"] for r in metric["relatedMetrics"]] == (
        [] if file_related == [] else ["learner_completion_status"]
    )


def test_version_mismatch_stops_before_the_first_file(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    server.version = "9.9.9"
    assert _sync(metrics_dir, dry_run=True) is True
    assert server.calls == [("GET", "/v1/system/version")]


def test_structural_changes_ignore_server_keys_and_extra_tags() -> None:
    body = {
        "name": "m",
        "metricType": "COUNT",
        "measures": [{"name": "a", "expression": "count(*)"}],
        "tags": [{"tagFQN": "G.t", "source": "Glossary"}],
    }
    live = {
        "metricType": "COUNT",
        "measures": [{"name": "a", "expression": "count(*)", "fullyQualifiedName": "m.measure.a"}],
        "tags": [{"tagFQN": "G.t", "source": "Glossary", "labelType": "Manual"}, {"tagFQN": "Tier.Tier1"}],
    }
    assert structural_changes(body, live) == []
    # A field the file dropped is cleared by the PUT, so it counts.
    assert structural_changes(body, {**live, "granularity": "DAY"}) == ["granularity"]
    assert structural_changes({**body, "metricType": "SUM"}, live) == ["metricType"]
    # A property the file dropped from a measure stays on the server unless the PUT replaces it.
    assert structural_changes({**body, "measures": [{"name": "a"}]}, live) == ["measures"]
    # A bot PUT cannot change a stored displayName, so only a missing one counts.
    assert structural_changes({**body, "displayName": "M"}, {**live, "displayName": "N"}) == []
    assert structural_changes(body, {**live, "displayName": "N"}) == []
    assert structural_changes({**body, "displayName": "M"}, live) == ["displayName"]


def test_display_name_conflicts_only_with_a_stored_name_that_differs() -> None:
    assert display_name_conflict({"displayName": "M"}, None) is None
    assert display_name_conflict({"displayName": "M"}, {"displayName": ""}) is None
    assert display_name_conflict({"displayName": "M"}, {"displayName": "M"}) is None
    assert display_name_conflict({"displayName": "M"}, {"displayName": "N"}) == "N"
    assert display_name_conflict({}, {"displayName": "N"}) == "N"


def test_descriptions_compare_after_the_server_encoding_is_undone() -> None:
    assert same_description(_sanitize('a "b" & c\'s @d'), 'a "b" & c\'s @d')
    assert same_description(_sanitize("a &amp; b"), "a &amp; b")
    assert not same_description(_sanitize("a b"), "a c")


def test_binding_missing_from_the_manifest_is_unresolved(metrics_dir: Path) -> None:
    [certified, _] = load_metrics(metrics_dir)
    with pytest.raises(ValueError, match="not in the manifest"):
        lineage_sources(certified, ManifestRegistry(), SERVICE)


class _Response(io.BytesIO):
    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def test_client_sends_json_patch_maps_404_to_none_and_reads_an_empty_body(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[Any] = []

    def fake_open(req: Any, timeout: int) -> _Response:
        sent.append(req)
        if req.full_url.endswith("/missing"):
            raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {}, io.BytesIO(b"{}"))  # type: ignore[arg-type]
        if req.full_url.endswith("/broken"):
            raise urllib.error.HTTPError(req.full_url, 500, "Error", {}, io.BytesIO(b"boom"))  # type: ignore[arg-type]
        if req.full_url.endswith("/lineage"):
            return _Response(b"")
        return _Response(b'{"ok": true}')

    monkeypatch.setattr(openmetadata._OPENER, "open", fake_open)
    client = OpenMetadataClient("https://om.example/api/", "token")
    assert client.get_or_none("/v1/metrics/name/missing") is None
    with pytest.raises(OpenMetadataError, match="500: boom") as exc:
        client.get_or_none("/v1/metrics/name/broken")
    assert exc.value.status == 500
    ops = [{"op": "add", "path": "/description", "value": "d"}]
    assert client.request("PATCH", "/v1/metrics/1", ops, content_type=openmetadata.JSON_PATCH_CONTENT_TYPE)
    assert sent[-1].full_url == "https://om.example/api/v1/metrics/1"
    assert sent[-1].get_header("Content-type") == "application/json-patch+json"
    assert sent[-1].get_header("Authorization") == "Bearer token"
    assert json.loads(sent[-1].data) == ops
    assert client.request("PUT", "/v1/lineage", {"edge": {}}) is None

"""Tests for `ol-dbt metrics sync` against an in-memory stand-in for the OpenMetadata API.

The stand-in replaces only ``OpenMetadataClient.request``, so name lookups,
404 handling and the command's ordering run as they would against a server.
Its PUT and PATCH follow what the OpenMetadata 2.0.5 source does for a bot
(see ``ol_dbt_cli.lib.metric_sync``); that behaviour is not checked against a
live server here. It does not consolidate a user's edits within a session the
way the server does, which sync does not depend on: it reads only the newest
version the bot wrote.
"""

from __future__ import annotations

import copy
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
from ol_dbt_cli.lib.metric_sync import lineage_sources, same_description, structural_changes
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
TEAM = [{"type": "team", "name": "data-platform"}]
TEAM_REF = [{"id": "team-1", "type": "team"}]


def _sanitize(description: str | None) -> str | None:
    """Store a plain-text description the way DescriptionSanitizer.sanitize does: decode, then re-encode."""
    if description is None:
        return None
    return "".join(_SANITIZER_ENTITIES.get(c, c) for c in html.unescape(description))


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
        self.team_members = {"team-1": {"jane"}}
        self.glossary_terms = {"Learning.Course completion"}
        self.tables = {PROGRESS_FQN: "table-1"}
        self.metrics: dict[str, dict[str, Any]] = {}
        # Every version of each metric, oldest first.
        self.history: dict[str, list[dict[str, Any]]] = {}
        self.lineage: list[dict[str, Any]] = []
        self.calls: list[tuple[str, str]] = []
        # Metrics whose GET answers 500.
        self.broken: set[str] = set()

    def writes(self) -> list[tuple[str, str]]:
        return [c for c in self.calls if c[0] != "GET"]

    def edit_in_ui(self, name: str, user: str, **fields: Any) -> None:
        """Apply a person's PATCH, which the server sanitizes like any other."""
        metric = self.metrics[name]
        if "description" in fields:
            fields["description"] = _sanitize(fields["description"])
        metric.update(fields, updatedBy=user, version=round(metric["version"] + 0.1, 1))
        self._record(metric)

    def _record(self, metric: dict[str, Any]) -> None:
        self.history.setdefault(metric["name"], []).append(copy.deepcopy(metric))

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
                [metric] = [m for m in self.metrics.values() if m["id"] == metric_id]
                self._check_status_change(metric, body)
                for op in body:
                    assert op["op"] == "add"
                    field = op["path"].lstrip("/")
                    metric[field] = _sanitize(op["value"]) if field == "description" else op["value"]
                metric.update(updatedBy=BOT, version=round(metric["version"] + 0.1, 1))
                self._record(metric)
                return copy.deepcopy(metric)
            case ("PUT", "lineage"):
                self.lineage.append(body)
                # LineageResource.addLineage answers 200 with no body.
                return None
        raise OpenMetadataError(f"{method} {path} -> 404: not found", 404)

    def _check_status_change(self, metric: dict[str, Any], ops: list[dict[str, Any]]) -> None:
        """Refuse the bot's move out of In Review to Approved or Rejected unless it is a reviewer.

        EntityUpdater.updateEntityStatus and checkUpdatedByReviewer; the server
        answers 403 (AuthorizationException) and stores none of the PATCH.
        """
        status = next((op["value"] for op in ops if op["path"] == "/entityStatus"), None)
        if metric["entityStatus"] != "In Review" or status not in ("Approved", "Rejected"):
            return
        reviewers = metric.get("reviewers") or []
        if reviewers and not any(
            BOT in self.team_members.get(r["id"], set()) if r["type"] == "team" else r.get("name") == BOT
            for r in reviewers
        ):
            message = json.dumps({"code": 403, "message": f"User '{BOT}' is not a reviewer"})
            raise OpenMetadataError(f"PATCH /v1/metrics/{metric['id']} -> 403: {message}", 403)

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
        if live.get("owners"):
            updated["owners"] = live["owners"]
        updated["reviewers"] = body.get("reviewers") or []
        updated["tags"] = live.get("tags", []) + [t for t in body.get("tags", []) if t not in live.get("tags", [])]
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
    assert "then PATCH entityStatus" in out
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


IN_REVIEW_MESSAGE = "certified_learners: In Review in OpenMetadata, and sync does not override a review in progress"


@pytest.mark.parametrize("structural", [False, True])
@pytest.mark.parametrize("force", [False, True])
def test_a_metric_in_review_is_left_alone_and_fails_while_the_others_sync(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str], force: bool, structural: bool
) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", reviewers=TEAM_REF, entityStatus="In Review")
    if structural:
        # A PUT alone would reset In Review to Unprocessed.
        _edit_file(metrics_dir, "certified_learners", metricType="SUM")
    metric_id = server.metrics["certified_learners"]["id"]
    version = server.metrics["certified_learners"]["version"]
    server.calls.clear()
    server.lineage.clear()
    capsys.readouterr()

    assert _sync(metrics_dir, force=force) is True
    # Rich wraps lines at the console width.
    out = " ".join(capsys.readouterr().out.split())
    assert IN_REVIEW_MESSAGE in out
    assert "drift" not in out
    assert "overriding" not in out
    assert ("PUT", "/v1/metrics") not in server.writes()
    assert ("PATCH", f"/v1/metrics/{metric_id}") not in server.writes()
    assert all(e["edge"]["toEntity"]["id"] != metric_id for e in server.lineage)
    metric = server.metrics["certified_learners"]
    assert (metric["entityStatus"], metric["version"]) == ("In Review", version)
    # The other metric still synced, lineage included.
    assert "synced learner_completion_status" in out
    assert [e["edge"]["toEntity"]["id"] for e in server.lineage] == [server.metrics["learner_completion_status"]["id"]]


def test_dry_run_reports_a_metric_in_review(
    server: FakeOpenMetadata, metrics_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", entityStatus="In Review")
    server.calls.clear()
    capsys.readouterr()
    assert _sync(metrics_dir, dry_run=True, force=True) is True
    out = " ".join(capsys.readouterr().out.split())
    assert IN_REVIEW_MESSAGE in out
    assert "would certified_learners" not in out
    assert "would learner_completion_status" in out
    assert server.writes() == []


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
    assert metric["tags"] == [TIER1]


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
    term = {"tagFQN": "Learning.Course completion", "source": "Glossary", "labelType": "Manual", "state": "Confirmed"}
    _edit_file(metrics_dir, "certified_learners", owners=TEAM, reviewers=TEAM, tags=[term])
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    metric = server.metrics["certified_learners"]
    assert metric["owners"] == metric["reviewers"] == TEAM_REF
    assert metric["tags"] == [term]


@pytest.mark.parametrize(
    "fields",
    [
        {"reviewers": [{"type": "team", "name": "nobody"}]},
        {"assets": [{"id": "table-1", "type": "table"}]},
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

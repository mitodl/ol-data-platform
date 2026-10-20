"""Tests for `ol-dbt metrics sync` against an in-memory stand-in for the OpenMetadata API.

The stand-in replaces only ``OpenMetadataClient.request``, so name lookups,
404 handling and the command's ordering run as they would against a server.
Its PUT and PATCH follow what the OpenMetadata 2.0.5 source does for a bot
(see ``ol_dbt_cli.lib.metric_sync``); that behaviour is not checked against a
live server here.
"""

from __future__ import annotations

import copy
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
from ol_dbt_cli.lib.metric_sync import lineage_sources, structural_changes
from ol_dbt_cli.lib.openmetadata import OpenMetadataClient, OpenMetadataError

FIXTURES = Path(__file__).parent / "fixtures" / "metrics"
SERVICE = "Starburst Galaxy"
BOT = "metrics-sync-bot"
PROGRESS = "afact_learner_courserun_progress"
PROGRESS_FQN = f"{SERVICE}.ol_data_lake_production.ol_warehouse_production_dimensional.{PROGRESS}"
# Every other model the fixtures bind is a b2b StarRocks view.
STARROCKS_VIEWS = sorted({i.binding.target for m in load_metrics(FIXTURES) for i in m.implemented_by} - {PROGRESS})
SNAPSHOT_VERSION = json.loads(SCHEMA_SNAPSHOT.read_text())[SCHEMA_VERSION_KEY]


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
        self.lineage: list[dict[str, Any]] = []
        self.calls: list[tuple[str, str]] = []

    def writes(self) -> list[tuple[str, str]]:
        return [c for c in self.calls if c[0] != "GET"]

    def edit_in_ui(self, name: str, user: str, **fields: Any) -> None:
        self.metrics[name].update(fields, updatedBy=user)

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
            case ("GET", "metrics", "name", name) if name in self.metrics:
                return copy.deepcopy(self.metrics[name])
            case ("PUT", "metrics"):
                return self._put_metric(body)
            case ("PATCH", "metrics", metric_id):
                assert content_type == openmetadata.JSON_PATCH_CONTENT_TYPE
                [metric] = [m for m in self.metrics.values() if m["id"] == metric_id]
                for op in body:
                    assert op["op"] == "add"
                    metric[op["path"].lstrip("/")] = op["value"]
                metric.update(updatedBy=BOT, version=round(metric["version"] + 0.1, 1))
                return copy.deepcopy(metric)
            case ("PUT", "lineage"):
                self.lineage.append(body)
                return {}
        raise OpenMetadataError(f"{method} {path} -> 404: not found", 404)

    def _put_metric(self, body: dict[str, Any]) -> dict[str, Any]:
        live = self.metrics.get(body["name"])
        if live is None:
            created = {**copy.deepcopy(body), "id": f"metric-{len(self.metrics) + 1}", "version": 0.1}
            created.update(fullyQualifiedName=body["name"], entityStatus="Unprocessed", updatedBy=BOT)
            self.metrics[body["name"]] = created
            return copy.deepcopy(created)
        updated = {**copy.deepcopy(body), "id": live["id"], "fullyQualifiedName": live["fullyQualifiedName"]}
        if live.get("description"):
            updated["description"] = live["description"]
        if live.get("owners"):
            updated["owners"] = live["owners"]
        updated["reviewers"] = body.get("reviewers") or []
        updated["tags"] = live.get("tags", []) + [t for t in body.get("tags", []) if t not in live.get("tags", [])]
        updated.update(entityStatus="Unprocessed", updatedBy=BOT, version=round(live["version"] + 0.1, 1))
        self.metrics[body["name"]] = updated
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


def test_ui_edit_to_status_is_drift_until_forced(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    server.edit_in_ui("certified_learners", "jane", entityStatus="Deprecated")
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is True
    assert server.writes() == []
    assert server.metrics["certified_learners"]["entityStatus"] == "Deprecated"

    assert _sync(metrics_dir, names=("certified_learners",), force=True) is False
    assert server.metrics["certified_learners"]["entityStatus"] == "Approved"


def test_ui_edit_to_a_field_sync_does_not_compare_is_not_drift(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    tier = {"tagFQN": "Tier.Tier1", "source": "Classification", "labelType": "Manual", "state": "Confirmed"}
    server.edit_in_ui("certified_learners", "jane", tags=[tier])
    server.calls.clear()
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.writes() == [("PUT", "/v1/lineage")]


def test_reviewers_the_file_does_not_set_survive_a_put(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    _sync(metrics_dir)
    reviewers = [{"id": "team-1", "type": "team", "name": "data-platform"}]
    server.metrics["certified_learners"]["reviewers"] = reviewers
    _edit_file(metrics_dir, "certified_learners", metricType="SUM")
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    assert server.metrics["certified_learners"]["reviewers"] == [{"id": "team-1", "type": "team"}]


def test_owners_reviewers_and_glossary_terms_resolve_by_name(server: FakeOpenMetadata, metrics_dir: Path) -> None:
    team = [{"type": "team", "name": "data-platform"}]
    term = {"tagFQN": "Learning.Course completion", "source": "Glossary", "labelType": "Manual", "state": "Confirmed"}
    _edit_file(metrics_dir, "certified_learners", owners=team, reviewers=team, tags=[term])
    assert _sync(metrics_dir, names=("certified_learners",)) is False
    metric = server.metrics["certified_learners"]
    assert metric["owners"] == metric["reviewers"] == [{"id": "team-1", "type": "team"}]
    assert metric["tags"] == [term]


@pytest.mark.parametrize(
    "fields",
    [
        {"reviewers": [{"type": "team", "name": "nobody"}]},
        {"tags": [{"tagFQN": "Learning.Missing", "source": "Glossary", "labelType": "Manual", "state": "Confirmed"}]},
    ],
)
def test_unresolved_name_skips_only_that_metric(
    server: FakeOpenMetadata, metrics_dir: Path, fields: dict[str, Any]
) -> None:
    _edit_file(metrics_dir, "certified_learners", **fields)
    assert _sync(metrics_dir) is True
    assert set(server.metrics) == {"learner_completion_status"}


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


def test_binding_missing_from_the_manifest_is_unresolved(metrics_dir: Path) -> None:
    [certified, _] = load_metrics(metrics_dir)
    with pytest.raises(ValueError, match="not in the manifest"):
        lineage_sources(certified, ManifestRegistry(), SERVICE)


class _Response(io.BytesIO):
    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def test_client_sends_json_patch_and_maps_404_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[Any] = []

    def fake_open(req: Any, timeout: int) -> _Response:
        sent.append(req)
        if req.full_url.endswith("/missing"):
            raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {}, io.BytesIO(b"{}"))  # type: ignore[arg-type]
        if req.full_url.endswith("/broken"):
            raise urllib.error.HTTPError(req.full_url, 500, "Error", {}, io.BytesIO(b"boom"))  # type: ignore[arg-type]
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

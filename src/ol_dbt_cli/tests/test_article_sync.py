"""Tests for `ol-dbt articles sync` and the article files it reads.

As in ``test_glossary_sync``, the stand-in replaces only
``OpenMetadataClient.request``. It stores a page's description with a marker
prepended, standing in for OpenMetadata's HTML sanitizer, so a test that sees
an unchanged article skipped shows sync did not compare the description text.
It accepts no PUT, since OpenMetadata keeps a stored description against a
bot's PUT and the sync must not rely on one.
"""

from __future__ import annotations

import copy
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote

import pytest
import yaml

from ol_dbt_cli.commands import articles as articles_command
from ol_dbt_cli.lib.article_registry import DEFAULT_ARTICLES_DIR, load_articles
from ol_dbt_cli.lib.article_sync import REPO_BLOB_URL, REPO_RAW_URL, related_entities, render_body
from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry
from ol_dbt_cli.lib.openmetadata import JSON_PATCH_CONTENT_TYPE, OpenMetadataClient, OpenMetadataError

REPO_ROOT = Path(__file__).parents[3]
SERVICE = "Starburst Galaxy"
STARROCKS_SERVICE = "StarRocks"
TEAM_ID = "team-1"
LAKE_MODELS = ("integrations__learn__ocw_courses", "integrations__learn__xpro_courses", "dim_user")
SANITIZED = "<!-- sanitized -->"


def _model(name: str, **fields: Any) -> ManifestModel:
    return ManifestModel(
        unique_id=f"model.open_learning.{name}",
        name=name,
        resource_type="model",
        original_file_path="",
        schema="ol_warehouse_production_integrations",
        database="ol_data_lake_production",
        **fields,
    )


def _manifest() -> ManifestRegistry:
    """Build a production manifest: lake models on Trino, one StarRocks view, one model disabled outright."""
    registry = ManifestRegistry()
    for name in LAKE_MODELS:
        registry.by_name[name] = _model(name)
    registry.disabled["integrations__learn__b2b_view"] = _model(
        "integrations__learn__b2b_view", tags=["starrocks"], alias="integrations__learn__b2b_view", custom_schema="b2b"
    )
    registry.disabled["integrations__learn__retired"] = _model("integrations__learn__retired")
    return registry


def _fqn(name: str) -> str:
    return f"{SERVICE}.ol_data_lake_production.ol_warehouse_production_integrations.{name}"


class FakeOpenMetadata(OpenMetadataClient):
    """Holds tables by FQN and pages by name."""

    def __init__(self, tables: list[str]) -> None:
        super().__init__("https://om.example/api", "token")
        self.tables = {fqn: f"table-{index}" for index, fqn in enumerate(tables)}
        self.pages: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str, Any]] = []

    def writes(self) -> list[tuple[str, str, Any]]:
        return [c for c in self.calls if c[0] != "GET"]

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        query: dict[str, str] | None = None,
        content_type: str = "application/json",
    ) -> Any:
        self.calls.append((method, path, copy.deepcopy(body)))
        match (method, *path.strip("/").split("/")[1:]):
            case ("GET", "teams", "name", "data-platform"):
                return {"id": TEAM_ID}
            case ("GET", "tables", "name", fqn) if unquote(fqn) in self.tables:
                return {"id": self.tables[unquote(fqn)]}
            case ("GET", "contextCenter", "pages", "name", name) if unquote(name) in self.pages:
                return copy.deepcopy(self.pages[unquote(name)])
            case ("POST", "contextCenter", "pages"):
                assert body["name"] not in self.pages
                assert body["pageType"] == "Article"
                assert body["page"] == {}
                assert body["relatedEntities"], "an empty list attaches the page to Organization"
                self.pages[body["name"]] = {
                    **copy.deepcopy(body),
                    "id": f"page-{len(self.pages)}",
                    "description": SANITIZED + body["description"],
                }
                return copy.deepcopy(self.pages[body["name"]])
            case ("PATCH", "contextCenter", "pages", page_id):
                assert content_type == JSON_PATCH_CONTENT_TYPE
                [page] = [p for p in self.pages.values() if p["id"] == page_id]
                for op in body:
                    assert op["op"] == "add"
                    field = op["path"].lstrip("/")
                    assert field in ("description", "relatedEntities")
                    value = op["value"]
                    page[field] = SANITIZED + value if field == "description" else copy.deepcopy(value)
                return copy.deepcopy(page)
        raise OpenMetadataError(f"{method} {path} -> 404: not found", 404)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "contract.md").write_text(
        "# Contract\n\nModels are named `integrations__learn__<source>_<entity>`.\n\n"
        "See [the design](design/pull.md#scope) and [MIT Learn](https://learn.mit.edu).\n\n"
        "![Flow](img/flow.png)\n"
    )
    (tmp_path / DEFAULT_ARTICLES_DIR).mkdir()
    _write(tmp_path, "contract", related=[{"type": "table", "dbt_model": "integrations__learn__*"}])
    return tmp_path


def _write(repo: Path, name: str, **fields: Any) -> None:
    raw = {
        "name": name,
        "displayName": name.title(),
        "source": "docs/contract.md",
        "owners": [{"type": "team", "name": "data-platform"}],
        **fields,
    }
    (repo / DEFAULT_ARTICLES_DIR / f"{name}.yaml").write_text(yaml.safe_dump(raw, sort_keys=False))


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch, repo: Path) -> FakeOpenMetadata:
    fake = FakeOpenMetadata([_fqn(name) for name in LAKE_MODELS])
    monkeypatch.setattr(OpenMetadataClient, "from_env", classmethod(lambda cls: fake))
    monkeypatch.setattr(articles_command, "get_repo_root", lambda: repo)
    monkeypatch.setattr(articles_command, "load_catalog_manifest", lambda dbt_dir, path: _manifest())
    return fake


def _sync(**kwargs: Any) -> bool:
    """Run sync; return whether it exited non-zero."""
    try:
        articles_command.sync(service=SERVICE, starrocks_service=STARROCKS_SERVICE, **kwargs)
    except SystemExit as exc:
        return exc.code != 0
    return False


def test_the_repository_articles_load() -> None:
    articles = load_articles(REPO_ROOT / DEFAULT_ARTICLES_DIR, REPO_ROOT)
    assert {a.spec.name for a in articles} >= {"learn-marts-contract", "irx-simeon-mapping"}


@pytest.mark.parametrize(
    ("fields", "error"),
    [
        ({"source": "/etc/passwd.md"}, "repository-relative .md path"),
        ({"source": "../outside.md"}, "repository-relative .md path"),
        ({"source": "docs/missing.md"}, "does not exist"),
        ({"displayName": ""}, "displayName"),
        ({"related": []}, "related"),
        ({"related": [{"type": "view", "dbt_model": "a"}]}, "not one of"),
        ({"related": [{"type": "table"}]}, "exactly one of"),
        ({"related": [{"type": "table", "dbt_model": "a", "fqn": "b"}]}, "exactly one of"),
    ],
)
def test_a_bad_article_file_is_refused(repo: Path, fields: dict[str, Any], error: str) -> None:
    _write(repo, "bad", **{"related": [{"type": "table", "dbt_model": "x"}], **fields})
    with pytest.raises(ValueError, match=error):
        load_articles(repo / DEFAULT_ARTICLES_DIR, repo)


def test_an_article_name_cannot_hold_a_dot(repo: Path) -> None:
    raw = yaml.safe_load((repo / DEFAULT_ARTICLES_DIR / "contract.yaml").read_text())
    (repo / DEFAULT_ARTICLES_DIR / "contract.yaml").write_text(yaml.safe_dump({**raw, "name": "learn.contract"}))
    with pytest.raises(ValueError, match="String should match pattern"):
        load_articles(repo / DEFAULT_ARTICLES_DIR, repo)


def test_two_files_cannot_share_an_article_name(repo: Path) -> None:
    raw = yaml.safe_load((repo / DEFAULT_ARTICLES_DIR / "contract.yaml").read_text())
    (repo / DEFAULT_ARTICLES_DIR / "copy.yml").write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="more than one file: contract"):
        load_articles(repo / DEFAULT_ARTICLES_DIR, repo)


def test_render_keeps_angle_brackets_as_text_and_points_relative_links_at_github(repo: Path) -> None:
    html = render_body((repo / "docs" / "contract.md").read_text(), PurePosixPath("docs/contract.md"))
    assert "<code>integrations__learn__&lt;source&gt;_&lt;entity&gt;</code>" in html
    assert f'href="{REPO_BLOB_URL}/docs/design/pull.md#scope"' in html
    assert 'href="https://learn.mit.edu"' in html
    assert f'src="{REPO_RAW_URL}/docs/img/flow.png"' in html
    assert html.endswith("Edits made here are overwritten when that file changes.</em></p>")
    assert f'<a href="{REPO_BLOB_URL}/docs/contract.md">docs/contract.md</a>' in html


def test_related_patterns_expand_to_lake_models_and_starrocks_views_only(repo: Path) -> None:
    [article] = load_articles(repo / DEFAULT_ARTICLES_DIR, repo)
    sources = related_entities(article, _manifest(), SERVICE, STARROCKS_SERVICE)
    assert [(s.fqn, s.starrocks) for s in sources] == [
        ("StarRocks.default_catalog.b2b.integrations__learn__b2b_view", True),
        (_fqn("integrations__learn__ocw_courses"), False),
        (_fqn("integrations__learn__xpro_courses"), False),
    ]


def test_a_pattern_that_matches_nothing_fails_the_article(repo: Path) -> None:
    _write(repo, "contract", related=[{"type": "table", "dbt_model": "nothing__*"}])
    [article] = load_articles(repo / DEFAULT_ARTICLES_DIR, repo)
    with pytest.raises(ValueError, match="matches no model"):
        related_entities(article, _manifest(), SERVICE, STARROCKS_SERVICE)


def test_first_sync_creates_the_article_attached_to_the_catalogued_tables(server: FakeOpenMetadata) -> None:
    assert _sync() is False
    [(method, path, body)] = server.writes()
    assert (method, path) == ("POST", "/v1/contextCenter/pages")
    assert body["name"] == "contract"
    assert body["displayName"] == "Contract"
    assert body["owners"] == [{"id": TEAM_ID, "type": "team"}]
    # The StarRocks view is not catalogued yet, so it is skipped rather than failing the article.
    assert body["relatedEntities"] == [
        {"id": server.tables[_fqn("integrations__learn__ocw_courses")], "type": "table"},
        {"id": server.tables[_fqn("integrations__learn__xpro_courses")], "type": "table"},
    ]
    assert "entityStatus" not in body


def test_an_unchanged_article_is_not_rewritten(server: FakeOpenMetadata) -> None:
    _sync()
    server.calls.clear()
    assert _sync() is False
    assert server.writes() == []


def test_a_changed_source_rewrites_the_article(server: FakeOpenMetadata, repo: Path) -> None:
    _sync()
    (repo / "docs" / "contract.md").write_text("# Contract\n\nRevised.\n")
    server.calls.clear()
    assert _sync() is False
    [(method, path, ops)] = server.writes()
    assert method == "PATCH"
    assert path == f"/v1/contextCenter/pages/{server.pages['contract']['id']}"
    assert [op["path"] for op in ops] == ["/description"]
    assert "Revised." in server.pages["contract"]["description"]
    server.calls.clear()
    _sync()
    assert server.writes() == []


def test_a_newly_catalogued_table_is_attached_on_the_next_sync(server: FakeOpenMetadata) -> None:
    _sync()
    server.tables["StarRocks.default_catalog.b2b.integrations__learn__b2b_view"] = "view-1"
    server.calls.clear()
    _sync()
    [(_, _, ops)] = server.writes()
    assert [op["path"] for op in ops] == ["/relatedEntities"]
    assert {"id": "view-1", "type": "table"} in server.pages["contract"]["relatedEntities"]


def test_dry_run_writes_nothing(server: FakeOpenMetadata) -> None:
    assert _sync(dry_run=True) is False
    assert server.writes() == []


def test_one_failing_article_does_not_stop_the_others(server: FakeOpenMetadata, repo: Path) -> None:
    _write(repo, "broken", related=[{"type": "table", "dbt_model": "nothing__*"}])
    assert _sync() is True
    assert [body["name"] for _, _, body in server.writes()] == ["contract"]


def test_an_article_with_no_catalogued_entity_is_not_written(server: FakeOpenMetadata) -> None:
    server.tables.clear()
    assert _sync() is False
    assert server.writes() == []
    assert server.pages == {}


def test_sources_resolve_against_the_repository_of_the_articles_dir(server: FakeOpenMetadata, tmp_path: Path) -> None:
    # The pipeline reads articles/ from a different checkout than the one it runs in.
    other = tmp_path / "other"
    (other / "docs").mkdir(parents=True)
    (other / "docs" / "contract.md").write_text("# From the other checkout\n")
    (other / DEFAULT_ARTICLES_DIR).mkdir()
    _write(other, "contract", related=[{"type": "table", "dbt_model": "integrations__learn__*"}])
    assert _sync(articles_dir_path=str(other / DEFAULT_ARTICLES_DIR)) is False
    assert "From the other checkout" in server.pages["contract"]["description"]

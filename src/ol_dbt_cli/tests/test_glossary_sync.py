"""Tests for `ol-dbt glossary sync` and the glossary files it reads.

Like ``test_metric_sync``, the stand-in replaces only
``OpenMetadataClient.request``, so name lookups, 404 handling and the
command's ordering run as they would against a server. It stores glossaries
and terms the way the requests in ``ol_dbt_cli.lib.glossary_sync`` expect, not
OpenMetadata's approval workflow: it never changes a term's entityStatus, so a
test that finds one changed caught sync writing it.
"""

from __future__ import annotations

import copy
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pytest
import yaml

from ol_dbt_cli.commands import glossary as glossary_command
from ol_dbt_cli.lib import openmetadata
from ol_dbt_cli.lib.glossary_registry import load_glossaries
from ol_dbt_cli.lib.openmetadata import OpenMetadataClient, OpenMetadataError

REPO_GLOSSARY = Path(__file__).parents[3] / "glossary"
TEAM_ID = "team-1"
RELATED = "relatedTerms"
FQN = "fullyQualifiedName"


class FakeOpenMetadata(OpenMetadataClient):
    """Holds glossaries and terms in memory, keyed by FQN."""

    def __init__(self) -> None:
        super().__init__("https://om.example/api", "token")
        self.teams = {"data-platform": TEAM_ID}
        self.glossaries: dict[str, dict[str, Any]] = {}
        self.terms: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str, Any]] = []

    def writes(self) -> list[tuple[str, str, Any]]:
        return [c for c in self.calls if c[0] != "GET"]

    def _by_id(self, entity_id: str) -> dict[str, Any]:
        [entity] = [e for e in (*self.glossaries.values(), *self.terms.values()) if e["id"] == entity_id]
        return entity

    def _link(self, term: dict[str, Any], other: dict[str, Any]) -> None:
        for a, b in ((term, other), (other, term)):
            if all(link["term"]["id"] != b["id"] for link in a["relatedTerms"]):
                ref = {"id": b["id"], "type": "glossaryTerm", "fullyQualifiedName": b["fullyQualifiedName"]}
                a["relatedTerms"].append({"relationType": "relatedTo", "term": ref})

    def _get(self, store: dict[str, dict[str, Any]], fqn: str, query: dict[str, str] | None) -> Any:
        entity = store.get(unquote(fqn))
        if entity is None or (entity.get("deleted") and (query or {}).get("include") != "all"):
            raise OpenMetadataError(f"GET {fqn} -> 404: not found", 404)
        found = copy.deepcopy(entity)
        if store is self.terms and RELATED not in (query or {}).get("fields", ""):
            found.pop("relatedTerms")
        return found

    def _create_term(self, body: dict[str, Any]) -> dict[str, Any]:
        parent = body.get("parent", body["glossary"])
        if parent not in (self.terms if "parent" in body else self.glossaries):
            raise OpenMetadataError(f"POST /v1/glossaryTerms -> 404: {parent} not found", 404)
        if any(
            t["name"].lower() == body["name"].lower() and t["fullyQualifiedName"].startswith(f"{body['glossary']}.")
            for t in self.terms.values()
        ):
            raise OpenMetadataError(
                f"POST /v1/glossaryTerms -> 400: Entity with name {body['name']} already exists", 400
            )
        fqn = f"{parent}.{body['name']}"
        term = {
            **{k: copy.deepcopy(v) for k, v in body.items() if k not in ("glossary", "parent", "relatedTerms")},
            "id": f"term-{len(self.terms) + 1}",
            "fullyQualifiedName": fqn,
            "entityStatus": "In Review",
            "relatedTerms": [],
            "deleted": False,
        }
        self.terms[fqn] = term
        for related in body.get("relatedTerms", []):
            self._link(term, self.terms[related])
        return term

    def _patch(self, entity: dict[str, Any], ops: list[dict[str, Any]]) -> dict[str, Any]:
        for op in ops:
            assert op["op"] == "add"
            field = op["path"].lstrip("/")
            if field == RELATED:
                entity[field] = [
                    {**link, "term": {**link["term"], "fullyQualifiedName": self._by_id(link["term"]["id"])[FQN]}}
                    for link in op["value"]
                ]
            else:
                entity[field] = copy.deepcopy(op["value"])
        return copy.deepcopy(entity)

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        query: dict[str, str] | None = None,
        content_type: str = "application/json",
    ) -> Any:
        self.calls.append((method, path, copy.deepcopy(body)))
        route = (method, *path.strip("/").split("/")[1:])
        match route:
            case ("GET", "teams", "name", team) if team in self.teams:
                return {"id": self.teams[team]}
            case ("GET", "glossaries", "name", name):
                return self._get(self.glossaries, name, query)
            case ("GET", "glossaryTerms", "name", fqn):
                return self._get(self.terms, fqn, query)
            case ("POST", "glossaries"):
                created = {**copy.deepcopy(body), "id": "glossary-1", FQN: body["name"], "deleted": False}
                self.glossaries[body["name"]] = created
                return copy.deepcopy(created)
            case ("POST", "glossaryTerms"):
                return copy.deepcopy(self._create_term(body))
            case ("PATCH", "glossaries" | "glossaryTerms", entity_id):
                assert content_type == openmetadata.JSON_PATCH_CONTENT_TYPE
                return self._patch(self._by_id(entity_id), body)
        raise OpenMetadataError(f"{method} {path} -> 404: not found", 404)


@pytest.fixture
def glossary_dir(tmp_path: Path) -> Path:
    shutil.copytree(REPO_GLOSSARY, tmp_path / "glossary", ignore=shutil.ignore_patterns("*.md"))
    return tmp_path / "glossary"


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch, glossary_dir: Path) -> FakeOpenMetadata:
    fake = FakeOpenMetadata()
    monkeypatch.setattr(OpenMetadataClient, "from_env", classmethod(lambda cls: fake))
    monkeypatch.setattr(glossary_command, "get_repo_root", lambda: glossary_dir.parent)
    return fake


def _sync(glossary_dir: Path, **kwargs: Any) -> bool:
    """Run sync; return whether it exited non-zero."""
    try:
        glossary_command.sync(glossary_dir_path=str(glossary_dir), **kwargs)
    except SystemExit as exc:
        return exc.code != 0
    return False


def _edit_term(glossary_dir: Path, name: str, **fields: Any) -> None:
    path = glossary_dir / "open-learning.yaml"
    raw = yaml.safe_load(path.read_text())
    [term] = [t for t in raw["terms"] if t["name"] == name]
    term.update(fields)
    path.write_text(yaml.safe_dump(raw, sort_keys=False))


def _write(glossary_dir: Path, terms: list[dict[str, Any]]) -> None:
    team = [{"type": "team", "name": "data-platform"}]
    raw = {
        "glossary": {"name": "g", "displayName": "G", "description": "d", "owners": team, "reviewers": team},
        "terms": [{"displayName": t["name"], "description": t["name"], **t} for t in terms],
    }
    (glossary_dir / "open-learning.yaml").write_text(yaml.safe_dump(raw, sort_keys=False))


def test_the_repository_glossary_loads() -> None:
    [(_, glossary)] = load_glossaries(REPO_GLOSSARY)
    assert glossary.glossary.name == "open-learning"
    assert len(glossary.terms) == 20
    assert "open-learning.identifiability.direct-identifier" in glossary.term_fqns()


def test_first_sync_creates_the_glossary_and_every_term(server: FakeOpenMetadata, glossary_dir: Path) -> None:
    assert _sync(glossary_dir) is False
    glossary = server.glossaries["open-learning"]
    assert glossary["owners"] == glossary["reviewers"] == [{"id": TEAM_ID, "type": "team"}]
    assert glossary["mutuallyExclusive"] is False
    assert len(server.terms) == 20
    posts = [c for c in server.writes() if c[1] == "/v1/glossaryTerms"]
    assert len(posts) == 20
    assert all(c[0] == "POST" for c in server.writes())
    assert all("entityStatus" not in body for _, _, body in posts)
    child = next(body for _, _, body in posts if body["name"] == "direct-identifier")
    assert child["parent"] == "open-learning.identifiability"
    assert child["references"] == [
        {
            "name": "NIST SP 800-122",
            "endpoint": "https://nvlpubs.nist.gov/nistpubs/legacy/sp/nistspecialpublication800-122.pdf",
        }
    ]
    assert server.terms["open-learning.identifiability"]["mutuallyExclusive"] is True


def test_dry_run_on_an_empty_server_reports_every_create_and_writes_nothing(
    server: FakeOpenMetadata, glossary_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _sync(glossary_dir, dry_run=True) is False
    assert server.writes() == []
    out = capsys.readouterr().out
    assert out.count("create") == 21
    assert "skipped" not in out


def test_a_glossary_that_already_matches_gets_no_writes(server: FakeOpenMetadata, glossary_dir: Path) -> None:
    _sync(glossary_dir)
    # Synonyms in another order, and an empty list where the file sets none (as production returns it).
    platform = server.terms["open-learning.platform"]
    platform["synonyms"].reverse()
    platform["references"] = []
    assert "synonyms" not in server.terms["open-learning.identifiability"]
    server.calls.clear()
    assert _sync(glossary_dir) is False
    assert server.writes() == []


def test_a_description_change_is_one_patch(server: FakeOpenMetadata, glossary_dir: Path) -> None:
    _sync(glossary_dir)
    server.calls.clear()
    _edit_term(glossary_dir, "learner", description="A user who has enrolled.")
    assert _sync(glossary_dir) is False
    term = server.terms["open-learning.learner"]
    assert server.writes() == [
        (
            "PATCH",
            f"/v1/glossaryTerms/{term['id']}",
            [{"op": "add", "path": "/description", "value": "A user who has enrolled."}],
        )
    ]


def test_an_ui_edit_to_a_synced_field_is_put_back(server: FakeOpenMetadata, glossary_dir: Path) -> None:
    _sync(glossary_dir)
    server.terms["open-learning.course"]["synonyms"] = ["class"]
    server.glossaries["open-learning"]["description"] = "edited"
    server.calls.clear()
    assert _sync(glossary_dir) is False
    assert [(m, [op["path"] for op in body]) for m, _, body in server.writes()] == [
        ("PATCH", ["/description"]),
        ("PATCH", ["/synonyms"]),
    ]
    assert server.terms["open-learning.course"]["synonyms"] == ["course concept"]


def test_an_approved_term_stays_approved(server: FakeOpenMetadata, glossary_dir: Path) -> None:
    _sync(glossary_dir)
    for term in server.terms.values():
        term["entityStatus"] = "Approved"
    server.calls.clear()
    assert _sync(glossary_dir) is False
    assert server.writes() == []
    _edit_term(glossary_dir, "user", synonyms=["account holder"])
    assert _sync(glossary_dir) is False
    assert all(op["path"] != "/entityStatus" for _, _, body in server.writes() for op in body)
    assert {t["entityStatus"] for t in server.terms.values()} == {"Approved"}


def test_display_name_and_mutually_exclusive_are_set_only_on_create(
    server: FakeOpenMetadata, glossary_dir: Path
) -> None:
    _sync(glossary_dir)
    server.calls.clear()
    _edit_term(glossary_dir, "identifiability", displayName="How identifying", mutuallyExclusive=False)
    assert _sync(glossary_dir) is False
    assert server.writes() == []


def test_a_soft_deleted_term_is_left_alone(
    server: FakeOpenMetadata, glossary_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(glossary_dir)
    server.terms["open-learning.learner"].update(deleted=True, description="gone")
    server.calls.clear()
    assert _sync(glossary_dir) is False
    assert server.writes() == []
    assert "open-learning.learner: deleted in OpenMetadata, left alone" in " ".join(capsys.readouterr().out.split())


def test_a_missing_term_under_a_missing_parent_is_skipped(
    server: FakeOpenMetadata, glossary_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(glossary_dir)
    # A steward moved identifiability's children under pii.
    for name in ("direct-identifier", "indirect-identifier"):
        term = server.terms.pop(f"open-learning.identifiability.{name}")
        server.terms[f"open-learning.pii.{name}"] = {**term, FQN: f"open-learning.pii.{name}"}
    server.terms["open-learning.identifiability"]["deleted"] = True
    server.calls.clear()
    assert _sync(glossary_dir) is False
    assert server.writes() == []
    out = " ".join(capsys.readouterr().out.split())
    assert "open-learning.identifiability.direct-identifier: open-learning.identifiability is missing or deleted" in out


def test_a_term_whose_name_is_held_elsewhere_is_skipped(
    server: FakeOpenMetadata, glossary_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _sync(glossary_dir)
    term = server.terms.pop("open-learning.verified")
    server.terms["open-learning.enrollment-mode.verified"] = {**term, FQN: "open-learning.enrollment-mode.verified"}
    server.calls.clear()
    assert _sync(glossary_dir) is False
    assert [c[0] for c in server.writes()] == ["POST"]
    assert "name held elsewhere in the glossary" in " ".join(capsys.readouterr().out.split())


def test_a_missing_team_fails_the_glossary_and_the_terms_are_skipped(
    server: FakeOpenMetadata, glossary_dir: Path
) -> None:
    server.teams.clear()
    assert _sync(glossary_dir) is True
    assert server.writes() == []


def test_related_terms_are_declared_on_the_later_term(server: FakeOpenMetadata, glossary_dir: Path) -> None:
    _write(glossary_dir, [{"name": "a"}, {"name": "b", "relatedTerms": ["a"]}, {"name": "c"}])
    assert _sync(glossary_dir) is False
    [b_create] = [body for _, path, body in server.writes() if path == "/v1/glossaryTerms" and body["name"] == "b"]
    assert b_create["relatedTerms"] == ["g.a"]
    server.calls.clear()
    # The link b declares shows on a too, and a leaves it to b.
    assert _sync(glossary_dir) is False
    assert server.writes() == []

    _write(glossary_dir, [{"name": "a"}, {"name": "b", "relatedTerms": ["a"]}, {"name": "c", "relatedTerms": ["a"]}])
    assert _sync(glossary_dir) is False
    c = server.terms["g.c"]
    assert server.writes() == [
        (
            "PATCH",
            f"/v1/glossaryTerms/{c['id']}",
            [
                {
                    "op": "add",
                    "path": "/relatedTerms",
                    "value": [
                        {"relationType": "relatedTo", "term": {"id": server.terms["g.a"]["id"], "type": "glossaryTerm"}}
                    ],
                }
            ],
        )
    ]


@pytest.mark.parametrize(
    ("terms", "problem"),
    [
        ([{"name": "b", "parent": "a"}, {"name": "a"}], "not a term declared above it"),
        ([{"name": "a", "relatedTerms": ["a"]}], "not a term declared above it"),
        ([{"name": "a"}, {"name": "A"}], "declared twice"),
        ([{"name": "a.b"}], "pattern"),
        ([{"name": "a", "entityStatus": "Approved"}], "Extra inputs are not permitted"),
        ([{"name": "a", "description": "one row per <platform>"}], "HTML sanitizer"),
    ],
)
def test_a_malformed_glossary_file_is_refused(glossary_dir: Path, terms: list[dict[str, Any]], problem: str) -> None:
    _write(glossary_dir, terms)
    with pytest.raises(ValueError, match=problem):
        load_glossaries(glossary_dir)


def test_a_missing_glossary_directory_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no such glossary directory"):
        load_glossaries(tmp_path / "glossary")

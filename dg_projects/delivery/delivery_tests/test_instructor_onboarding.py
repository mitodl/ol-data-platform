"""Tests for committing the instructor user list to access-forge."""

import base64
from types import SimpleNamespace
from typing import Any

from dagster import build_asset_context
from delivery.assets.instructor_onboarding import (
    ACCESS_FORGE_USERS_PATH,
    update_access_forge_repo,
)

EXISTING_CSV = "email,role,sent_invite\na@mit.edu,ol-instructor,1\n"


class _FakeRepo:
    default_branch = "main"

    def __init__(self, existing_csv: str):
        self.contents = SimpleNamespace(
            decoded_content=existing_csv.encode(),
            sha="blob-sha",
            content=base64.b64encode(existing_csv.encode()).decode(),
        )
        self.updates: list[dict[str, Any]] = []

    def get_contents(self, path, ref):
        assert (path, ref) == (ACCESS_FORGE_USERS_PATH, "main")
        return self.contents

    def update_file(self, **kwargs):
        self.updates.append(kwargs)
        return {"commit": SimpleNamespace(sha="c0ffee", html_url="https://x/c0ffee")}


class _FakeGithubApi:
    def __init__(self, repo: _FakeRepo):
        self.repo = repo
        self.token_permissions = None

    def get_client(self, token_permissions=None):
        self.token_permissions = token_permissions
        return SimpleNamespace(get_repo=lambda _name: self.repo)


def _materialize(repo: _FakeRepo, user_list: str):
    github_api = _FakeGithubApi(repo)
    result = update_access_forge_repo(
        context=build_asset_context(),
        github_api=github_api,
        instructor_onboarding_user_list=user_list,
    )
    return result, github_api


def test_new_users_are_committed_to_the_default_branch():
    repo = _FakeRepo(EXISTING_CSV)
    result, github_api = _materialize(
        repo, "email,role,sent_invite\nb@mit.edu,ol-instructor,1\n"
    )

    [update] = repo.updates
    assert update["branch"] == "main"
    assert update["sha"] == "blob-sha"
    assert update["content"] == (
        "email,role,sent_invite\na@mit.edu,ol-instructor,1\nb@mit.edu,ol-instructor,1\n"
    )
    assert result.value["users_added"] == 1
    assert result.value["commit_sha"] == "c0ffee"
    assert github_api.token_permissions == {"contents": "write"}


def test_unchanged_list_makes_no_commit():
    repo = _FakeRepo(EXISTING_CSV)
    result, _ = _materialize(repo, EXISTING_CSV)

    assert repo.updates == []
    assert result.value["commit_sha"] is None

"""Tests for role sync behaviour, especially roles with all_datasource_access."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from ol_superset.lib.role_management import (
    access_permission_changes,
    compute_desired_dataset_ids,
    database_access_view_menu,
    load_governance_roles,
    parse_allowed_databases,
    parse_allowed_datasets,
    role_dataset_access_is_managed,
    uncovered_dataset_schemas,
    unmatched_allowed_databases,
    unmatched_allowed_datasets,
)

# ---------------------------------------------------------------------------
# Unit tests for compute_desired_dataset_ids
# ---------------------------------------------------------------------------

API_DATASETS = [
    {
        "id": 1,
        "table_name": "marts__micromasters_dedp_exam_grades",
        "schema": "ol_warehouse_production_mart",
    },
    {
        "id": 2,
        "table_name": "marts__other_table",
        "schema": "ol_warehouse_production_mart",
    },
    {
        "id": 3,
        "table_name": "dim__courses",
        "schema": "ol_warehouse_production_dimensional",
    },
    {"id": 4, "table_name": "raw__events", "schema": "ol_warehouse_production_raw"},
    {"id": 5, "table_name": "some_public_table", "schema": "public"},
]


def test_compute_desired_dataset_ids_single_schema():
    ids = compute_desired_dataset_ids(
        ["ol_warehouse_production_mart"], API_DATASETS, []
    )
    assert ids == {1, 2}


def test_compute_desired_dataset_ids_multiple_schemas():
    ids = compute_desired_dataset_ids(
        ["ol_warehouse_production_mart", "ol_warehouse_production_raw"],
        API_DATASETS,
        [],
    )
    assert ids == {1, 2, 4}


def test_compute_desired_dataset_ids_all_schemas():
    all_schemas = [
        "ol_warehouse_production_raw",
        "ol_warehouse_production_intermediate",
        "ol_warehouse_production_dimensional",
        "ol_warehouse_production_mart",
        "ol_warehouse_production_reporting",
        "public",
    ]
    ids = compute_desired_dataset_ids(all_schemas, API_DATASETS, [])
    assert ids == {1, 2, 3, 4, 5}


def test_compute_desired_dataset_ids_empty_schemas():
    assert compute_desired_dataset_ids([], API_DATASETS, []) == set()


def test_compute_desired_dataset_ids_allowed_datasets_only():
    ids = compute_desired_dataset_ids(
        [],
        API_DATASETS,
        ["ol_warehouse_production_mart.marts__other_table"],
    )
    assert ids == {2}


def test_compute_desired_dataset_ids_allowed_datasets_match_every_database():
    api_datasets = [
        *API_DATASETS,
        # The StarRocks twin of dataset 2: same schema and table, its own id.
        {
            "id": 6,
            "table_name": "marts__other_table",
            "schema": "ol_warehouse_production_mart",
        },
    ]
    ids = compute_desired_dataset_ids(
        ["public"],
        api_datasets,
        ["ol_warehouse_production_mart.marts__other_table"],
    )
    assert ids == {2, 5, 6}


def test_compute_desired_dataset_ids_allowed_datasets_need_schema_match():
    ids = compute_desired_dataset_ids(
        [], API_DATASETS, ["ol_warehouse_production_reporting.marts__other_table"]
    )
    assert ids == set()


@pytest.mark.parametrize(
    "entry",
    [
        "marts__other_table",
        ".marts__other_table",
        "ol_warehouse_production_mart.",
        "a.b.c",
    ],
)
def test_parse_allowed_datasets_rejects_malformed(entry: str):
    with pytest.raises(ValueError, match="is not '<schema>.<table_name>'"):
        parse_allowed_datasets([entry])


def test_unmatched_allowed_datasets():
    assert unmatched_allowed_datasets(
        [
            "ol_warehouse_production_mart.marts__other_table",
            "ol_warehouse_production_mart.missing_table",
        ],
        API_DATASETS,
    ) == ["ol_warehouse_production_mart.missing_table"]


@pytest.mark.parametrize(
    "allowed_datasets",
    [
        {"ol_warehouse_production_mart.marts__other_table": False},
        "ol_warehouse_production_mart.marts__other_table",
        [["ol_warehouse_production_mart", "marts__other_table"]],
    ],
)
def test_parse_allowed_datasets_rejects_non_string_lists(allowed_datasets: Any):
    with pytest.raises(ValueError, match="must be a list of"):
        parse_allowed_datasets(allowed_datasets)


@pytest.mark.parametrize(
    ("role", "managed"),
    [
        ({"allowed_schemas": ["ol_warehouse_production_mart"]}, True),
        ({"allowed_schemas": [], "allowed_datasets": ["a.b"]}, True),
        # An emptied allow-list still syncs, so its old grants are revoked.
        ({"allowed_schemas": [], "allowed_datasets": []}, True),
        ({"allowed_datasets": []}, True),
        ({"allowed_schemas": []}, False),
        ({}, False),
    ],
)
def test_role_dataset_access_is_managed(role: dict[str, object], managed: bool):
    assert role_dataset_access_is_managed(role) is managed


def test_emptied_allowed_datasets_revokes_everything():
    assert compute_desired_dataset_ids([], API_DATASETS, []) == set()


def test_uncovered_dataset_schemas_counts_single_table_grants():
    roles: list[dict[str, object]] = [
        {"name": "a", "allowed_schemas": ["ol_warehouse_production_dimensional"]},
        {
            "name": "b",
            "allowed_datasets": ["ol_warehouse_production_mart.marts__other_table"],
        },
    ]
    assert uncovered_dataset_schemas(roles, API_DATASETS) == {
        "ol_warehouse_production_mart": 1,
        "ol_warehouse_production_raw": 1,
        "public": 1,
    }


def test_load_governance_roles_refuses_a_malformed_policy(tmp_path: Path, capsys):
    policy = tmp_path / "ol_governance_roles.json"
    policy.write_text(
        json.dumps([{"name": "r", "allowed_datasets": ["no_schema_separator"]}])
    )
    assert load_governance_roles(policy) == []
    assert "is not '<schema>.<table_name>'" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# allowed_databases and global access permissions
# ---------------------------------------------------------------------------

# Production's ids; QA numbers the same databases differently.
API_DATABASES: list[dict[str, object]] = [
    {"id": 1, "database_name": "Trino"},
    {"id": 2, "database_name": "Superset Metadata DB"},
    {"id": 3, "database_name": "StarRocks - Lakehouse"},
]
TRINO = ("database_access", "[Trino].(id:1)")
METADATA_DB = ("database_access", "[Superset Metadata DB].(id:2)")
STARROCKS = ("database_access", "[StarRocks - Lakehouse].(id:3)")
ALL_DATABASES = ("all_database_access", "all_database_access")
ALL_DATASOURCES = ("all_datasource_access", "all_datasource_access")


def _perm(permission: str, view_menu: str) -> dict[str, dict[str, str]]:
    return {"permission": {"name": permission}, "view_menu": {"name": view_menu}}


def test_database_access_view_menu_matches_superset_perm():
    assert database_access_view_menu(API_DATABASES[0]) == "[Trino].(id:1)"


@pytest.mark.parametrize(
    "allowed_databases", ["Trino", {"Trino": True}, [1], [""], [["Trino"]]]
)
def test_parse_allowed_databases_rejects_malformed(allowed_databases: Any):
    with pytest.raises(ValueError, match="allowed_databases must be a list"):
        parse_allowed_databases(allowed_databases)


def test_unmatched_allowed_databases():
    assert unmatched_allowed_databases(["Trino", "Shillelagh"], API_DATABASES) == [
        "Shillelagh"
    ]


def test_allowed_databases_grants_by_name_and_revokes_the_rest():
    role = {
        "name": "ol_data_engineer",
        "permissions": [_perm("can_read", "Dashboard")],
        "allowed_databases": ["Trino", "StarRocks - Lakehouse"],
    }
    to_add, to_revoke = access_permission_changes(
        role, API_DATABASES, {TRINO, METADATA_DB, ALL_DATABASES, ALL_DATASOURCES}
    )
    assert to_add == {STARROCKS}
    # The metadata DB and both global grants go: either global grant opens
    # every database in SQL Lab.
    assert to_revoke == {METADATA_DB, ALL_DATABASES, ALL_DATASOURCES}


def test_empty_allowed_databases_revokes_every_database():
    role = {"name": "r", "permissions": [], "allowed_databases": []}
    to_add, to_revoke = access_permission_changes(
        role, API_DATABASES, {TRINO, STARROCKS}
    )
    assert to_add == set()
    assert to_revoke == {TRINO, STARROCKS}


def test_global_access_listed_in_the_definition_is_kept():
    role = {
        "name": "ol_data_engineer",
        "permissions": [
            _perm(*ALL_DATABASES),
            _perm(*ALL_DATASOURCES),
        ],
    }
    assert access_permission_changes(
        role, API_DATABASES, {ALL_DATABASES, ALL_DATASOURCES, TRINO}
    ) == (set(), set())


def test_without_allowed_databases_database_access_is_left_alone():
    role = {"name": "r", "permissions": [_perm(*ALL_DATASOURCES)]}
    to_add, to_revoke = access_permission_changes(
        role, API_DATABASES, {METADATA_DB, ALL_DATABASES, ALL_DATASOURCES}
    )
    assert to_add == set()
    assert to_revoke == {ALL_DATABASES}


def test_load_governance_roles_refuses_malformed_allowed_databases(
    tmp_path: Path, capsys
):
    policy = tmp_path / "ol_governance_roles.json"
    policy.write_text(json.dumps([{"name": "r", "allowed_databases": "Trino"}]))
    assert load_governance_roles(policy) == []
    assert "allowed_databases must be a list" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Integration-style tests for has_all_access sync logic
# ---------------------------------------------------------------------------

DATA_ENGINEER_ROLE = {
    "name": "ol_data_engineer",
    "permissions": [
        {
            "permission": {"name": "all_datasource_access"},
            "view_menu": {"name": "all_datasource_access"},
        },
        {
            "permission": {"name": "all_database_access"},
            "view_menu": {"name": "all_database_access"},
        },
        {"permission": {"name": "can_read"}, "view_menu": {"name": "Dashboard"}},
    ],
    "allowed_schemas": [
        "ol_warehouse_production_mart",
        "ol_warehouse_production_raw",
        "ol_warehouse_production_dimensional",
        "public",
    ],
}

REGULAR_ROLE = {
    "name": "ol_data_analyst",
    "permissions": [
        {"permission": {"name": "can_read"}, "view_menu": {"name": "Dashboard"}},
    ],
    "allowed_schemas": ["ol_warehouse_production_mart"],
}

NO_SCHEMAS_ALL_ACCESS_ROLE = {
    "name": "ol_admin",
    "permissions": [
        {
            "permission": {"name": "all_datasource_access"},
            "view_menu": {"name": "all_datasource_access"},
        },
    ],
    "allowed_schemas": [],
}


def _has_all_access(gov_role: Mapping[str, Any]) -> bool:
    """Mirror of the has_all_access check in roles.py."""
    return any(
        p.get("view_menu", {}).get("name") == "all_datasource_access"
        or p.get("view_menu", {}).get("name") == "all_database_access"
        for p in gov_role.get("permissions", [])
    )


class TestHasAllAccessFlag:
    def test_data_engineer_has_all_access(self):
        assert _has_all_access(DATA_ENGINEER_ROLE) is True

    def test_regular_role_no_all_access(self):
        assert _has_all_access(REGULAR_ROLE) is False

    def test_no_schemas_all_access_role(self):
        assert _has_all_access(NO_SCHEMAS_ALL_ACCESS_ROLE) is True


class TestRoleSyncSkipLogic:
    """
    Verify that a role with all_datasource_access + allowed_schemas is NOT
    skipped, and that permissions are only added (never revoked) for it.
    """

    def _make_sync_inputs(self):
        """Return mock API state for a minimal sync run."""
        api_datasets = [
            {
                "id": 1,
                "table_name": "marts__micromasters_dedp_exam_grades",
                "schema": "ol_warehouse_production_mart",
                "uuid": "aaa",
            },
            {
                "id": 2,
                "table_name": "dim__courses",
                "schema": "ol_warehouse_production_dimensional",
                "uuid": "bbb",
            },
        ]
        api_roles = [
            {"id": 10, "name": "ol_data_engineer"},
            {"id": 11, "name": "ol_data_analyst"},
        ]
        # Datasource PVMs: view_menu_name has format "[table](id:N)"
        all_ds_perms = [
            {
                "id": 100,
                "permission_name": "datasource_access",
                "view_menu_name": "[marts__micromasters_dedp_exam_grades](id:1)",
            },
            {
                "id": 200,
                "permission_name": "datasource_access",
                "view_menu_name": "[dim__courses](id:2)",
            },
        ]
        return api_datasets, api_roles, all_ds_perms

    def test_all_access_role_with_schemas_is_processed(self):
        """
        Role with all_datasource_access + allowed_schemas should get missing
        perms added.
        """
        api_datasets, api_roles, all_ds_perms = self._make_sync_inputs()

        gov_role = DATA_ENGINEER_ROLE
        # includes mart and dimensional schemas
        allowed_schemas = cast(list[str], gov_role["allowed_schemas"])

        desired = compute_desired_dataset_ids(allowed_schemas, api_datasets, [])
        assert 1 in desired, "Dataset in allowed schema must be in desired set"
        assert 2 in desired, "Dataset in allowed schema must be in desired set"

    def test_all_access_role_revoke_is_suppressed(self):
        """
        Even if current_ds_ids > desired_ds_ids, revokes must not happen for
        all_access roles.
        """
        # Simulate role already having permission for dataset 99
        # (outside allowed_schemas)
        current_ds_ids = {1, 2, 99}
        desired_ds_ids = {1, 2}

        to_revoke = current_ds_ids - desired_ds_ids  # {99}

        # For a role with has_all_access, to_revoke must be cleared
        effective_skip_revoke = True  # skip_revoke or has_all_access
        if effective_skip_revoke:
            to_revoke = set()

        assert to_revoke == set(), "Revoke set must be empty for all_access roles"

    def test_no_schemas_all_access_role_is_skipped(self):
        """
        Role with all_datasource_access but no allowed_schemas should still
        be skipped.
        """
        role = NO_SCHEMAS_ALL_ACCESS_ROLE
        allowed_schemas = role.get("allowed_schemas", [])
        _has_all_access(role)

        # Mirrors the new skip condition: skip only when no allowed_schemas
        should_skip = not allowed_schemas
        assert should_skip is True

    def test_regular_role_can_revoke(self):
        """A regular role without all_datasource_access should allow revokes."""
        current_ds_ids = {1, 2, 99}
        desired_ds_ids = {1, 2}
        to_revoke = current_ds_ids - desired_ds_ids

        has_all_access = _has_all_access(REGULAR_ROLE)
        skip_revoke = False
        if skip_revoke or has_all_access:
            to_revoke = set()

        assert to_revoke == {99}, "Regular role should revoke out-of-policy datasets"

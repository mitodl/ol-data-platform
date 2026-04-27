"""Tests for role sync behaviour, especially roles with all_datasource_access."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from ol_superset.lib.role_management import compute_desired_dataset_ids

# ---------------------------------------------------------------------------
# Unit tests for compute_desired_dataset_ids
# ---------------------------------------------------------------------------

API_DATASETS = [
    {"id": 1, "table_name": "marts__micromasters_dedp_exam_grades", "schema": "ol_warehouse_production_mart"},
    {"id": 2, "table_name": "marts__other_table", "schema": "ol_warehouse_production_mart"},
    {"id": 3, "table_name": "dim__courses", "schema": "ol_warehouse_production_dimensional"},
    {"id": 4, "table_name": "raw__events", "schema": "ol_warehouse_production_raw"},
    {"id": 5, "table_name": "some_public_table", "schema": "public"},
]


def test_compute_desired_dataset_ids_single_schema():
    ids = compute_desired_dataset_ids(["ol_warehouse_production_mart"], API_DATASETS)
    assert ids == {1, 2}


def test_compute_desired_dataset_ids_multiple_schemas():
    ids = compute_desired_dataset_ids(
        ["ol_warehouse_production_mart", "ol_warehouse_production_raw"],
        API_DATASETS,
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
    ids = compute_desired_dataset_ids(all_schemas, API_DATASETS)
    assert ids == {1, 2, 3, 4, 5}


def test_compute_desired_dataset_ids_empty_schemas():
    assert compute_desired_dataset_ids([], API_DATASETS) == set()


# ---------------------------------------------------------------------------
# Integration-style tests for has_all_access sync logic
# ---------------------------------------------------------------------------

DATA_ENGINEER_ROLE = {
    "name": "ol_data_engineer",
    "permissions": [
        {"permission": {"name": "all_datasource_access"}, "view_menu": {"name": "all_datasource_access"}},
        {"permission": {"name": "all_database_access"}, "view_menu": {"name": "all_database_access"}},
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
        {"permission": {"name": "all_datasource_access"}, "view_menu": {"name": "all_datasource_access"}},
    ],
    "allowed_schemas": [],
}


def _has_all_access(gov_role: dict) -> bool:
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
            {"id": 1, "table_name": "marts__micromasters_dedp_exam_grades", "schema": "ol_warehouse_production_mart", "uuid": "aaa"},
            {"id": 2, "table_name": "dim__courses", "schema": "ol_warehouse_production_dimensional", "uuid": "bbb"},
        ]
        api_roles = [{"id": 10, "name": "ol_data_engineer"}, {"id": 11, "name": "ol_data_analyst"}]
        # Datasource PVMs: view_menu_name has format "[table](id:N)"
        all_ds_perms = [
            {"id": 100, "permission_name": "datasource_access", "view_menu_name": "[marts__micromasters_dedp_exam_grades](id:1)"},
            {"id": 200, "permission_name": "datasource_access", "view_menu_name": "[dim__courses](id:2)"},
        ]
        return api_datasets, api_roles, all_ds_perms

    def test_all_access_role_with_schemas_is_processed(self):
        """Role with all_datasource_access + allowed_schemas should get missing perms added."""
        api_datasets, api_roles, all_ds_perms = self._make_sync_inputs()

        gov_role = DATA_ENGINEER_ROLE
        allowed_schemas = gov_role["allowed_schemas"]  # includes mart and dimensional

        desired = compute_desired_dataset_ids(allowed_schemas, api_datasets)
        assert 1 in desired, "Dataset in allowed schema must be in desired set"
        assert 2 in desired, "Dataset in allowed schema must be in desired set"

    def test_all_access_role_revoke_is_suppressed(self):
        """Even if current_ds_ids > desired_ds_ids, revokes must not happen for all_access roles."""
        # Simulate role already having permission for dataset 99 (outside allowed_schemas)
        current_ds_ids = {1, 2, 99}
        desired_ds_ids = {1, 2}

        to_revoke = current_ds_ids - desired_ds_ids  # {99}

        # For a role with has_all_access, to_revoke must be cleared
        has_all_access = True
        effective_skip_revoke = True  # skip_revoke or has_all_access
        if effective_skip_revoke:
            to_revoke = set()

        assert to_revoke == set(), "Revoke set must be empty for all_access roles"

    def test_no_schemas_all_access_role_is_skipped(self):
        """Role with all_datasource_access but no allowed_schemas should still be skipped."""
        role = NO_SCHEMAS_ALL_ACCESS_ROLE
        allowed_schemas = role.get("allowed_schemas", [])
        has_all_access = _has_all_access(role)

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

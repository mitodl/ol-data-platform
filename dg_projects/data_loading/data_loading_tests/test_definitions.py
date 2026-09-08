"""Smoke test that the code location loads the same way the gRPC server does.

`dagster api grpc -m data_loading.definitions` (the user-code server in K8s) imports
this module and builds the repository. Importing it here exercises that same
import + build path, so a broken code location is caught in CI without needing a
Docker/gRPC round-trip. Importing under the default (dev) profile is hermetic —
pipeline/destination objects are constructed but nothing connects to S3/Glue.
"""

import importlib
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from data_loading.definitions import defs
from data_loading.defs.ingestion.assets import MITXONLINE_APP_DLT_ENVIRONMENTS

_REPO = defs.get_repository_def()

# Packages whose module-level code reads DAGSTER_ENVIRONMENT, so a build under
# a different environment has to re-import them rather than reuse what the
# module-level import above cached.
_ENVIRONMENT_SENSITIVE_ROOTS = frozenset({"data_loading", "ol_orchestrate"})

# The three tables RFC 12711 step 8's pilot slice reads.
_B2B_PILOT_ASSET_KEYS = (
    "ol_warehouse_raw_data/raw__mitxonline__app__postgres__b2b_organizationpage",
    "ol_warehouse_raw_data/raw__mitxonline__app__postgres__b2b_contractpage",
    "ol_warehouse_raw_data/raw__mitxonline__app__postgres__courses_courserun",
)


@contextmanager
def _repository_for(environment: str) -> Iterator[Any]:
    """Build the code location as the gRPC server would under ``environment``.

    ``DAGSTER_ENV`` is resolved once, at import of
    ``ol_orchestrate.lib.constants``, and the ingestion modules read it at
    their own import -- so an environment gate can only be exercised by
    re-importing the tree with the variable set. Asserting on the gate's
    constant instead would stay green if somebody dropped the conditional it
    guards, which is the regression worth catching.

    ``DLT_PROFILE`` is pinned to ``dev`` so the re-import stays hermetic: the
    destination objects are constructed either way, and a production one has
    no business being built in a test.
    """
    saved_modules = {
        name: module
        for name, module in sys.modules.items()
        if name.split(".")[0] in _ENVIRONMENT_SENSITIVE_ROOTS
    }
    saved_env = {
        key: os.environ.get(key) for key in ("DAGSTER_ENVIRONMENT", "DLT_PROFILE")
    }

    def _purge() -> None:
        for name in list(sys.modules):
            if name.split(".")[0] in _ENVIRONMENT_SENSITIVE_ROOTS:
                del sys.modules[name]

    os.environ["DAGSTER_ENVIRONMENT"] = environment
    os.environ["DLT_PROFILE"] = "dev"
    _purge()
    try:
        module = importlib.import_module("data_loading.definitions")
        yield module.defs.get_repository_def()
    finally:
        _purge()
        sys.modules.update(saved_modules)
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_code_location_builds() -> None:
    asset_keys = {k.to_user_string() for k in _REPO.assets_defs_by_key}
    assert asset_keys, "code location exposed no assets"
    # Every ingest asset lands under the raw-data key prefix...
    assert all(k.startswith("ol_warehouse_raw_data/") for k in asset_keys)
    # ...and the headline sources (simple, edxorg S3, edxorg programs) are present.
    for expected in (
        "ol_warehouse_raw_data/raw__oll__google_sheets__courses",
        "ol_warehouse_raw_data/raw__edxorg__s3__tables__auth_user",
        "ol_warehouse_raw_data/raw__edxorg__discovery__api__programs",
    ):
        assert expected in asset_keys


def test_schedules_and_sensors_load() -> None:
    assert {s.name for s in _REPO.schedule_defs} >= {
        "oll_ingest_daily_schedule",
        "mitpe_ingest_daily_schedule",
        "mit_climate_ingest_daily_schedule",
        "mit_edx_programs_ingest_daily_schedule",
        "podcast_rss_ingest_daily_schedule",
    }
    assert "edxorg_upstream_changes_sensor" in {s.name for s in _REPO.sensor_defs}


def test_dlt_resource_present() -> None:
    assert "dlt" in defs.resources


@pytest.mark.parametrize("environment", sorted(MITXONLINE_APP_DLT_ENVIRONMENTS))
def test_mitxonline_app_assets_load_where_dlt_owns_the_unit(environment: str) -> None:
    """Every environment in the gate really does get the assets and schedule.

    Parameterized over the gate itself, and each case builds under that
    environment explicitly, so the result does not depend on whichever
    environment the test runner happens to be in.
    """
    with _repository_for(environment) as repo:
        asset_keys = {key.to_user_string() for key in repo.assets_defs_by_key}
        assert set(_B2B_PILOT_ASSET_KEYS) <= asset_keys
        assert (
            len(
                [key for key in asset_keys if "raw__mitxonline__app__postgres__" in key]
            )
            == 64
        )
        assert "mitxonline_app_ingest_schedule" in {s.name for s in repo.schedule_defs}


def test_mitxonline_app_dlt_does_not_run_in_production() -> None:
    """Production still loads this unit through Airbyte, and the keys collide.

    The lakehouse code location prefixes every Airbyte stream asset with
    ``ol_warehouse_raw_data`` and keys it on the connection prefix plus the
    stream name -- exactly the keys these dlt assets produce. Two code
    locations claiming one asset key, and two loaders writing one Iceberg
    table. Adding ``production`` to the gate is only correct in the same change
    that disables the Airbyte connection.

    Builds the code location under ``production`` rather than inspecting the
    gate's constant: dropping the conditional the constant guards is the
    regression this exists to catch, and a constant-only assertion would not
    notice.
    """
    with _repository_for("production") as repo:
        asset_keys = {key.to_user_string() for key in repo.assets_defs_by_key}
        assert asset_keys, "code location exposed no assets under production"
        assert not [key for key in asset_keys if "mitxonline" in key]
        assert "mitxonline_app_ingest_schedule" not in {
            s.name for s in repo.schedule_defs
        }

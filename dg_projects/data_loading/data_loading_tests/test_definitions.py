"""Smoke test that the code location loads the same way the gRPC server does.

`dagster api grpc -m data_loading.definitions` (the user-code server in K8s) imports
this module and builds the repository. Importing it here exercises that same
import + build path, so a broken code location is caught in CI without needing a
Docker/gRPC round-trip. Importing under the default (dev) profile is hermetic —
pipeline/destination objects are constructed but nothing connects to S3/Glue.
"""

from data_loading.definitions import defs
from data_loading.defs.ingestion.assets import MITXONLINE_APP_DLT_ENVIRONMENTS

_REPO = defs.get_repository_def()


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


def test_mitxonline_app_assets_load_outside_production() -> None:
    """The MITx Online app tables are dlt assets everywhere dlt owns them.

    Tests import under the default ``dev`` profile, which is inside
    ``MITXONLINE_APP_DLT_ENVIRONMENTS``, so the assets must be here.
    """
    asset_keys = {k.to_user_string() for k in _REPO.assets_defs_by_key}
    for expected in (
        "ol_warehouse_raw_data/raw__mitxonline__app__postgres__b2b_organizationpage",
        "ol_warehouse_raw_data/raw__mitxonline__app__postgres__b2b_contractpage",
        "ol_warehouse_raw_data/raw__mitxonline__app__postgres__courses_courserun",
    ):
        assert expected in asset_keys
    assert "mitxonline_app_ingest_schedule" in {s.name for s in _REPO.schedule_defs}


def test_mitxonline_app_dlt_does_not_run_in_production() -> None:
    """Production still loads this unit through Airbyte, and the keys collide.

    The lakehouse code location prefixes every Airbyte stream asset with
    ``ol_warehouse_raw_data`` and keys it on the connection prefix plus the
    stream name -- exactly the keys these dlt assets produce. Two code
    locations claiming one asset key, and two loaders writing one Iceberg
    table. Adding ``production`` here is only correct in the same change that
    disables the Airbyte connection.
    """
    assert "production" not in MITXONLINE_APP_DLT_ENVIRONMENTS

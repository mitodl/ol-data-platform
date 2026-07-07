"""Smoke test that the code location loads the same way the gRPC server does.

`dagster api grpc -m data_loading.definitions` (the user-code server in K8s) imports
this module and builds the repository. Importing it here exercises that same
import + build path, so a broken code location is caught in CI without needing a
Docker/gRPC round-trip. Importing under the default (dev) profile is hermetic —
pipeline/destination objects are constructed but nothing connects to S3/Glue.
"""

from data_loading.definitions import defs

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
    }
    assert "edxorg_upstream_changes_sensor" in {s.name for s in _REPO.sensor_defs}


def test_dlt_resource_present() -> None:
    assert "dlt" in defs.resources

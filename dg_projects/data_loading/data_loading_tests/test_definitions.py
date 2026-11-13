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

import dagster as dg
import pytest
from data_loading.definitions import defs
from data_loading.defs.ingestion.assets import (
    MITXONLINE_APP_DLT_ENVIRONMENTS,
    MITXONLINE_APP_KEYSET_ENVIRONMENTS,
    MITXONLINE_APP_KEYSET_UNIT,
    QA_FIRST_APP_DATABASE_ENVIRONMENTS,
    QA_FIRST_APP_DATABASES,
    XPRO_APP_DLT_ENVIRONMENTS,
)
from ol_dlt.sources import (
    edxapp,
    learn_ai_app,
    mitlearn_app,
    mitxonline_app,
    ocw_studio_app,
    xpro_app,
)
from sqlalchemy.engine import URL

from ol_dlt import database, keyset

_REPO = defs.get_repository_def()

# Packages whose module-level code reads DAGSTER_ENVIRONMENT, so a build under
# a different environment has to re-import them rather than reuse what the
# module-level import above cached.
_ENVIRONMENT_SENSITIVE_ROOTS = frozenset({"data_loading", "ol_orchestrate"})

# Moved from the canvas code location (issue #2260) with their keys unchanged,
# so their materialization history and partitions carry over.
_CANVAS_EXPORT_ASSET_KEYS = frozenset(
    {"canvas/course_content", "canvas/course_metadata"}
)

# One partition per OCW course. It writes the raw table, whose own key stays
# unpartitioned.
_OCW_COURSE_LOAD_KEY = "ocw/course_content_load"

# The Canvas instigators that were RUNNING in production in the canvas location.
_CANVAS_INSTIGATOR_NAMES = frozenset(
    {"canvas_course_export_schedule", "canvas_google_sheet_course_id_sensor"}
)

# The four tables RFC 12711 step 8's pilot slice reads.
_B2B_PILOT_ASSET_KEYS = (
    "ol_warehouse_raw_data/raw__mitxonline__app__postgres__b2b_organizationpage",
    "ol_warehouse_raw_data/raw__mitxonline__app__postgres__b2b_contractpage",
    "ol_warehouse_raw_data/raw__mitxonline__app__postgres__courses_courserun",
    "ol_warehouse_raw_data/raw__mitxonline__app__postgres__courses_courserun_b2b_contracts",
)


def _is_mitxonline_app_table(key: str) -> bool:
    """Return True for a MITx Online app raw table, and not its keyset."""
    return "raw__mitxonline__app__postgres__" in key and not key.endswith(
        keyset.KEYSET_SUFFIX
    )


@contextmanager
def _repository_for(environment: str, profile: str = "dev") -> Iterator[Any]:
    """Build the code location as the gRPC server would under ``environment``.

    ``DAGSTER_ENV`` is resolved once, at import of
    ``ol_orchestrate.lib.constants``, and the ingestion modules read it at
    their own import -- so an environment gate can only be exercised by
    re-importing the tree with the variable set. Asserting on the gate's
    constant instead would stay green if somebody dropped the conditional it
    guards, which is the regression worth catching.

    ``DLT_PROFILE`` defaults to ``dev`` so the re-import stays hermetic: the
    destination objects are constructed either way, and a production one has
    no business being built in a test. Pass ``qa`` only to catch a source that
    fails to build under the profile QA's code server actually runs.
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
    os.environ["DLT_PROFILE"] = profile
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
    # Every ingest asset lands under the raw-data key prefix, except the Canvas
    # course exports, which keep the keys they had in the canvas location...
    assert all(
        k.startswith("ol_warehouse_raw_data/")
        or k in _CANVAS_EXPORT_ASSET_KEYS
        or k == _OCW_COURSE_LOAD_KEY
        for k in asset_keys
    )
    assert asset_keys >= _CANVAS_EXPORT_ASSET_KEYS
    # ...and the headline sources (simple, edxorg S3, edxorg programs) are present.
    for expected in (
        "ol_warehouse_raw_data/raw__oll__google_sheets__courses",
        "ol_warehouse_raw_data/raw__edxorg__s3__tables__auth_user",
        "ol_warehouse_raw_data/raw__edxorg__discovery__api__programs",
        "ol_warehouse_raw_data/raw__edxorg__discovery__api__program",
        "ol_warehouse_raw_data/raw__edxorg__discovery__api__program_course",
        "ol_warehouse_raw_data/raw__edxorg__discovery__api__mitx_course",
        "ol_warehouse_raw_data/raw__edxorg__discovery__api__mitx_course_run",
        "ol_warehouse_raw_data/raw__posthog__learn__s3__events",
        "ol_warehouse_raw_data/raw__see__api__courses",
        "ol_warehouse_raw_data/raw__see__api__course_offerings",
        "ol_warehouse_raw_data/raw__mitpe__api__news",
        "ol_warehouse_raw_data/raw__mitpe__api__events",
        "ol_warehouse_raw_data/raw__openlearning__api__events",
        "ol_warehouse_raw_data/raw__medium__rss__posts",
        "ol_warehouse_raw_data/raw__youtube__api__channels",
        "ol_warehouse_raw_data/raw__youtube__api__playlists",
        "ol_warehouse_raw_data/raw__youtube__api__playlist_items",
        "ol_warehouse_raw_data/raw__youtube__api__videos",
        "ol_warehouse_raw_data/raw__google_ads__api__keyword_daily",
        "ol_warehouse_raw_data/raw__google_ads__api__change_events",
        "ol_warehouse_raw_data/raw__edxorg__s3__course_xml_blocks",
        "ol_warehouse_raw_data/raw__edxorg__s3__course_structure_blocks",
        "ol_warehouse_raw_data/raw__openedx__s3__course_xml_blocks",
        "ol_warehouse_raw_data/raw__mitx__openedx__api__course_structure_blocks",
        "ol_warehouse_raw_data/raw__mitxonline__openedx__api__course_structure_blocks",
        "ol_warehouse_raw_data/raw__xpro__openedx__api__course_structure_blocks",
        "ol_warehouse_raw_data/raw__openedx__s3__course_document_text",
        "ol_warehouse_raw_data/raw__openedx__s3__course_transcript_text",
        "ol_warehouse_raw_data/raw__openedx__s3__course_file_exclusions",
        "ol_warehouse_raw_data/raw__openedx__s3__course_units",
        "ol_warehouse_raw_data/raw__ocw__s3__course_content",
        "ol_warehouse_raw_data/raw__edxorg__s3__program_learner_report",
        "ol_warehouse_raw_data/raw__xpro__openedx__tracking_log_events",
        "ol_warehouse_raw_data/raw__mitxonline__openedx__tracking_log_events",
        "ol_warehouse_raw_data/raw__mitx__openedx__tracking_log_events",
    ):
        assert expected in asset_keys


def test_schedules_and_sensors_load() -> None:
    assert {s.name for s in _REPO.schedule_defs} >= {
        "oll_ingest_daily_schedule",
        "mitpe_ingest_daily_schedule",
        "google_ads_ingest_daily_schedule",
        "mit_climate_ingest_daily_schedule",
        "mit_edx_programs_ingest_daily_schedule",
        "podcast_rss_ingest_daily_schedule",
        "see_ingest_daily_schedule",
        "news_events_ingest_daily_schedule",
        "posthog_events_ingest_hourly_schedule",
        "course_xml_blocks_ingest_daily_schedule",
        "edxorg_program_reports_ingest_daily_schedule",
        "xpro_tracking_logs_ingest_schedule",
        "mitxonline_tracking_logs_ingest_schedule",
        "mitx_tracking_logs_ingest_schedule",
        "ocw_content_ingest_daily_schedule",
        "canvas_content_ingest_daily_schedule",
    }
    assert {s.name for s in _REPO.sensor_defs} >= {
        "edxorg_upstream_changes_sensor",
        "ocw_content_publish_sensor",
        "google_ads_freshness_sensor",
    }


@pytest.mark.parametrize("environment", ["dev", "ci", "qa", "production"])
def test_canvas_instigators_start_running_in_production_only(environment: str) -> None:
    """Read off the built repository, so the wiring and the status are both pinned.

    Both were RUNNING in production in the canvas location and never ticked in
    QA. Instigator state is keyed on location name, so without the production
    default they would arrive STOPPED and the exports would stop.
    """
    expected = "RUNNING" if environment == "production" else "STOPPED"
    with _repository_for(environment) as repo:
        instigators = {
            instigator.name: instigator
            for instigator in [*repo.schedule_defs, *repo.sensor_defs]
            if instigator.name in _CANVAS_INSTIGATOR_NAMES
        }
        assert set(instigators) == _CANVAS_INSTIGATOR_NAMES
        assert {
            instigator.default_status.value for instigator in instigators.values()
        } == {expected}


def test_canvas_sheet_sensor_requests_only_local_assets() -> None:
    """The webhook asset is in delivery, and a sensor cannot launch it from here.

    A run request naming canvas/course_content_metadata would fail at launch,
    taking the new course's export with it.
    """
    sensor = _REPO.get_sensor_def("canvas_google_sheet_course_id_sensor")
    selected = {
        key.to_user_string()
        for key in sensor.asset_selection.resolve(_REPO.asset_graph)
    }
    assert selected == _CANVAS_EXPORT_ASSET_KEYS


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
        assert len([key for key in asset_keys if _is_mitxonline_app_table(key)]) == len(
            mitxonline_app.MITXONLINE_APP_SPEC.tables
        )
        schedule = repo.get_schedule_def("mitxonline_app_ingest_schedule")
        # The MITx Online structure blocks share the "mitxonline" group and
        # read the production landing zone, so the app schedule must not
        # select them.
        scheduled = {
            key.to_user_string()
            for key in repo.get_job(schedule.job_name).asset_layer.selected_asset_keys
        }
        assert scheduled == {key for key in asset_keys if _is_mitxonline_app_table(key)}


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
        assert not [key for key in asset_keys if "raw__mitxonline__app__" in key]
        assert "mitxonline_app_ingest_schedule" not in {
            s.name for s in repo.schedule_defs
        }


_QA_FIRST_SPECS = {
    "mitlearn_app": mitlearn_app.MITLEARN_APP_SPEC,
    "learn_ai_app": learn_ai_app.LEARN_AI_APP_SPEC,
    "ocw_studio_app": ocw_studio_app.OCW_STUDIO_APP_SPEC,
    **{spec.name: spec for spec in edxapp.EDXAPP_SPECS.values()},
}


def test_every_qa_first_app_database_has_a_spec_here() -> None:
    assert set(QA_FIRST_APP_DATABASES) == set(_QA_FIRST_SPECS)


@pytest.mark.parametrize("environment", sorted(QA_FIRST_APP_DATABASE_ENVIRONMENTS))
@pytest.mark.parametrize("name", sorted(_QA_FIRST_SPECS))
def test_qa_first_app_database_loads_and_is_scheduled(
    environment: str, name: str
) -> None:
    spec = _QA_FIRST_SPECS[name]
    expected = {
        f"ol_warehouse_raw_data/{spec.raw_table_name(table)}" for table in spec.tables
    }
    with _repository_for(environment) as repo:
        asset_keys = {key.to_user_string() for key in repo.assets_defs_by_key}
        assert expected <= asset_keys
        schedule = repo.get_schedule_def(f"{name}_ingest_schedule")
        scheduled = {
            key.to_user_string()
            for key in repo.get_job(schedule.job_name).asset_layer.selected_asset_keys
        }
        assert scheduled == expected


@pytest.mark.parametrize("environment", ["dev", "production"])
def test_qa_first_app_databases_do_not_run_outside_their_environments(
    environment: str,
) -> None:
    """Airbyte still loads these units in production under the same keys, and
    the local lake gets them from fixtures carrying the production loader's
    columns.
    """
    with _repository_for(environment) as repo:
        asset_keys = {key.to_user_string() for key in repo.assets_defs_by_key}
        assert asset_keys, f"code location exposed no assets under {environment}"
        prefixes = tuple(spec.raw_table_prefix for spec in _QA_FIRST_SPECS.values())
        assert not [
            key for key in asset_keys if key.split("/")[-1].startswith(prefixes)
        ]
        schedule_names = {s.name for s in repo.schedule_defs}
        assert not {f"{name}_ingest_schedule" for name in _QA_FIRST_SPECS} & (
            schedule_names
        )


@pytest.mark.parametrize("environment", sorted(MITXONLINE_APP_KEYSET_ENVIRONMENTS))
def test_mitxonline_app_keysets_load_unscheduled(environment: str) -> None:
    """One keyset asset per rendered table, and no schedule runs them.

    The run after every load belongs to the keyset automation, which has to
    order it after the load and before the snapshots.
    """
    with _repository_for(environment) as repo:
        keyset_keys = {
            key.to_user_string()
            for key in repo.assets_defs_by_key
            if key.to_user_string().endswith(keyset.KEYSET_SUFFIX)
        }
        assert keyset_keys == {
            "ol_warehouse_raw_data/raw__mitxonline__app__postgres__"
            f"{table.name}{keyset.KEYSET_SUFFIX}"
            for table in keyset.inventory_keyset_tables()[MITXONLINE_APP_KEYSET_UNIT]
        }
        for schedule in repo.schedule_defs:
            selected = repo.get_job(schedule.job_name).asset_layer.selected_asset_keys
            assert not {key.to_user_string() for key in selected} & keyset_keys


def test_mitxonline_app_keysets_do_not_run_in_production() -> None:
    """Production keysets read the replica, whose address is not exported yet."""
    with _repository_for("production") as repo:
        asset_keys = {key.to_user_string() for key in repo.assets_defs_by_key}
        assert asset_keys, "code location exposed no assets under production"
        assert not [key for key in asset_keys if key.endswith(keyset.KEYSET_SUFFIX)]


@pytest.mark.parametrize("environment", sorted(XPRO_APP_DLT_ENVIRONMENTS))
def test_xpro_app_assets_load_where_dlt_owns_the_unit(environment: str) -> None:
    with _repository_for(environment) as repo:
        asset_keys = {key.to_user_string() for key in repo.assets_defs_by_key}
        assert len(
            [key for key in asset_keys if "raw__xpro__app__postgres__" in key]
        ) == len(xpro_app.XPRO_APP_SPEC.tables)
        schedule = repo.get_schedule_def("xpro_app_ingest_schedule")
        # The xPro structure blocks share the "xpro" group, so the app schedule
        # must not select them.
        scheduled = {
            key.to_user_string()
            for key in repo.get_job(schedule.job_name).asset_layer.selected_asset_keys
        }
        assert scheduled == {
            key for key in asset_keys if "raw__xpro__app__postgres__" in key
        }


def test_xpro_app_dlt_does_not_run_in_production() -> None:
    """Production still loads this unit through Airbyte under the same keys."""
    with _repository_for("production") as repo:
        asset_keys = {key.to_user_string() for key in repo.assets_defs_by_key}
        assert asset_keys, "code location exposed no assets under production"
        assert not [key for key in asset_keys if "raw__xpro__app__postgres__" in key]
        assert "xpro_app_ingest_schedule" not in {s.name for s in repo.schedule_defs}


class _FakeInstance:
    def __init__(self, in_flight: bool) -> None:  # noqa: FBT001
        self.in_flight = in_flight
        self.filters: list[Any] = []

    def get_run_records(self, filters: Any, limit: int) -> list[object]:  # noqa: ARG002
        self.filters.append(filters)
        return [object()] if self.in_flight else []


class _FakeScheduleContext:
    def __init__(self, instance: _FakeInstance) -> None:
        self.instance = instance


@pytest.mark.parametrize(("in_flight", "expected"), [(True, False), (False, True)])
def test_posthog_schedule_skips_while_a_run_is_in_flight(
    in_flight: bool,  # noqa: FBT001
    expected: bool,  # noqa: FBT001
) -> None:
    from data_loading.defs.ingestion import schedules  # noqa: PLC0415

    instance = _FakeInstance(in_flight)
    assert (
        schedules.no_posthog_run_in_flight(_FakeScheduleContext(instance)) is expected
    )
    (runs_filter,) = instance.filters
    assert runs_filter.tags == {
        "dagster/schedule_name": schedules.POSTHOG_SCHEDULE_NAME
    }


_TRACKING_LOGS_DEPLOYMENTS = ("xpro", "mitxonline", "mitx")


@pytest.mark.parametrize("deployment", _TRACKING_LOGS_DEPLOYMENTS)
@pytest.mark.parametrize(("in_flight", "expected"), [(True, False), (False, True)])
def test_tracking_logs_schedule_skips_while_a_run_is_in_flight(
    deployment: str,
    in_flight: bool,  # noqa: FBT001
    expected: bool,  # noqa: FBT001
) -> None:
    from data_loading.defs.ingestion import schedules  # noqa: PLC0415

    instance = _FakeInstance(in_flight)
    schedule = schedules.tracking_logs_ingest_schedules[deployment]
    # ScheduleDefinition keeps should_execute only as this private attribute.
    should_execute = schedule._should_execute
    assert should_execute(_FakeScheduleContext(instance)) is expected
    (runs_filter,) = instance.filters
    assert runs_filter.tags == {
        "dagster/schedule_name": f"{deployment}_tracking_logs_ingest_schedule"
    }


def test_tracking_logs_schedules_start_an_hour_apart() -> None:
    """Xpro keeps the times it had before mitxonline and mitx were added."""
    from data_loading.defs.ingestion import schedules  # noqa: PLC0415

    assert {
        deployment: schedule.cron_schedule
        for deployment, schedule in schedules.tracking_logs_ingest_schedules.items()
    } == {
        "xpro": "50 0-23/12 * * *",
        "mitxonline": "50 1-23/12 * * *",
        "mitx": "50 2-23/12 * * *",
    }


@pytest.mark.parametrize("deployment", _TRACKING_LOGS_DEPLOYMENTS)
@pytest.mark.parametrize(
    ("environment", "status"),
    [
        ("qa", dg.DefaultScheduleStatus.RUNNING),
        ("production", dg.DefaultScheduleStatus.RUNNING),
        ("dev", dg.DefaultScheduleStatus.STOPPED),
    ],
)
def test_tracking_logs_schedule_runs_where_its_bucket_is_its_own(
    deployment: str, environment: str, status: dg.DefaultScheduleStatus
) -> None:
    with _repository_for(environment) as repo:
        schedule = repo.get_schedule_def(f"{deployment}_tracking_logs_ingest_schedule")
        assert schedule.default_status == status


def test_posthog_schedule_runs_get_the_memory_limit_and_runtime_cap() -> None:
    """The run pod must get the 16Gi limit a single large hour needs.

    Parsed the way K8sRunLauncher parses run tags, so a malformed or dropped tag
    fails here rather than as an OOMKilled run.
    """
    from dagster_k8s.job import get_user_defined_k8s_config  # noqa: PLC0415
    from data_loading.defs.ingestion import schedules  # noqa: PLC0415

    tags = schedules.posthog_events_ingest_schedule.tags
    container = get_user_defined_k8s_config(tags).container_config
    assert container["resources"]["limits"]["memory"] == "16Gi"
    assert tags["dagster/max_runtime"] == str(6 * 60 * 60)


@pytest.mark.parametrize("deployment", _TRACKING_LOGS_DEPLOYMENTS)
def test_tracking_logs_runs_get_the_memory_limit_and_runtime_cap(
    deployment: str,
) -> None:
    from dagster_k8s.job import get_user_defined_k8s_config  # noqa: PLC0415
    from data_loading.defs.ingestion import schedules  # noqa: PLC0415

    tags = schedules.tracking_logs_ingest_schedules[deployment].tags
    container = get_user_defined_k8s_config(tags).container_config
    assert container["resources"]["limits"]["memory"] == "16Gi"
    assert container["resources"]["requests"]["memory"] == "2Gi"
    assert tags["dagster/max_runtime"] == str(11 * 60 * 60)


def test_google_ads_schedule_selects_every_google_ads_api_table() -> None:
    schedule = _REPO.get_schedule_def("google_ads_ingest_daily_schedule")
    scheduled = {
        key.to_user_string()
        for key in _REPO.get_job(schedule.job_name).asset_layer.selected_asset_keys
    }
    google_ads_keys = {
        key.to_user_string()
        for key in _REPO.assets_defs_by_key
        if key.path[-1].startswith("raw__google_ads__api__")
    }
    assert "ol_warehouse_raw_data/raw__google_ads__api__change_events" in scheduled
    assert scheduled == google_ads_keys


@pytest.mark.parametrize("table", ["change_events", "keyword_daily"])
def test_google_ads_tables_have_a_freshness_check(table: str) -> None:
    key = dg.AssetKey(["ol_warehouse_raw_data", f"raw__google_ads__api__{table}"])
    assert any(
        check_key.asset_key == key for check_key in _REPO.asset_checks_defs_by_key
    )


def _course_xml_blocks_schedule_keys(repo: Any) -> set[str]:
    schedule = repo.get_schedule_def("course_xml_blocks_ingest_daily_schedule")
    return {
        key.to_user_string()
        for key in repo.get_job(schedule.job_name).asset_layer.selected_asset_keys
    }


@pytest.mark.parametrize(
    ("environment", "status"),
    [
        ("qa", dg.DefaultScheduleStatus.RUNNING),
        ("production", dg.DefaultScheduleStatus.RUNNING),
        ("dev", dg.DefaultScheduleStatus.STOPPED),
    ],
)
def test_course_xml_blocks_schedule_runs_in_qa_without_edxorg(
    environment: str, status: dg.DefaultScheduleStatus
) -> None:
    """edX.org's tables are `qa: mirror` and live only in production's zone."""
    with _repository_for(environment) as repo:
        schedule = repo.get_schedule_def("course_xml_blocks_ingest_daily_schedule")
        assert schedule.default_status == status
        scheduled = _course_xml_blocks_schedule_keys(repo)
    edxorg = {
        key for key in scheduled if key.split("/")[-1].startswith("raw__edxorg__")
    }
    assert "ol_warehouse_raw_data/raw__openedx__s3__course_xml_blocks" in scheduled
    if environment == "qa":
        assert not edxorg
    else:
        assert edxorg == {
            "ol_warehouse_raw_data/raw__edxorg__s3__course_xml_blocks",
            "ol_warehouse_raw_data/raw__edxorg__s3__course_structure_blocks",
        }


def test_code_location_builds_under_the_qa_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """QA's code server runs with DLT_PROFILE=qa, and dlt sources are built at
    import. A source that refuses the qa profile (an edX.org course table)
    would stop the whole location from loading, which the dev-profile builds
    above cannot see. Database hosts come from the Pulumi stack in QA, so the
    connection URL is stubbed; nothing connects either way.
    """
    monkeypatch.setattr(
        database,
        "_connection_url",
        lambda spec, profile: URL.create(spec.drivername, host="qa.invalid"),  # noqa: ARG005
    )
    with _repository_for("qa", profile="qa") as repo:
        keys = {key.to_user_string() for key in repo.assets_defs_by_key}
    assert "ol_warehouse_raw_data/raw__openedx__s3__course_xml_blocks" in keys
    assert not {
        key
        for key in keys
        if key.split("/")[-1].startswith("raw__edxorg__s3__course_")
        and key.split("/")[-1].endswith("_blocks")
    }


@pytest.mark.parametrize(
    "schedule_name",
    [
        "posthog_events_ingest_hourly_schedule",
        "edxorg_program_reports_ingest_daily_schedule",
    ],
)
@pytest.mark.parametrize("environment", ["dev", "qa", "production"])
def test_production_landing_zone_schedules_are_not_defined_in_qa(
    environment: str, schedule_name: str
) -> None:
    """posthog/s3 is `qa: omit` and edxorg/program_reports `qa: mirror`; both
    read production's landing zone, and a hand-started QA PostHog schedule
    failed hourly.
    """
    with _repository_for(environment) as repo:
        names = {schedule.name for schedule in repo.schedule_defs}
    assert (schedule_name in names) == (environment != "qa")

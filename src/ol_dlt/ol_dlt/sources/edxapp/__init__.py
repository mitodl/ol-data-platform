"""Open edX edxapp database ingestion via dlt, one spec per deployment.

Loads each Open edX deployment's edxapp MariaDB database into QA raw (RFC
12711), the way ``mitxonline_app`` loads MITx Online's app database. QA had no
current snapshot for most of these tables (qa_observation 2026-10-28: 0 of 68
mitxonline, 0 of 70 xpro, 8 of 74 mitx), while dim_user, tfact_enrollment and
afact_learner_courserun_progress declare the units as QA branches. Production
stays on each deployment's
Airbyte connections until the unit's dlt cutover.

Data flow:
    <deployment> edxapp MariaDB  ->  raw__<deployment>__openedx__mysql__<table>

Scope is the tables each inventory unit (``<deployment>/mysql``) marks
``modeled: true``: 61 for mitx, 54 for xpro, 56 for mitxonline.
``coursewarehistoryextended_studentmodulehistoryextended`` lives in the
``edxapp_csmh`` database on the same server; Vault's ``readonly`` MariaDB role
can SELECT on every database, so one connection reads both.

Every table is loaded with ``write_disposition="replace"`` and no cursor, as
``mitxonline_app`` does. That is affordable in QA only: production's history
tables run to 114M rows (mitx ``courseware_studentmodulehistory``, Athena
2026-11-10), so the production cutover needs a cursor for those.

``auth_user.password`` is excluded: a Django password hash that no dbt model
selects.
"""

from functools import partial
from typing import Any

from ol_dlt.database import (
    MYSQL_DRIVER,
    DatabaseSourceSpec,
    DatabaseTable,
    build_database_source,
    pipeline_for,
)

MYSQL_PORT = 3306

# The modeled tables every deployment has.
_COMMON_TABLES = (
    "assessment_assessment",
    "assessment_assessmentfeedback",
    "assessment_assessmentfeedback_assessments",
    "assessment_assessmentfeedback_options",
    "assessment_assessmentfeedbackoption",
    "assessment_assessmentpart",
    "assessment_criterion",
    "assessment_criterionoption",
    "assessment_peerworkflow",
    "assessment_peerworkflowitem",
    "assessment_rubric",
    "assessment_studenttrainingworkflow",
    "assessment_studenttrainingworkflowitem",
    "assessment_trainingexample",
    "assessment_trainingexample_options_selected",
    "auth_user",
    "auth_userprofile",
    "certificates_generatedcertificate",
    "completion_blockcompletion",
    "course_groups_cohortmembership",
    "course_groups_courseusergroup",
    "courseware_studentmodule",
    "coursewarehistoryextended_studentmodulehistoryextended",
    "credit_creditcourse",
    "credit_crediteligibility",
    "django_comment_client_role",
    "django_comment_client_role_users",
    "django_content_type",
    "edxval_coursevideo",
    "edxval_video",
    "forum_abuseflagger",
    "forum_comment",
    "forum_commentthread",
    "forum_historicalabuseflagger",
    "forum_mongocontent",
    "forum_uservote",
    "grades_persistentcoursegrade",
    "grades_persistentsubsectiongrade",
    "grades_persistentsubsectiongradeoverride",
    "organizations_organization",
    "organizations_organizationcourse",
    "student_anonymoususerid",
    "student_courseaccessrole",
    "student_courseenrollment",
    "student_languageproficiency",
    "submissions_score",
    "submissions_scoresummary",
    "submissions_studentitem",
    "submissions_submission",
    "teams_courseteam",
    "teams_courseteammembership",
    "user_api_usercoursetag",
    "workflow_assessmentworkflow",
    "workflow_assessmentworkflowstep",
)

# The modeled tables only some deployments have.
_DEPLOYMENT_TABLES = {
    "mitx": (
        "assessment_aiclassifier",
        "assessment_aiclassifierset",
        "assessment_aigradingworkflow",
        "assessment_aitrainingworkflow",
        "assessment_aitrainingworkflow_training_examples",
        "course_overviews_courseoverview",
        "courseware_studentmodulehistory",
    ),
    "xpro": (),
    "mitxonline": ("bulk_email_optout", "grades_visibleblocks"),
}

# Tables outside the edxapp database, by the database they live in.
_TABLE_SCHEMAS = {
    "coursewarehistoryextended_studentmodulehistoryextended": "edxapp_csmh",
}

# Django password hash; no dbt model selects it.
_EXCLUDED_COLUMNS = {"auth_user": ("password",)}


def _table(name: str) -> DatabaseTable:
    return DatabaseTable(
        name=name,
        primary_key="id",
        excluded_columns=_EXCLUDED_COLUMNS.get(name, ()),
        db_schema=_TABLE_SCHEMAS.get(name),
    )


def _spec(deployment: str) -> DatabaseSourceSpec:
    return DatabaseSourceSpec(
        name=f"{deployment}_edxapp",
        raw_table_prefix=f"raw__{deployment}__openedx__mysql__",
        database="edxapp",
        vault_mount=f"mariadb-{deployment}",
        tables=tuple(
            _table(name)
            for name in sorted(_COMMON_TABLES + _DEPLOYMENT_TABLES[deployment])
        ),
        # MySQL has no schema below the database named in the URL.
        db_schema=None,
        port=MYSQL_PORT,
        drivername=MYSQL_DRIVER,
    )


EDXAPP_SPECS: dict[str, DatabaseSourceSpec] = {
    deployment: _spec(deployment) for deployment in _DEPLOYMENT_TABLES
}
EDXAPP_PIPELINES = {
    deployment: pipeline_for(spec) for deployment, spec in EDXAPP_SPECS.items()
}


def build_source(deployment: str, tables: list[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate one deployment's edxapp source (entrypoint for Dagster)."""
    return build_database_source(EDXAPP_SPECS[deployment], tables=tables)


def source_builder(deployment: str) -> partial[Any]:
    """Return a no-argument builder for one deployment's source."""
    return partial(build_source, deployment)

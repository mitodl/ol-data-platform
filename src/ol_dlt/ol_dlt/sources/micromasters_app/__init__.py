"""MicroMasters application-database ingestion via dlt.

Loads the MicroMasters app database into QA raw (RFC 12711), the way
``mitxonline_app`` loads MITx Online's. QA's copy was a single Airbyte extract
from 2024-08-28, converted in place to Iceberg on 2026-09-08, and nothing has
loaded it since. Production stays on the Airbyte connection ``MicroMasters
Production App DB → S3 Data Lake`` until the unit's dlt cutover.

Data flow:
    MicroMasters RDS Postgres  ->  raw__micromasters__app__postgres__<table>

Scope is the 28 tables the inventory unit ``micromasters/app_postgres`` marks
``modeled: true``; the production connection syncs 39.

Every table is loaded with ``write_disposition="replace"`` and no cursor, for
the reasons ``mitxonline_app`` gives. A replace load has one row per key, and
the inventory resolves the QA dedup column to ``_dlt_load_id``.

Two credential columns are excluded: ``auth_user.password`` (a Django password
hash) and ``social_auth_usersocialauth.extra_data`` (the OAuth access and
refresh tokens MicroMasters calls edX.org and MITx Online with on the user's
behalf). No dbt model selects either.
"""

from typing import Any

from ol_dlt.database import (
    DatabaseSourceSpec,
    DatabaseTable,
    build_database_source,
    pipeline_for,
)

MICROMASTERS_APP_SPEC = DatabaseSourceSpec(
    name="micromasters_app",
    raw_table_prefix="raw__micromasters__app__postgres__",
    database="micromasters",
    vault_mount="postgres-micromasters",
    tables=(
        DatabaseTable(
            name="auth_user", primary_key="id", excluded_columns=("password",)
        ),
        DatabaseTable(name="cms_programpage", primary_key="page_ptr_id"),
        DatabaseTable(name="courses_course", primary_key="id"),
        DatabaseTable(name="courses_courserun", primary_key="id"),
        DatabaseTable(name="courses_electivecourse", primary_key="id"),
        DatabaseTable(name="courses_electivesset", primary_key="id"),
        DatabaseTable(name="courses_program", primary_key="id"),
        DatabaseTable(name="dashboard_programenrollment", primary_key="id"),
        DatabaseTable(name="django_content_type", primary_key="id"),
        DatabaseTable(name="ecommerce_coupon", primary_key="id"),
        DatabaseTable(name="ecommerce_couponinvoice", primary_key="id"),
        DatabaseTable(name="ecommerce_line", primary_key="id"),
        DatabaseTable(name="ecommerce_order", primary_key="id"),
        DatabaseTable(name="ecommerce_receipt", primary_key="id"),
        DatabaseTable(name="ecommerce_redeemedcoupon", primary_key="id"),
        DatabaseTable(name="ecommerce_usercoupon", primary_key="id"),
        DatabaseTable(name="exams_examrun", primary_key="id"),
        DatabaseTable(name="grades_combinedfinalgrade", primary_key="id"),
        DatabaseTable(name="grades_finalgrade", primary_key="id"),
        DatabaseTable(name="grades_micromasterscoursecertificate", primary_key="id"),
        DatabaseTable(name="grades_micromastersprogramcertificate", primary_key="id"),
        DatabaseTable(name="grades_proctoredexamgrade", primary_key="id"),
        DatabaseTable(name="profiles_education", primary_key="id"),
        DatabaseTable(name="profiles_employment", primary_key="id"),
        DatabaseTable(name="profiles_profile", primary_key="id"),
        DatabaseTable(
            name="social_auth_usersocialauth",
            primary_key="id",
            excluded_columns=("extra_data",),
        ),
        DatabaseTable(name="wagtailcore_page", primary_key="id"),
        DatabaseTable(name="wagtailimages_image", primary_key="id"),
    ),
)

micromasters_app_pipeline = pipeline_for(MICROMASTERS_APP_SPEC)


def build_source(tables: list[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate the MicroMasters app source (uniform entrypoint for Dagster)."""
    return build_database_source(MICROMASTERS_APP_SPEC, tables=tables)

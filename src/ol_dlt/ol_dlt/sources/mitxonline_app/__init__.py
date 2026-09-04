"""MITx Online application-database ingestion via dlt.

Replaces the Airbyte connection ``MITx Online Production App DB → S3 Data Lake``
(RFC 12319 §6.5, RFC 12711 step 8). Scope is the 64 tables that connection
declares, which is also what the inventory unit ``mitxonline/app_postgres``
records.

Data flow:
    MITx Online RDS Postgres  ->  raw__mitxonline__app__postgres__<table>

The database read is aligned with the deployment: QA Dagster reads the QA
MITx Online database, production reads production, and local development reads
the ``mitxonline`` database in the local-dev CloudNativePG cluster (see
``ol_dlt.database``). That alignment is the point of the migration for RFC
12711: the B2B dimensional models key on Keycloak organization UUIDs
(``users_user.global_id``, ``b2b_organizationpage``), so a QA build has to read
the QA app database or nothing joins.

Every table is loaded with ``write_disposition="replace"``; none declares a
``cursor_column``. Airbyte replicated 36 of these incrementally by ``xmin``,
which dlt has no practical equivalent for (INGESTION_INVENTORY_SPEC.md §3.4),
so each of them needed either a replacement cursor or a decision to re-read it
whole. They are all re-read whole, for three measured reasons:

    Affordable.  The whole unit is 16.5M rows / 1.01 GB across 64 tables at the
        current Iceberg snapshot (production Glue, 2026-08-31). The largest
        table, ``openedx_openedxuser``, is 2.7M rows / 126 MB.
    Not safely keyable.  39 tables carry ``updated_on``, but that is Django's
        ``auto_now=True``, which fires on ``Model.save()`` and NOT on
        ``queryset.update()``. Nobody has audited MITx Online's bulk-update
        paths, and `ol-dbt inventory cursors` reports a candidate column, never
        an approval -- see the caveat in ``ol_dbt_cli/lib/cursor_audit.py``. A
        cursor that misses an edit yields a load that looks healthy and drifts.
    Deletes matter here.  Replace is the only disposition that propagates a
        source delete. Unlinking an organization from a contract, or retiring a
        contract, has to disappear from ``dim_organization`` /
        ``dim_contract``; a ``merge`` load would leave the row behind forever.

Incrementality is a per-table optimization to revisit once the bulk-update
paths are audited. Run ``ol-dbt inventory cursors --unit mitxonline/app_postgres
--attention-only`` for the current shortlist; the tables that would actually pay
for the audit are ``openedx_openedxuser``, ``courses_courserunenrollment`` and
``courses_courserungrade``, which are 55% of the unit's rows between them.

The Wagtail page subclasses (``b2b_contractpage``, ``b2b_organizationpage``,
``cms_coursepage``, ...) cannot be incrementalised at all, structurally: they
are multi-table-inheritance children joined to ``wagtailcore_page`` on
``page_ptr_id`` and carry no timestamp of their own.

``users_user.password`` is excluded. It is a Django PBKDF2 hash, it is landing
in the production warehouse today, and no dbt model selects it -- only the
source YAML declares it, and that declaration goes at cutover.

Run standalone against local-dev (port-forward the CNPG cluster first):
    kubectl port-forward -n local-infra svc/local-pg-rw 5432:5432
    DLT_PROFILE=dev python -m ol_dlt.sources.mitxonline_app
"""

from typing import Any

from ol_dlt.database import (
    DatabaseSourceSpec,
    DatabaseTable,
    build_database_source,
    pipeline_for,
)

MITXONLINE_APP_SPEC = DatabaseSourceSpec(
    name="mitxonline_app",
    raw_table_prefix="raw__mitxonline__app__postgres__",
    database="mitxonline",
    vault_mount="postgres-mitxonline",
    tables=(
        # --- B2B: organizations, contracts, and their attachments -----------
        # The reason this source exists first. dim_organization and
        # dim_contract read these, and their keys are realm-scoped.
        DatabaseTable(name="b2b_contractpage", primary_key="page_ptr_id"),
        DatabaseTable(
            name="b2b_discountcontractattachmentredemption", primary_key="id"
        ),
        DatabaseTable(name="b2b_organizationindexpage", primary_key="page_ptr_id"),
        DatabaseTable(name="b2b_organizationpage", primary_key="page_ptr_id"),
        DatabaseTable(name="b2b_userorganization", primary_key="id"),
        # --- CMS: Wagtail page subclasses -----------------------------------
        DatabaseTable(name="cms_certificatepage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_courseindexpage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_coursepage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_coursepage_topics", primary_key="id"),
        DatabaseTable(name="cms_instructorpage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_instructorpagelink", primary_key="id"),
        DatabaseTable(name="cms_programpage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_signatoryindexpage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_signatorypage", primary_key="page_ptr_id"),
        # --- courses: catalog, runs, enrollments, grades, certificates ------
        DatabaseTable(name="courses_blockedcountry", primary_key="id"),
        DatabaseTable(name="courses_course", primary_key="id"),
        DatabaseTable(name="courses_course_departments", primary_key="id"),
        DatabaseTable(name="courses_courserun", primary_key="id"),
        DatabaseTable(name="courses_courseruncertificate", primary_key="id"),
        DatabaseTable(name="courses_courserunenrollment", primary_key="id"),
        DatabaseTable(name="courses_courserunenrollmentaudit", primary_key="id"),
        DatabaseTable(name="courses_courserungrade", primary_key="id"),
        DatabaseTable(name="courses_courserungradeaudit", primary_key="id"),
        DatabaseTable(name="courses_coursestopic", primary_key="id"),
        DatabaseTable(name="courses_department", primary_key="id"),
        DatabaseTable(
            name="courses_learnerprogramrecordshare", primary_key="share_uuid"
        ),
        DatabaseTable(name="courses_paidcourserun", primary_key="id"),
        DatabaseTable(name="courses_partnerschool", primary_key="id"),
        DatabaseTable(name="courses_program", primary_key="id"),
        DatabaseTable(name="courses_program_departments", primary_key="id"),
        DatabaseTable(name="courses_programcertificate", primary_key="id"),
        DatabaseTable(name="courses_programenrollment", primary_key="id"),
        DatabaseTable(name="courses_programenrollmentaudit", primary_key="id"),
        DatabaseTable(name="courses_programrequirement", primary_key="id"),
        DatabaseTable(name="courses_programrun", primary_key="id"),
        DatabaseTable(name="courses_relatedprogram", primary_key="id"),
        # --- Django plumbing the models actually resolve against ------------
        DatabaseTable(name="django_content_type", primary_key="id"),
        # --- ecommerce: baskets, orders, discounts, transactions ------------
        DatabaseTable(name="ecommerce_basket", primary_key="id"),
        DatabaseTable(name="ecommerce_basketdiscount", primary_key="id"),
        DatabaseTable(name="ecommerce_basketitem", primary_key="id"),
        DatabaseTable(name="ecommerce_discount", primary_key="id"),
        DatabaseTable(name="ecommerce_discountproduct", primary_key="id"),
        DatabaseTable(name="ecommerce_discountredemption", primary_key="id"),
        DatabaseTable(name="ecommerce_line", primary_key="id"),
        DatabaseTable(name="ecommerce_order", primary_key="id"),
        DatabaseTable(name="ecommerce_product", primary_key="id"),
        DatabaseTable(name="ecommerce_transaction", primary_key="id"),
        DatabaseTable(name="ecommerce_userdiscount", primary_key="id"),
        # --- flexible pricing ------------------------------------------------
        DatabaseTable(name="flexiblepricing_countryincomethreshold", primary_key="id"),
        DatabaseTable(name="flexiblepricing_currencyexchangerate", primary_key="id"),
        DatabaseTable(name="flexiblepricing_flexibleprice", primary_key="id"),
        DatabaseTable(name="flexiblepricing_flexiblepricetier", primary_key="id"),
        DatabaseTable(
            name="flexiblepricing_flexiblepricingrequestsubmission", primary_key="id"
        ),
        # --- Open edX linkage --------------------------------------------------
        DatabaseTable(name="openedx_openedxuser", primary_key="id"),
        # --- django-reversion audit trail ---------------------------------------
        DatabaseTable(name="reversion_revision", primary_key="id"),
        DatabaseTable(name="reversion_version", primary_key="id"),
        # --- users and profiles --------------------------------------------------
        DatabaseTable(name="users_legaladdress", primary_key="id"),
        DatabaseTable(
            name="users_user",
            primary_key="id",
            # Django PBKDF2 password hash. Credential material with no
            # analytical use, on the same footing as Keycloak's `credential`
            # table. It lands in the production warehouse today and nothing
            # reads it; stop carrying it.
            excluded_columns=("password",),
        ),
        DatabaseTable(name="users_user_b2b_contracts", primary_key="id"),
        DatabaseTable(name="users_userprofile", primary_key="id"),
        # --- Wagtail core ----------------------------------------------------------
        DatabaseTable(name="wagtailcore_page", primary_key="id"),
        DatabaseTable(name="wagtailcore_revision", primary_key="id"),
        DatabaseTable(name="wagtailimages_image", primary_key="id"),
        DatabaseTable(name="wagtailusers_userprofile", primary_key="id"),
    ),
)

mitxonline_app_pipeline = pipeline_for(MITXONLINE_APP_SPEC)


def build_source(tables: list[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate the MITx Online app source (uniform entrypoint for Dagster)."""
    return build_database_source(MITXONLINE_APP_SPEC, tables=tables)

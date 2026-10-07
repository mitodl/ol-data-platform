"""xPro application-database ingestion via dlt.

Loads the xPro app database the way ``mitxonline_app`` loads MITx Online's, so
QA raw holds data from the QA xPro app (RFC 12711). 83 ``qa_branches``
declarations name ``xpro/app_postgres``, and before this the QA copy was an
Airbyte load last written in 2025-02 (24 tables) and 2026-06 (159).

Data flow:
    xPro RDS Postgres  ->  raw__xpro__app__postgres__<table>

The database read follows the deployment: QA Dagster reads the QA xPro
database, production reads production, and local development reads the ``xpro``
database in the local-dev CloudNativePG cluster (see ``ol_dlt.database``).

Scope is the 55 tables a dbt model reads: the ones the inventory unit
``xpro/app_postgres`` marks ``modeled: true``. The production Airbyte
connection ``xPro Production App DB → S3 Data Lake`` syncs 178, and the other
123 are deliberately left out. Nothing reads them, and they include
``django_session``, the ``oauth2_provider_*`` token tables and the
``social_auth_*`` tables, which are credential material. A table that gains a
dbt model gets ``modeled: true`` in the unit and a line here, and
``test_xpro_app.py`` fails until both agree.

Every table is loaded with ``write_disposition="replace"``; none declares a
``cursor_column``, for the reasons ``mitxonline_app`` gives: Django's
``auto_now`` does not fire on ``queryset.update()``, the Wagtail page subclasses
carry no timestamp, and replace is the only disposition that propagates a
source delete. It is affordable: the 55 tables were 2,493,174 rows in production
raw on 2026-10-06 (Athena), the largest being ``ecommerce_couponeligibility`` at
1,135,726.

``users_user.password`` is excluded. It is a Django PBKDF2 hash, the Airbyte
load lands it in the warehouse, and no dbt model selects it.

Run standalone against local-dev (port-forward the CNPG cluster first):
    kubectl port-forward -n local-infra svc/local-pg-rw 5432:5432
    DLT_PROFILE=dev python -m ol_dlt.sources.xpro_app
"""

from typing import Any

from ol_dlt.database import (
    DatabaseSourceSpec,
    DatabaseTable,
    build_database_source,
    pipeline_for,
)

XPRO_APP_SPEC = DatabaseSourceSpec(
    name="xpro_app",
    raw_table_prefix="raw__xpro__app__postgres__",
    database="xpro",
    vault_mount="postgres-xpro",
    tables=(
        # --- B2B ecommerce: bulk coupon orders and their receipts --------------
        DatabaseTable(name="b2b_ecommerce_b2bcoupon", primary_key="id"),
        DatabaseTable(name="b2b_ecommerce_b2bcouponaudit", primary_key="id"),
        DatabaseTable(name="b2b_ecommerce_b2bcouponredemption", primary_key="id"),
        DatabaseTable(name="b2b_ecommerce_b2border", primary_key="id"),
        DatabaseTable(name="b2b_ecommerce_b2borderaudit", primary_key="id"),
        DatabaseTable(name="b2b_ecommerce_b2breceipt", primary_key="id"),
        # --- CMS: Wagtail page subclasses --------------------------------------
        DatabaseTable(name="cms_certificatepage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_coursepage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_coursepage_topics", primary_key="id"),
        DatabaseTable(name="cms_coursesinprogrampage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_externalcoursepage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_externalcoursepage_topics", primary_key="id"),
        DatabaseTable(name="cms_externalprogrampage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_facultymemberspage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_programpage", primary_key="page_ptr_id"),
        DatabaseTable(name="cms_signatorypage", primary_key="page_ptr_id"),
        # --- courses: catalog, runs, enrollments, grades, certificates ---------
        DatabaseTable(name="courses_course", primary_key="id"),
        DatabaseTable(name="courses_courserun", primary_key="id"),
        DatabaseTable(name="courses_courseruncertificate", primary_key="id"),
        DatabaseTable(name="courses_courserunenrollment", primary_key="id"),
        DatabaseTable(name="courses_courserungrade", primary_key="id"),
        DatabaseTable(name="courses_coursetopic", primary_key="id"),
        DatabaseTable(name="courses_platform", primary_key="id"),
        DatabaseTable(name="courses_program", primary_key="id"),
        DatabaseTable(name="courses_programcertificate", primary_key="id"),
        DatabaseTable(name="courses_programenrollment", primary_key="id"),
        DatabaseTable(name="courses_programrun", primary_key="id"),
        # --- Django plumbing the models resolve against ------------------------
        DatabaseTable(name="django_content_type", primary_key="id"),
        # --- ecommerce: baskets, orders, coupons, products, receipts -----------
        DatabaseTable(name="ecommerce_basket", primary_key="id"),
        DatabaseTable(name="ecommerce_basketitem", primary_key="id"),
        DatabaseTable(name="ecommerce_bulkcouponassignment", primary_key="id"),
        DatabaseTable(name="ecommerce_company", primary_key="id"),
        DatabaseTable(name="ecommerce_coupon", primary_key="id"),
        DatabaseTable(name="ecommerce_couponeligibility", primary_key="id"),
        DatabaseTable(name="ecommerce_couponpayment", primary_key="id"),
        DatabaseTable(name="ecommerce_couponpaymentversion", primary_key="id"),
        DatabaseTable(name="ecommerce_couponredemption", primary_key="id"),
        DatabaseTable(name="ecommerce_couponselection", primary_key="id"),
        DatabaseTable(name="ecommerce_couponversion", primary_key="id"),
        DatabaseTable(name="ecommerce_courserunselection", primary_key="id"),
        DatabaseTable(name="ecommerce_line", primary_key="id"),
        DatabaseTable(name="ecommerce_linerunselection", primary_key="id"),
        DatabaseTable(name="ecommerce_order", primary_key="id"),
        DatabaseTable(name="ecommerce_orderaudit", primary_key="id"),
        DatabaseTable(name="ecommerce_product", primary_key="id"),
        DatabaseTable(name="ecommerce_productcouponassignment", primary_key="id"),
        DatabaseTable(name="ecommerce_productversion", primary_key="id"),
        DatabaseTable(name="ecommerce_programrunline", primary_key="id"),
        DatabaseTable(name="ecommerce_receipt", primary_key="id"),
        DatabaseTable(name="ecommerce_taxrate", primary_key="id"),
        # --- users and profiles ------------------------------------------------
        DatabaseTable(name="users_legaladdress", primary_key="id"),
        DatabaseTable(name="users_profile", primary_key="id"),
        DatabaseTable(
            name="users_user",
            primary_key="id",
            # Django PBKDF2 password hash. Credential material with no
            # analytical use; no dbt model selects it. Same exclusion as
            # mitxonline_app.
            excluded_columns=("password",),
        ),
        # --- Wagtail core ------------------------------------------------------
        DatabaseTable(name="wagtailcore_page", primary_key="id"),
        DatabaseTable(name="wagtailimages_image", primary_key="id"),
    ),
)

xpro_app_pipeline = pipeline_for(XPRO_APP_SPEC)


def build_source(tables: list[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate the xPro app source (uniform entrypoint for Dagster)."""
    return build_database_source(XPRO_APP_SPEC, tables=tables)

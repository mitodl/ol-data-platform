"""xPro-specific tests for the xPro application-database source.

The spec-vs-inventory, primary-key, cursor, naming and password checks it
shares with the other QA-first app databases are in
test_app_database_sources.py.
"""

from ol_dlt.sources import xpro_app

# Unmodeled tables the production Airbyte connection syncs that hold sessions
# and tokens. Named here so that marking one `modeled: true` cannot quietly
# pull it into the load.
CREDENTIAL_TABLE_PREFIXES = (
    "courseware_openedxapiauth",
    "django_session",
    "oauth2_provider_",
    "oauth_toolkit_extensions_",
    "sheets_googleapiauth",
    "social_auth_",
    "users_changeemailrequest",
    "wagtailcore_pageviewrestriction",
)


def test_no_credential_table_is_loaded() -> None:
    assert not [
        table.name
        for table in xpro_app.XPRO_APP_SPEC.tables
        if table.name.startswith(CREDENTIAL_TABLE_PREFIXES)
    ]


def test_pipeline_targets_the_xpro_app_prefix() -> None:
    assert xpro_app.xpro_app_pipeline.pipeline_name == "xpro_app"

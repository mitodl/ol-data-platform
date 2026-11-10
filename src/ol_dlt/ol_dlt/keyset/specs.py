"""Connection specs for the databases whose tables get a keyset.

Keyed by inventory unit (``deployment/layer``), the same key ``tables.json``
uses. The units with a dlt loader reuse its spec. The rest have none yet, so
their specs carry no tables: a keyset needs only the database, the raw table
prefix and the Vault mount.

Names, databases and mounts follow ol-infrastructure (each application's
``OLVaultPostgresDatabaseConfig`` / ``OLVaultMysqlDatabaseConfig``). The host
comes from ``<NAME>_DB_HOST`` in the data_loading deployment. ol-infrastructure
sets it only for the units with a dlt loader, so the micromasters and ovs specs
here have no host yet.
"""

from ol_dlt.database import DatabaseSourceSpec
from ol_dlt.sources.edxapp import EDXAPP_SPECS
from ol_dlt.sources.learn_ai_app import LEARN_AI_APP_SPEC
from ol_dlt.sources.mitlearn_app import MITLEARN_APP_SPEC
from ol_dlt.sources.mitxonline_app import MITXONLINE_APP_SPEC
from ol_dlt.sources.ocw_studio_app import OCW_STUDIO_APP_SPEC
from ol_dlt.sources.xpro_app import XPRO_APP_SPEC

KEYSET_SPECS: dict[str, DatabaseSourceSpec] = {
    "mitxonline/app_postgres": MITXONLINE_APP_SPEC,
    "xpro/app_postgres": XPRO_APP_SPEC,
    "micromasters/app_postgres": DatabaseSourceSpec(
        name="micromasters_app",
        raw_table_prefix="raw__micromasters__app__postgres__",
        database="micromasters",
        vault_mount="postgres-micromasters",
    ),
    "mitlearn/app_postgres": MITLEARN_APP_SPEC,
    "ovs/app_postgres": DatabaseSourceSpec(
        name="ovs_app",
        raw_table_prefix="raw__ovs__postgres__",
        database="odlvideo",
        vault_mount="postgres-odl-video-service",
    ),
    "ocw/app_postgres": OCW_STUDIO_APP_SPEC,
    "learn_ai/app_postgres": LEARN_AI_APP_SPEC,
    "mitx/mysql": EDXAPP_SPECS["mitx"],
    "xpro/mysql": EDXAPP_SPECS["xpro"],
    "mitxonline/mysql": EDXAPP_SPECS["mitxonline"],
}

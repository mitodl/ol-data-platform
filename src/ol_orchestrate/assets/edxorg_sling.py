from typing import Any

from dagster_sling import SlingResource, sling_assets


def generate_edxorg_raw_table_assets(replication_config: dict[str, Any]):
    @sling_assets(name="edxorg_raw_tables", replication_config=replication_config)
    def edxorg_raw_table_assets(context, sling: SlingResource):
        yield from sling.replicate(context=context)
        for row in sling.stream_raw_logs():
            context.log.info(row)

    return edxorg_raw_table_assets

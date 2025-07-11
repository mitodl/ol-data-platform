from dagster import (
    Definitions,
)
from dagster_aws.s3 import S3Resource

from ol_orchestrate.assets.edxorg_sling import generate_edxorg_raw_table_assets
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.sling_config import (
    create_sling_resource,
    edxorg_replication_config,
)

if DAGSTER_ENV == "production":
    source_bucket_name = "ol-data-lake-landing-zone-production"
    glue_warehouse = "s3://ol-data-lake-raw-production/"
    glue_namespace = "ol_data_lake_raw_production"
else:
    source_bucket_name = "ol-data-lake-landing-zone-qa"
    glue_warehouse = "s3://ol-data-lake-raw-qa/"
    glue_namespace = "ol_data_lake_raw_qa"

source_bucket_prefix = (
    f"s3://{source_bucket_name}/edxorg-raw-data/edxorg/raw_data/db_table"
)

s3_resource = S3Resource()

sling_resource = create_sling_resource(source_bucket_name, glue_warehouse)
replication_config = edxorg_replication_config(source_bucket_prefix, glue_namespace)
assets = generate_edxorg_raw_table_assets(replication_config)

edxorg_raw_table_defs = Definitions(
    assets=[assets],
    resources={
        "sling": sling_resource,
        "s3": s3_resource,
    },
)

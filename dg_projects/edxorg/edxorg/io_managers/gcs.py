from dagster import (
    ConfigurableIOManager,
    DagsterEventType,
    EventRecordsFilter,
    InputContext,
    OutputContext,
)
from upath import UPath

from ol_orchestrate.lib.dagster_types.files import DagsterPath
from ol_orchestrate.resources.gcp_gcs import GCSConnection


class GCSFileIOManager(ConfigurableIOManager):
    gcs_bucket: str | None = None
    gcs_prefix: str | None = None
    gcs: GCSConnection = None

    def load_input(self, context: InputContext) -> DagsterPath:
        if context.has_asset_key:
            asset_dep = context.instance.get_event_records(
                event_records_filter=EventRecordsFilter(
                    asset_key=context.asset_key,
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                    asset_partitions=[context.partition_key],
                ),
                limit=1,
            )[0]

            asset_path = UPath(asset_dep.asset_materialization.metadata["path"].value)
            bucket, blob = asset_path.path.split("/", 1)
        else:
            bucket = self.gcs_bucket
            blob = f"{self.gcs_prefix}/{context.get_identifier()}"

        gcs_bucket = self.gcs.client.get_bucket(bucket)
        gcs_blob = gcs_bucket.blob(blob)
        gcs_blob.download_to_filename(context.partition_key)
        return DagsterPath(context.partition_key)

    def handle_output(self, context: OutputContext, obj: DagsterPath) -> None: ...

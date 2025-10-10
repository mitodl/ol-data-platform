from dagster import (
    ConfigurableIOManager,
    DagsterEventType,
    EventRecordsFilter,
    InputContext,
    OutputContext,
)
from ol_orchestrate.lib.dagster_types.files import DagsterPath
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from upath import UPath


class GCSFileIOManager(ConfigurableIOManager):
    gcs_bucket: str | None = None
    gcs_prefix: str | None = None
    gcs: GCSConnection | None = None

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
            path_parts = asset_path.path.split("/", 1)
            bucket: str = path_parts[0]
            blob: str = path_parts[1] if len(path_parts) > 1 else ""
        else:
            if not self.gcs_bucket:
                msg = "gcs_bucket must be set when loading inputs without asset keys"
                raise ValueError(msg)
            bucket = self.gcs_bucket
            identifier = "/".join(context.get_identifier())
            blob = f"{self.gcs_prefix}/{identifier}" if self.gcs_prefix else identifier

        if not self.gcs:
            msg = "GCS connection not initialized"
            raise ValueError(msg)
        gcs_bucket = self.gcs.client.get_bucket(bucket)
        gcs_blob = gcs_bucket.blob(blob)
        gcs_blob.download_to_filename(context.partition_key)
        return DagsterPath(context.partition_key)

    def handle_output(self, context: OutputContext, obj: DagsterPath) -> None: ...

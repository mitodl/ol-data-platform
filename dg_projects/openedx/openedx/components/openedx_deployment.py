"""Component factory for OpenEdX deployment assets, sensors, and resources."""

from typing import Literal

from dagster import (
    AssetsDefinition,
    AutomationConditionSensorDefinition,
    ConfigurableResource,
    DefaultSensorStatus,
    Definitions,
    SensorDefinition,
    SourceAsset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory
from ol_orchestrate.resources.secrets.vault import Vault

from openedx.assets.openedx import (
    build_courseware_source_asset,
    course_structure,
    course_xml,
    extract_courserun_details,
    openedx_course_content_webhook,
)
from openedx.lib.assets_helper import (
    add_prefix_to_asset_keys,
    late_bind_partition_to_asset,
)
from openedx.partitions.openedx import OPENEDX_COURSE_RUN_PARTITIONS
from openedx.sensors.openedx import course_run_sensor


class OpenEdxDeploymentComponent:
    """Create OpenEdX deployment assets, sensors, and resources as a component.

    This component creates a complete set of Dagster definitions for a single OpenEdX
    deployment, including:
    - Assets for course data extraction (courseware, structure, XML, metadata)
    - Sensors for detecting new courses and course version changes
    - Resources for API client configuration

    Args:
        deployment_name: The name of the OpenEdX deployment
            (e.g., "mitx", "mitxonline", "xpro")
        vault: The Vault resource for retrieving credentials

    """

    def __init__(
        self,
        deployment_name: Literal["mitx", "mitxonline", "xpro", "edxorg"],
        vault: Vault,
    ):
        self.deployment_name = deployment_name
        self.vault = vault

    def build_assets(self) -> dict[str, AssetsDefinition | SourceAsset]:
        """Build asset definitions for the deployment.

        Returns:
            Dictionary of asset definitions with deployment-specific prefixes and
            partitions.
        """
        # The courseware source asset takes its key and partitions directly
        # rather than through the prefix/late-bind helpers, which only know how
        # to rewrite an AssetsDefinition.
        courseware_asset = build_courseware_source_asset(
            self.deployment_name,
            OPENEDX_COURSE_RUN_PARTITIONS[self.deployment_name],
        )

        # Create additional assets with deployment prefixes and partitions
        course_structure_asset = late_bind_partition_to_asset(
            add_prefix_to_asset_keys(course_structure, self.deployment_name),
            OPENEDX_COURSE_RUN_PARTITIONS[self.deployment_name],
        )
        course_xml_asset = late_bind_partition_to_asset(
            add_prefix_to_asset_keys(course_xml, self.deployment_name),
            OPENEDX_COURSE_RUN_PARTITIONS[self.deployment_name],
        )

        courserun_detail_asset = late_bind_partition_to_asset(
            add_prefix_to_asset_keys(extract_courserun_details, self.deployment_name),
            OPENEDX_COURSE_RUN_PARTITIONS[self.deployment_name],
        )

        course_content_webhook_asset = late_bind_partition_to_asset(
            add_prefix_to_asset_keys(
                openedx_course_content_webhook, self.deployment_name
            ),
            OPENEDX_COURSE_RUN_PARTITIONS[self.deployment_name],
        )

        return {
            "courseware_asset": courseware_asset,
            "course_structure_asset": course_structure_asset,
            "course_xml_asset": course_xml_asset,
            "courserun_detail_asset": courserun_detail_asset,
            "course_content_webhook_asset": course_content_webhook_asset,
        }

    def build_sensors(
        self, assets: dict[str, AssetsDefinition | SourceAsset]
    ) -> list[SensorDefinition]:
        """Build sensor definitions for the deployment.

        Args:
            assets: The deployment's assets, used to target the sensors.

        Returns:
            List of sensor definitions
        """
        # Access individual assets by their keys
        course_xml_asset = assets["course_xml_asset"]
        course_content_webhook_asset = assets["course_content_webhook_asset"]

        # Discovery only -- this sensor registers partitions and requests no
        # runs, so the selection exists purely to give the definition a target.
        # The courseware source asset is deliberately not in it: a sensor
        # cannot target something it can never materialize.
        courseware_sensor = SensorDefinition(
            name=f"{self.deployment_name}_courseware_sensor",
            description="Query a running Open edX system for a list of course runs.",
            asset_selection=[
                course_xml_asset,
                course_content_webhook_asset,
            ],
            job=None,
            default_status=DefaultSensorStatus.STOPPED,
            minimum_interval_seconds=60 * 60,
            evaluation_fn=course_run_sensor,
        )

        # Drives the whole export graph: it requests the hourly observation of
        # the courseware source asset, and every downstream's
        # upstream_or_code_changes() then reacts to the versions that
        # observation reports.
        automation_sensor = AutomationConditionSensorDefinition(
            f"{self.deployment_name}_openedx_automation_sensor",
            minimum_interval_seconds=300 if DAGSTER_ENV == "dev" else 60 * 60,
            target=list(assets.values()),
        )

        return [
            courseware_sensor,
            automation_sensor,
        ]

    def build_resource(
        self,
    ) -> dict[str, ConfigurableResource[OpenEdxApiClientFactory]]:
        """Build resource definition for the deployment.

        Returns:
            Dictionary with generic "openedx" key mapped to deployment-specific resource.
            This allows assets to use the generic "openedx" key while getting the
            correct deployment-specific resource within this repository.
        """  # noqa: E501
        return {
            "openedx": OpenEdxApiClientFactory(
                deployment=self.deployment_name, vault=self.vault
            )
        }

    def build_definitions(
        self,
        shared_resources: dict[str, ConfigurableResource[OpenEdxApiClientFactory]]
        | None = None,
    ) -> Definitions:
        """Build complete Definitions object for the deployment.

        Args:
            shared_resources: Optional dict of shared resources to include.

        Returns:
            Definitions object containing assets, sensors, and resources.
        """
        assets = self.build_assets()
        sensors = self.build_sensors(assets)
        deployment_resources = self.build_resource()

        # Combine deployment-specific and shared resources
        all_resources = {**deployment_resources}
        if shared_resources:
            all_resources.update(shared_resources)

        return Definitions(
            assets=list(assets.values()),
            sensors=sensors,
            resources=all_resources,
        )

"""Tests for OpenEdxDeploymentComponent's definition wiring."""

import pytest
from dagster import AssetsDefinition, FilesystemIOManager, SourceAsset
from dagster._core.definitions.assets.graph.asset_graph import AssetGraph
from dagster_aws.s3 import S3Resource
from ol_orchestrate.lib.constants import VAULT_ADDRESS
from ol_orchestrate.lib.utils import unauthenticated_vault
from openedx.components.openedx_deployment import OpenEdxDeploymentComponent

DEPLOYMENT = "mitxonline"

# Stand-ins for what definitions.py supplies at the code-location level. The
# tests inspect definitions rather than executing them, so only the keys have
# to line up.
SHARED_RESOURCES = {
    "io_manager": FilesystemIOManager(),
    "s3file_io_manager": FilesystemIOManager(),
    "s3": S3Resource(),
    "learn_api": None,
}


@pytest.fixture
def component() -> OpenEdxDeploymentComponent:
    """Build the component with a Vault resource that is never dereferenced.

    Nothing here touches `.client`; the tests inspect definitions only.
    """
    return OpenEdxDeploymentComponent(
        deployment_name=DEPLOYMENT, vault=unauthenticated_vault(VAULT_ADDRESS)
    )


def _asset_graph(component: OpenEdxDeploymentComponent) -> AssetGraph:
    """Resolve the component's full asset graph, source assets included."""
    return (
        component.build_definitions(shared_resources=SHARED_RESOURCES)
        .get_repository_def()
        .asset_graph
    )


def test_the_courseware_source_asset_has_no_automation_condition(
    component: OpenEdxDeploymentComponent,
) -> None:
    """Nothing may auto-observe courseware per partition.

    An AutomationCondition here is evaluated once per partition, so it asks for
    one whole-deployment outline sweep per course. The observation sensor is the
    only thing that should be driving observation.
    """
    graph = _asset_graph(component)
    courseware_key = next(
        key for key in graph.get_all_asset_keys() if key.path[-1] == "courseware"
    )

    assert graph.get(courseware_key).automation_condition is None


def test_the_observation_sensor_is_wired_hourly(
    component: OpenEdxDeploymentComponent,
) -> None:
    """The sweep runs once an hour, and it is the only thing that observes."""
    sensors = component.build_sensors(component.build_assets())
    observation_sensor = next(
        sensor for sensor in sensors if sensor.name.endswith("_observation_sensor")
    )

    assert observation_sensor.minimum_interval_seconds == 60 * 60


def test_build_assets_returns_both_asset_types(
    component: OpenEdxDeploymentComponent,
) -> None:
    """Courseware is a SourceAsset while everything downstream is materializable."""
    assets = component.build_assets()

    assert isinstance(assets["courseware_asset"], SourceAsset)
    assert all(
        isinstance(asset, AssetsDefinition)
        for name, asset in assets.items()
        if name != "courseware_asset"
    )


def test_the_automation_sensor_targets_the_courseware_source_asset(
    component: OpenEdxDeploymentComponent,
) -> None:
    """A SourceAsset in the sensor target is supported, and load-bearing.

    The automation sensor is the only thing that requests the courseware
    observation, and the observation is the only thing that gives the rest of
    the graph a data version to react to. Filtering the target down to
    AssetsDefinitions -- the obvious-looking way to keep the list homogeneous --
    would silently stop every course export in the deployment.
    """
    assets = component.build_assets()
    sensors = component.build_sensors(assets)
    automation_sensor = next(
        sensor for sensor in sensors if sensor.name.endswith("_automation_sensor")
    )

    graph = _asset_graph(component)
    targeted = automation_sensor.asset_selection.resolve(graph)

    assert assets["courseware_asset"].key in targeted
    assert targeted == graph.get_all_asset_keys(), "nothing dropped from the target"


def test_the_discovery_sensor_does_not_target_the_source_asset(
    component: OpenEdxDeploymentComponent,
) -> None:
    """course_run_sensor can only select assets it could materialize."""
    assets = component.build_assets()
    sensors = component.build_sensors(assets)
    courseware_sensor = next(
        sensor for sensor in sensors if sensor.name.endswith("_courseware_sensor")
    )

    targeted = courseware_sensor.asset_selection.resolve(_asset_graph(component))

    assert assets["courseware_asset"].key not in targeted

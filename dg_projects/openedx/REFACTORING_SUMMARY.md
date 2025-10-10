# OpenEdX Component Refactoring Summary

## Overview
Refactored the OpenEdX code location to use a custom component pattern for managing multiple OpenEdX deployments (mitx, mitxonline, xpro). This reduces code duplication and provides a reusable pattern for creating deployment-specific assets, sensors, and resources.

## Changes Made

### 1. Created Component Module Structure
**Location:** `/dg_projects/openedx/openedx/components/`

Created a new `components` module with:
- `__init__.py` - Module initialization and exports
- `openedx_deployment.py` - The `OpenEdxDeploymentComponent` factory class

### 2. Component Implementation
**File:** `openedx/components/openedx_deployment.py`

Created `OpenEdxDeploymentComponent` class with the following methods:

- `__init__(deployment_name, vault)` - Initialize component for a specific deployment
- `build_assets()` - Generate 4 assets per deployment:
  - `openedx_live_courseware` (with deployment-specific partitioning)
  - `course_structure`
  - `course_xml`
  - `extract_courserun_details`

- `build_sensors(assets)` - Generate 3 sensors per deployment:
  - `course_run_sensor` (shared sensor)
  - `{deployment}_course_version_sensor` (asset-bound)
  - `{deployment}_openedx_automation_sensor` (automation condition)

- `build_resource()` - Generate deployment-specific OpenEdX API client resource
- `build_definitions()` - Optionally build a complete Definitions object for a single deployment

### 3. Refactored definitions.py
**File:** `openedx/definitions.py`

**Before (lines 167-209, ~40 lines):**
```python
# Build assets for each deployment
all_deployment_assets = []
all_deployment_sensors = []
all_deployment_resources = {}

for deployment_name in OPENEDX_DEPLOYMENTS:
    course_version_asset = late_bind_partition_to_asset(
        add_prefix_to_asset_keys(openedx_live_courseware, deployment_name),
        OPENEDX_COURSE_RUN_PARTITIONS[deployment_name],
    )
    deployment_assets = [
        course_version_asset,
        add_prefix_to_asset_keys(course_structure, deployment_name),
        add_prefix_to_asset_keys(course_xml, deployment_name),
        add_prefix_to_asset_keys(extract_courserun_details, deployment_name),
    ]

    asset_bound_course_version_sensor = SensorDefinition(
        name=f"{deployment_name}_course_version_sensor",
        asset_selection=[course_version_asset],
        job=None,
        default_status=DefaultSensorStatus.STOPPED,
        minimum_interval_seconds=60 * 60,
        evaluation_fn=course_version_sensor,
    )

    deployment_sensors = [
        course_run_sensor,
        asset_bound_course_version_sensor,
        AutomationConditionSensorDefinition(
            f"{deployment_name}_openedx_automation_sensor",
            minimum_interval_seconds=300 if DAGSTER_ENV == "dev" else 60 * 60,
            target=deployment_assets,
        ),
    ]

    all_deployment_assets.extend(deployment_assets)
    all_deployment_sensors.extend(deployment_sensors)

    # Add deployment-specific resources
    all_deployment_resources[f"openedx_{deployment_name}"] = OpenEdxApiClientFactory(
        deployment=deployment_name, vault=vault
    )
```

**After (lines 155-173, ~18 lines):**
```python
# Build assets, sensors, and resources for each deployment using the component factory
all_deployment_assets = []
all_deployment_sensors = []
all_deployment_resources = {}

for deployment_name in OPENEDX_DEPLOYMENTS:
    # Create component instance for this deployment
    component = OpenEdxDeploymentComponent(
        deployment_name=deployment_name, vault=vault
    )

    # Build and collect assets, sensors, and resources
    deployment_assets = component.build_assets()
    deployment_sensors = component.build_sensors(deployment_assets)
    deployment_resources = component.build_resource()

    all_deployment_assets.extend(deployment_assets)
    all_deployment_sensors.extend(deployment_sensors)
    all_deployment_resources.update(deployment_resources)
```

**Removed imports** (no longer needed in definitions.py):
- `add_prefix_to_asset_keys`
- `late_bind_partition_to_asset`
- `OPENEDX_COURSE_RUN_PARTITIONS`
- `OpenEdxApiClientFactory`
- Asset imports: `course_structure`, `course_xml`, `extract_courserun_details`, `openedx_live_courseware`
- Sensor imports: `course_run_sensor`, `course_version_sensor`
- Dagster imports: `AutomationConditionSensorDefinition`, `DefaultSensorStatus`, `SensorDefinition`

**Added import:**
- `from openedx.components import OpenEdxDeploymentComponent`

## Benefits

1. **Reduced Code Duplication**: The component encapsulates the pattern used for all three deployments
2. **Improved Maintainability**: Changes to the deployment pattern only need to be made in one place
3. **Better Organization**: Clear separation of concerns with the component in its own module
4. **Reusability**: The component can be easily instantiated for new deployments
5. **Testability**: The component can be tested independently of the definitions module
6. **Cleaner definitions.py**: Reduced from ~40 lines to ~18 lines for the deployment loop

## Verification

All tests passed:
- ✓ Syntax validation
- ✓ Import validation
- ✓ Definitions loading
- ✓ Asset count: 12 assets (4 per deployment × 3 deployments)
- ✓ Sensor count: 9 sensors (3 per deployment × 3 deployments)
- ✓ Resource count: 8 resources (5 base + 3 deployment-specific)
- ✓ Linting: All ruff checks passed

## Preserved Functionality

- `OPENEDX_DEPLOYMENTS` constant is still used to iterate deployments
- All existing asset, sensor, and resource definitions remain unchanged
- Asset keys, partitioning, and automation conditions work identically
- No breaking changes to the public API

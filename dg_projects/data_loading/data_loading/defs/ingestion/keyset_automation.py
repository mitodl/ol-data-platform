"""Run each keyset after every load of its unit, where its keysets are automated.

The keyset assets carry upstream_or_code_changes on their raw tables (see
KeysetDltTranslator), so a load, from this location's dlt source or the
lakehouse location's Airbyte connection, requests the keysets once the load's
run has finished. The raw history snapshots in the lakehouse location run on the
keyset's update in turn (keyset_snapshot_upstream_or_code_changes).

Registered only where the condition is: an asset that carries a condition no
declared sensor covers gets Dagster's synthesized default sensor instead.
"""

import dagster as dg

from data_loading.defs.ingestion.assets import (
    MITXONLINE_APP_KEYSET_AUTOMATED,
    mitxonline_app_keyset_assets,
)

# Loads run every six hours or more, so five minutes adds little to the
# keyset's lag behind the load.
KEYSET_SENSOR_INTERVAL_SECONDS = 5 * 60

mitxonline_app_keyset_sensors = (
    [
        dg.AutomationConditionSensorDefinition(
            "mitxonline_app_keyset_automation_sensor",
            target=dg.AssetSelection.assets(mitxonline_app_keyset_assets),
            minimum_interval_seconds=KEYSET_SENSOR_INTERVAL_SECONDS,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
    ]
    if MITXONLINE_APP_KEYSET_AUTOMATED and mitxonline_app_keyset_assets
    else []
)

defs = dg.Definitions(sensors=mitxonline_app_keyset_sensors)

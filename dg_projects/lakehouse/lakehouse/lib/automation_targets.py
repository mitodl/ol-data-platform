"""What dbt_automation_sensor and ocw_learn_automation_sensor each build.

An asset belongs to one automation sensor, so the two targets must not overlap.
They live here rather than in definitions.py so a test can resolve both against
a synthetic manifest: definitions.py builds them over ``full_dbt_project``, and
importing that needs a parsed manifest, which only the image has.
"""

from dagster import AssetsDefinition, AssetSelection
from dagster_dbt import build_dbt_asset_selection

# The OCW content files, from staging to the two Learn views that read them.
# ocw_content_publish_sensor (data_loading) loads a course a few minutes after
# it is published, but staging was built only by non_airbyte_staging_daily, so
# the publish reached Learn after the next day's 06:00 build and the automation
# tick after it. ocw_learn_automation_sensor builds these on the tick after any
# ocw_content load, sweep or scoped. Every model here is unpartitioned, so a
# tick evaluates five conditions, not one per course.
OCW_LEARN_MODELS = (
    "stg__ocw__s3__course_content",
    "int__ocw__live_courses",
    "int__ocw__content_files",
    "integrations__learn__ocw_courses",
    "integrations__learn__ocw_content_files",
)


def ocw_learn_target(dbt_assets: AssetsDefinition) -> AssetSelection:
    """ocw_learn_automation_sensor's target.

    :param dbt_assets: the Trino dbt project's assets.
    :returns: the OCW_LEARN_MODELS assets.
    :rtype: AssetSelection
    """
    return build_dbt_asset_selection(
        [dbt_assets], dbt_select=" ".join(OCW_LEARN_MODELS)
    )


def dbt_automation_target(dbt_assets: AssetsDefinition) -> AssetSelection:
    """dbt_automation_sensor's target.

    Staging is excluded because the "sync_and_stage_" jobs build it, or the
    non-Airbyte staging schedules for dlt/Dagster sources. Note what that costs
    in production, where staging models DO carry a condition:
    get_default_automation_condition_sensor_target takes every conditioned key
    this selection does not cover and synthesizes
    `default_automation_condition_sensor` over AssetSelection.all() minus this
    one. So staging is automatable there via a sensor no one declared.
    Pre-existing and STOPPED unless started by hand, but the same invisible
    instance state DBT_AUTOMATION_ENVIRONMENTS exists to remove -- see the open
    question recorded beside it. Where automation is off the question does not
    arise: no asset carries a condition, so there is nothing to synthesize over.

    The OCW Learn models are ocw_learn_automation_sensor's, so they are taken out.

    :param dbt_assets: the Trino dbt project's assets.
    :returns: every non-staging dbt model outside OCW_LEARN_MODELS, and the
        Superset datasets.
    :rtype: AssetSelection
    """
    return (
        (
            AssetSelection.assets(dbt_assets)
            - AssetSelection.groups("staging")
            - ocw_learn_target(dbt_assets)
        )
        | AssetSelection.groups("superset_dataset")
        | AssetSelection.groups("superset_starrocks_dataset")
    )

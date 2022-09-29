from dagster import repository
from dagster_airbyte import airbyte_resource
from dagster_dbt import dbt_cli_resource

from ol_orchestrate.jobs.sync_assets_and_run_models import sync_assets_and_run_models

airbyte_resource = airbyte_resource.configured(
    {"host": "airbyte-qa.odl.mit.edu", "port": "443"}
)


@repository
def bootcamps_qa_app_db_to_datalake_raw():
    return [
        sync_assets_and_run_models.to_job(
            name="bootcamps_qa_app_db_to_datalake_raw",
            resource_defs={
                "airbyte": airbyte_resource,
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": "/opt/dbt/models/staging/bootcamps",
                        "profiles_dir": "/opt/dbt",
                    }
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": "d8415e19-9859-480a-830b-18cd2f6c0392"
                        }
                    }
                },
            },
        )
    ]


@repository
def mitxonline_qa_app_db_to_datalake_raw():
    return [
        sync_assets_and_run_models.to_job(
            name="mitxonline_qa_app_db_to_datalake_raw",
            resource_defs={
                "airbyte": airbyte_resource,
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": "/opt/dbt/models/staging/mitxonline",
                        "profiles_dir": "/opt/dbt",
                    }
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": "6e8cdba9-c711-4118-a196-91038cee78a7"
                        }
                    }
                },
            },
        )
    ]


@repository
def mitxpro_qa_app_db_to_datalake_raw():
    return [
        sync_assets_and_run_models.to_job(
            name="mitxpro_qa_app_db_to_datalake_raw",
            resource_defs={
                "airbyte": airbyte_resource,
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": "/opt/dbt/models/staging/mitxpro",
                        "profiles_dir": "/opt/dbt",
                    }
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": "442c98f7-9a51-4f10-945d-82f022f1e1a1"
                        }
                    }
                },
            },
        )
    ]


@repository
def mitx_residential_qa_openedx_db_to_datalake_raw():
    return [
        sync_assets_and_run_models.to_job(
            name="mitx_residential_qa_openedx_db_to_datalake_raw",
            resource_defs={
                "airbyte": airbyte_resource,
                "dbt": dbt_cli_resource.configured(
                    {"project_dir": "/opt/dbt", "profiles_dir": "/opt/dbt"}
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": "358a7580-97f8-419f-8636-a3d08f0e30ab"
                        }
                    }
                },
            },
        )
    ]


@repository
def ocw_studio_qa_to_datalake_raw():
    return [
        sync_assets_and_run_models.to_job(
            name="ocw_studio_qa_to_datalake_raw",
            resource_defs={
                "airbyte": airbyte_resource,
                "dbt": dbt_cli_resource.configured(
                    {"project_dir": "/opt/dbt", "profiles_dir": "/opt/dbt"}
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": "ba60ac15-de5c-4eef-9c38-0a89c2360653"
                        }
                    }
                },
            },
        )
    ]

import os

from dagster import repository
from dagster_airbyte import airbyte_resource
from dagster_dbt import dbt_cli_resource
from requests.auth import HTTPBasicAuth

from ol_orchestrate.jobs.sync_assets_and_run_models import sync_assets_and_run_models

configured_airbyte_resource = airbyte_resource.configured(
    {
         "host": {"env": "DAGSTER_AIRBYTE_HOST"},
         "port": "443",
         "use_https": True,
         "request_additional_params": {
             "auth": HTTPBasicAuth(*os.getenv("DAGSTER_AIRBYTE_AUTH", "").split(":")),
             "verify": False,
         },
    }
)
environment = {
    "qa": {
        "airbyte_resource": configured_airbyte_resource,
        "airbyte_connection": {
            "raw_bootcamps_application_database_tables": "d8415e19-9859-480a-830b-18cd2f6c0392",  # noqa: E501
            "raw_mitxonline_application_database_tables": "6e8cdba9-c711-4118-a196-91038cee78a7",  # noqa: E501
            "raw_mitxonline_openedx_database_tables": "79b5fe95-7b8c-4a94-ba4e-5b8e925cfdd9",  # noqa: E501
            "raw_mitxpro_application_database_tables": "442c98f7-9a51-4f10-945d-82f022f1e1a1",  # noqa: E501
            "raw_mitxpro_openedx_database_tables": "cda953b8-e089-4eb8-8cf5-95419d4b6fb4",  # noqa: E501
            "raw_mitx_residential_openedx_database_tables": "358a7580-97f8-419f-8636-a3d08f0e30ab",  # noqa: E501
            "raw_ocw_studio_application_database_tables": "ba60ac15-de5c-4eef-9c38-0a89c2360653",  # noqa: E501
        },
    },
    "production": {
        "airbyte_resource": configured_airbyte_resource,
    },
}

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")
env_map = environment[dagster_deployment]


@repository
def bootcamps():
    return [
        sync_assets_and_run_models.to_job(
            name="raw_bootcamps_application_database_tables",
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": env_map["airbyte_connection"][
                                "raw_bootcamps_application_database_tables"
                            ]
                        }
                    },
                    "materialize_dbt_model": {
                        "config": {"models_path": "/opt/dbt/models/staging/bootcamps"}
                    },
                },
            },
            resource_defs={
                "airbyte": env_map["airbyte_resource"],
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": "/opt/dbt",
                        "profiles_dir": "/opt/dbt",
                    }
                ),
            },
        )
    ]


@repository
def mitxonline():
    return [
        sync_assets_and_run_models.to_job(
            name="raw_mitxonline_application_database_tables",
            resource_defs={
                "airbyte": env_map["airbyte_resource"],
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": "/opt/dbt",
                        "profiles_dir": "/opt/dbt",
                    }
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": env_map["airbyte_connection"][
                                "raw_mitxonline_application_database_tables"
                            ]
                        }
                    },
                    "materialize_dbt_model": {
                        "config": {"models_path": "/opt/dbt/models/staging/mitxonline"}
                    },
                },
            },
        ),
        sync_assets_and_run_models.to_job(
            name="raw_mitxonline_openedx_database_tables",
            resource_defs={
                "airbyte": env_map["airbyte_resource"],
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": "/opt/dbt",
                        "profiles_dir": "/opt/dbt",
                    }
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": env_map["airbyte_connection"][
                                "raw_mitxonline_openedx_database_tables"
                            ]
                        }
                    },
                    "materialize_dbt_model": {
                        "config": {"models_path": "/opt/dbt/models/staging/mitxonline"}
                    },
                },
            },
        ),
    ]


@repository
def mitxpro():
    return [
        sync_assets_and_run_models.to_job(
            name="raw_mitxpro_application_database_tables",
            resource_defs={
                "airbyte": env_map["airbyte_resource"],
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": "/opt/dbt",
                        "profiles_dir": "/opt/dbt",
                    }
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": env_map["airbyte_connection"][
                                "raw_mitxpro_application_database_tables"
                            ]
                        }
                    },
                    "materialize_dbt_model": {
                        "config": {"models_path": "/opt/dbt/models/staging/mitxpro"}
                    },
                },
            },
        ),
        sync_assets_and_run_models.to_job(
            name="raw_mitxpro_openedx_database_tables",
            resource_defs={
                "airbyte": env_map["airbyte_resource"],
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": "/opt/dbt",
                        "profiles_dir": "/opt/dbt",
                    }
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": env_map["airbyte_connection"][
                                "raw_mitxpro_openedx_database_tables"
                            ]
                        }
                    },
                    "materialize_dbt_model": {
                        "config": {"models_path": "/opt/dbt/models/staging/mitxpro"}
                    },
                },
            },
        ),
    ]


@repository
def mitx_residential():
    return [
        sync_assets_and_run_models.to_job(
            name="raw_mitx_residential_openedx_database_tables",
            resource_defs={
                "airbyte": env_map["airbyte_resource"],
                "dbt": dbt_cli_resource.configured(
                    {"project_dir": "/opt/dbt", "profiles_dir": "/opt/dbt"}
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": env_map["airbyte_connection"][
                                "raw_mitx_residential_openedx_database_tables"
                            ]
                        }
                    },
                    "materialize_dbt_model": {
                        "config": {
                            "models_path": "/opt/dbt/models/staging/mitx_residential"
                        }
                    },
                },
            },
        )
    ]


@repository
def ocw_studio():
    return [
        sync_assets_and_run_models.to_job(
            name="raw_ocw_studio_application_database_tables",
            resource_defs={
                "airbyte": env_map["airbyte_resource"],
                "dbt": dbt_cli_resource.configured(
                    {"project_dir": "/opt/dbt", "profiles_dir": "/opt/dbt"}
                ),
            },
            config={
                "ops": {
                    "sync_airbyte": {
                        "config": {
                            "connection_id": env_map["airbyte_connection"][
                                "raw_ocw_studio_application_database_tables"
                            ]
                        }
                    },
                    "materialize_dbt_model": {
                        "config": {
                            "models_path": "/opt/dbt/models/staging/ocw_studio"
                        }
                    },
                },
            },
        )
    ]

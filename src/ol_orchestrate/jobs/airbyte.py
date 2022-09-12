from dagster import asset, op, job, Out, sensor, resource, DynamicOut, graph, DynamicOutput
from dagster._core.definitions import AssetGroup
from dagster_airbyte import airbyte_resource, airbyte_sync_op, build_airbyte_assets, AirbyteOutput
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project, dbt_run_op
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from typing import List
from collections import Counter

config = load_yaml_config("ol_orchestrate/asset_materialization_pipelines.yaml")

DBT_PROJECT_DIR = config['dbt']['project_dir']
DBT_PROFILES_DIR = config['dbt']['project_profile']

airbyte_resource = airbyte_resource.configured(
        {
            #"host": config['airbyte']['host'],
            #"port": config['airbyte']['port'],
            "host": "localhost",
            "port": "8000"
        }
)


@asset(required_resource_keys={"dbt"})
def run_dbt(context, arg):
    context.resources.dbt.cli("deps")
    dbt_assets = load_assets_from_dbt_project(
        DBT_PROJECT_DIR,
        DBT_PROFILES_DIR,
    )
    dbt_run_op.alias(name="load_dbt_assets")


@job(resource_defs={
     "airbyte": airbyte_resource,
     "dbt": dbt_cli_resource.configured({"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROJECT_DIR})
     })
def airbyte_job():
    for pipeline_config in config['pipelines']:
        connection_id = pipeline_config['airbyte']['connection_id']
        connection_name = pipeline_config['airbyte']['connection_name']
        run_dbt(airbyte_sync_op.configured({"connection_id": connection_id}, name=connection_name)())



# airbyte_assets = build_airbyte_assets(connection_id="06a2ce2b-65c2-423a-a449-e11c638050b1")
# dbt_assets = load_assets_from_dbt_project(DBT_PROJECT_DIR, DBT_PROFILES_DIR, select="staging/mitxonline")

# analytics_assets = AssetGroup(dbt_assets,
#     resource_defs={"airbyte": airbyte_resource, "dbt": dbt_cli_resource.configured({"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR})})

# @op(out={"destination_tables": Out()})
# def run_airbyte_sync(context):
#     connection_id = "06a2ce2b-65c2-423a-a449-e11c638050b1"
#     connection_name = "ol_devops_test_to_lake_formation"
#     airbyte_sync_op.configured({"connection_id": connection_id}, name=connection_name)()


# @op(required_resource_keys={'airbyte'})
# def get_workspaces(context) -> List[str]:
#     airbyte = context.resources.airbyte
#     response = airbyte.make_request('/workspaces/list', data=None)
#     return [
#         workspace['workspaceId']
#         for workspace in response['workspaces']
#     ]


# @op(
#     required_resource_keys={'airbyte'},
#     out=DynamicOut(),
# )
# def get_airbyte_connections(context):
#     airbyte = context.resources.airbyte
#     for pipeline_config in config['pipelines']:
#         connection_id = pipeline_config['airbyte']['connection_id']
#         connection_name = pipeline_config['airbyte']['connection_name']
#         yield DynamicOutput(
#             value=connection_id,
#             mapping_key=connection_id.replace('-', '_'),
#         )


# @op(required_resource_keys={'airbyte'})
# def sync_connection(context, connection_id) -> AirbyteOutput:
#     airbyte = context.resources.airbyte
#     airbyte_output = airbyte.sync_and_poll(
#         connection_id=connection_id,
#         poll_interval=5,
#     )
#     return airbyte_output


# @op
# def collate_results(
#     context,
#     airbyte_outputs: List[AirbyteOutput],
# ) -> Counter:
#     counter = Counter([
#         output.job_details.get('job', {}).get('status')
#         for output in airbyte_outputs
#     ])
#     context.log.info(f'Job statuses: {counter}')
#     return counter


# @graph
# def run_enabled_syncs():
#     airbyte_results = (
#         get_airbyte_connections()
#         .map(sync_connection)
#         .collect()
#     )

#     return collate_results(airbyte_results)

# prod_job = run_enabled_syncs.to_job(resource_defs={"airbyte": airbyte_localhost})

# @job(resource_defs={
#      "airbyte": airbyte_resource,
#      "dbt": dbt_cli_resource.configured({"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROJECT_DIR})
#      })
# def airbyte_job():
#     airbyte_sync_op()

# @sensor(job=airbyte_job)
# def launch_the_job():
#     for pipeline_config in config['pipelines']:
#         yield RunRequest(run_config={"ops": {"airbyte_sync_op": {"config": {"connection_id": pipeline_config['airbyte']['connection_id']}}}})



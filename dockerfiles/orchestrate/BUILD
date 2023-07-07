pex_binary(
    name="dagit-binary",
    dependencies=[
        "//:requirements#dagit",
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
    ],
    shebang="/usr/bin/env python",
    entry_point="dagit.cli:main",
)

pex_binary(
    name="dagster-daemon-binary",
    dependencies=[
        "//:requirements#dagster",
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
    ],
    shebang="/usr/bin/env python",
    entry_point="dagster.daemon.cli:main",
)

docker_image(
    name="dagit",
    source="Dockerfile.dagit",
    dependencies=[
        ":dagit-binary",
        ":dagster-daemon-binary",
        "src/ol_orchestrate:project-config",
    ],
    target_stage="dagit",
    tags=["docker_image"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

docker_image(
    name="dagster-daemon",
    source="Dockerfile.dagit",
    dependencies=[
        ":dagit-binary",
        ":dagster-daemon-binary",
        "src/ol_orchestrate:project-config",
    ],
    target_stage="dagster-daemon",
    tags=["docker_image"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

docker_image(
    name="edx-gcs-courses-pipeline",
    dependencies=["src/ol_orchestrate:pipeline-repositories"],
    source="Dockerfile.basic_pipeline",
    extra_build_args=["PACKAGE_NAME=edx-gcs-courses-pipelines"],
    tags=["data_pipeline", "docker_image"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)


docker_image(
    name="edx-pipeline",
    dependencies=["src/ol_orchestrate:pipeline-repositories"],
    source="Dockerfile.edx_pipeline",
    extra_build_args=["PACKAGE_NAME=open-edx-pipelines"],
    tags=["data_pipeline", "docker_image"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

docker_image(
    name="asset-materialization-pipeline",
    dependencies=[
        "src/ol_orchestrate:pipeline-repositories",
        "src/ol_dbt:dbt_project"
    ],
    source="Dockerfile.dbt_pipeline",
    extra_build_args=["PACKAGE_NAME=asset-materialization-pipelines"],
    tags=["data_pipeline", "docker_image"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

docker_image(
    name="lakehouse-assets-graph",
    dependencies=[
        "src/ol_orchestrate/definitions/lakehouse:ol-data-lakehouse",
        "src/ol_dbt:dbt_project",
    ],
    source="Dockerfile.dbt_pipeline",
    extra_build_args=["PACKAGE_NAME=lakehouse-assets"],
    tags=["data_pipeline", "docker_image", "data_lakehouse"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

docker_image(
    name="tracking-log-reprocessing",
    dependencies=[
        "src/ol_orchestrate/definitions/edx:ol-data-edx",
    ],
    source="Dockerfile.basic_pipeline",
    extra_build_args=["PACKAGE_NAME=tracking-log-reprocessing"],
    tags=["data_pipeline", "docker_image", "data_lakehouse"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

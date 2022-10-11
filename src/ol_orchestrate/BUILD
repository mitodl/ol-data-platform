python_sources()

files(
    name="project-config",
    sources=["dagster.yaml", "workspace.yaml"]
)

python_distribution(
    name="open-edx",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "./repositories/open_edx.py"
    ],
    provides=python_artifact(
        name="dagster-pipeline",
        version="0.5.3",
    ),
)

docker_image(
    name="edx-pipeline",
    dependencies=[
        ":open-edx",
        "src/ol_dbt:dbt_project"
    ],
    source="../../dockerfiles/orchestrate/Dockerfile.user_pipeline",
    extra_build_args=["PACKAGE_NAME=open-edx-pipelines"],
    tags=["data_pipeline", "docker_image"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

python_distribution(
    name="asset-materialization",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "./repositories/asset_materialization.py"
    ],
    provides=python_artifact(
        name="dagster-pipeline",
        version="0.5.3",
    ),
)

docker_image(
    name="asset-materialization-pipeline",
    dependencies=[
        ":asset-materialization",
        "src/ol_dbt:dbt_project"
    ],
    source="../../dockerfiles/orchestrate/Dockerfile.user_pipeline",
    extra_build_args=["PACKAGE_NAME=asset-materialization-pipelines"],
    tags=["data_pipeline", "docker_image"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

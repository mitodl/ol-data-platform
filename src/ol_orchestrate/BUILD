python_sources(name="ol-orchestrate")

files(name="project-config", sources=["dagster.yaml", "workspace.yaml"])

python_distribution(
    name="pipeline-repositories",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "//:requirements#dagster-gcp",
        "//:requirements#dbt-trino",
        "./repositories",
    ],
    provides=python_artifact(
        name="dagster-repositories",
        version="0.6.0",
    ),
)

python_distribution(
    name="ol-data-code-locations",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "//:requirements#dbt-trino",
        "./definitions",
    ],
    provides=python_artifact(
        name="dagster-code-locations",
        version="0.6.0",
    )
)

python_sources(name="ol-orchestrate")

files(name="project-config", sources=["dagster.yaml", "workspace.yaml"])

python_distribution(
    name="pipeline-repositories",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "//:requirements#dagster-gcp",
        "//:requirements#dbt-trino",
        "//:requirements#dbt-duckdb",
        "./repositories",
    ],
    provides=python_artifact(
        name="dagster-repositories",
        version="0.6.1",
    ),
)

python_distribution(
    name="ol-data-lakehouse",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "//:requirements#dbt-trino",
        "//:requirements#dbt-duckdb",
        "./definitions/lakehouse:lakehouse-code-locations",
    ],
    provides=python_artifact(
        name="dagster-lakehouse",
        version="0.6.1",
    )
)

python_distribution(
    name="ol-data-edx",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "//:requirements#dagster-duckdb",
        "./definitions/edx:edx-code-locations",
    ],
    provides=python_artifact(
        name="dagster-edx",
        version="0.6.1",
    )
)

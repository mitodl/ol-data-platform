python_sources(name="lakehouse-code-locations")

python_distribution(
    name="ol-data-lakehouse",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "//:requirements#dbt-trino",
        "//:requirements#dbt-duckdb",
        ":lakehouse-code-locations",
    ],
    provides=python_artifact(
        name="dagster-lakehouse",
        version="0.6.1",
    )
)

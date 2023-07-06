python_sources(name="edx-code-locations")

python_distribution(
    name="ol-data-edx",
    dependencies=[
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
        "//:requirements#dagster-duckdb",
        ":edx-code-locations",
    ],
    provides=python_artifact(
        name="dagster-edx",
        version="0.6.1",
    )
)

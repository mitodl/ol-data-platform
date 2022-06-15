python_sources()

pex_binary(
    name="dagit",
    dependencies=[
        "//:requirements#dagit",
        "//:requirements#dagster-postgres",
    ],
    shebang="/usr/bin/env python",
    entry_point="dagit.cli:main"
)

pex_binary(
    name="dagster-daemon",
    dependencies=[
        "//:requirements#dagster",
        "//:requirements#dagster-postgres",
    ],
    shebang="/usr/bin/env python",
    entry_point="dagster.daemon.cli:main"
)

files(
    name="project-config",
    sources=["dagster.yaml", "workspace.yaml"]
)

python_distribution(
    name="open-edx",
    dependencies=["./repositories/open_edx.py"],
    provides=python_artifact(
        name="open-edx-pipelines",
        version="0.5.2",
    ),
)

docker_image(
    name="edx-pipeline",
    dependencies=[
        ":open-edx",
        "src/ol_dbt:dbt_project"
    ],
    source="../../dockerfiles/orchestrate/Dockerfile.user_pipeline",
)

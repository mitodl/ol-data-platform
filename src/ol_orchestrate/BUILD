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

docker_image(
    name="ol-dagit",
    description="Dagster web service container iamge",
    dependencies=[
        ":dagit"
    ],
    source="dockerfiles/Dockerfile.dagster"
)

docker_image(
    name="ol-dagster-daemon",
    description="Dagster daemon container image for scheduling and run triggering",
    dependencies=[
        ":dagster-daemon"
    ],
    source="dockerfiles/Dockerfile.dagster"
)

python_distribution(
    name="open-edx",
    dependencies=["./repositories/open_edx.py"],
    provides=python_artifact(
        name="open-edx-pipelines",
        version="0.5.2",
    ),
)

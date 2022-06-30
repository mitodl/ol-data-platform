pex_binary(
    name="dagit",
    dependencies=[
        "//:requirements#dagit",
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
    ],
    shebang="/usr/bin/env python",
    entry_point="dagit.cli:main"
)

pex_binary(
    name="dagster-daemon",
    dependencies=[
        "//:requirements#dagster",
        "//:requirements#dagster-postgres",
        "//:requirements#dagster-aws",
    ],
    shebang="/usr/bin/env python",
    entry_point="dagster.daemon.cli:main"
)

docker_image(
    name="dagit",
    source="Dockerfile.dagit",
    dependencies=[
        ":dagit",
        ":dagster-daemon",
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
        ":dagit",
        ":dagster-daemon",
        "src/ol_orchestrate:project-config",
    ],
    target_stage="dagster-daemon",
    tags=["docker_image"],
    image_tags=["{build_args.DAGSTER_VERSION}", "latest"],
)

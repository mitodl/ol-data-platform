from pathlib import Path

from invoke import task
from poetry.factory import Factory
from vdist.builder import Builder
from vdist.source import git  # , directory


@task
def package_pipeline(context):
    poetry = Factory().create_poetry(cwd=Path(__file__))
    builder = Builder()
    app_name = 'ol_data_pipelines'
    builder.add_build(
        name=app_name,
        app=app_name,
        version=poetry.package.version.text,
        # source=directory(str(Path(__file__).parent)),
        source=git(
            uri=poetry.package.urls['Repository'],
            branch='main'
        ),
        profile='debian',
        python_version='3.8.5',
        build_deps=[
            'libffi-dev',
            'build-essential',
            'zlib1g-dev',
            'libssl-dev',
            'libmariadbclient-dev'
        ],
        compile_python=True,
        runtime_deps=[
            'mongo-tools'
        ],
        fpm_args=(
            '--maintainer=odl-devops@mit.edu '
            '--after-install=buildprofiles/after_install.sh '
            '--template-scripts '
            'workspace.yaml=/etc/dagster/workspace.yaml'
        )
    )
    builder.get_available_profiles()
    builder.create_build_folder_tree()
    builder.populate_build_folder_tree()
    builder.run_build()

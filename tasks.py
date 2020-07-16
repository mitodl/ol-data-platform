import sys
from pathlib import Path

from invoke import task
from poetry.factory import Factory
from vdist.builder import Builder
from vdist.source import git


@task
def package_pipeline(context):
    poetry = Factory().create_poetry(cwd=Path(__file__))
    builder = Builder()
    app_name = 'ol_data_pipelines'
    builder.add_build(
        name=app_name,
        app=app_name,
        version=poetry.package.version.text,
        source=git(
            uri=poetry.package.urls['Repository'],
            branch='main'
        ),
        profile='debian',
        python_version='3.8.4',
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
            '--config-files=workspace.yaml '
            '--maintainer=odl-devops@mit.edu '
            '--after-install=buildprofiles/after_install.sh '
            '--template-scripts'
        )
    )
    builder.get_available_profiles()
    builder.create_build_folder_tree()
    builder.populate_build_folder_tree()
    builder.run_build()

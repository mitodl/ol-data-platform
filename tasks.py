import sys
from pathlib import Path

from invoke import task
from poetry.factory import Factory
from vdist.builder import Builder, build_package
from vdist.configuration import Configuration
from vdist.source import git


@task
def package_pipeline(context):
    poetry = Factory().create_poetry(cwd=Path(__file__))
    locker = poetry.locker
    repository = locker.locked_repository()
    poetry.package.to_dependency()
    builder = Builder()
    builder.add_build(
        name='OL Data Pipelines',
        app='ol_data_pipelines',
        version=poetry.package.version.text,
        source=git(
            uri=poetry.package.urls['Repository']
        ),
        profile='debian',
        python_version='.'.join([str(v) for v in sys.version_info[:3]]),
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
        ]
    )
    builder.get_available_profiles()
    builder.create_build_folder_tree()
    builder.populate_build_folder_tree()
    builder.run_build()

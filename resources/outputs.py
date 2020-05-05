# -*- coding: utf-8 -*-

import os
from datetime import datetime, date
from pathlib import Path

from dagster import Field, InitResourceContext
from dagster import Path as DagsterPath
from dagster import String, resource

from typing import Text

class ResultsDir:

    def __init__(self, root_dir: Text=None):
        if root_dir is None:
            self.root_dir = Path(os.getcwd())
        else:
            self.root_dir = Path(root_dir)
        self.dir_name = 'results'

    def create_dir(self):
        try:
            os.mkdir(self.path)
        except FileExistsError:
            pass

    @property
    def path(self) -> Path:
        return Path(os.path.join(self.root_dir, self.dir_name))

class DailyResultsDir(ResultsDir):

    def __init__(self, root_dir: Text=None, date_format: Text='%Y-%m-%d', date_override: date=None):
        super().__init__(root_dir)
        if date_override:
            dir_date = datetime.strptime(date_override, date_format)
        else:
            dir_date = datetime.utcnow()
        self.dir_name = dir_date.strftime(date_format)


@resource(
    config={
        'outputs_root_dir': Field(
            DagsterPath,
            default_value='',
            is_required=False,
            description=('Base directory used for creating a results folder. Should be configured to allow writing '
                         'by the Dagster/Dagit user')
        ),
        'outputs_directory_date_format': Field(
            String,
            default_value='%Y-%m-%d',
            is_required=False,
            description='Format string for structuring the name of the daily outputs directory'
        ),
        'outputs_directory_date_override': Field(
            String,
            default_value='',
            is_required=False,
            description=('Specified date object to override the default of using the current date. Intended only for '
                         'purposes of backfill operations.')
        )
    }
)
def daily_dir(resource_context: InitResourceContext):
    results_dir = DailyResultsDir(
        root_dir=resource_context.resource_config['outputs_root_dir'],
        date_format=resource_context.resource_config['outputs_directory_date_format'],
        date_override=resource_context.resource_config['outputs_directory_date_override']
    )
    results_dir.create_dir()
    yield results_dir

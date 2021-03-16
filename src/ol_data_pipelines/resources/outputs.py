import os
import shutil
from datetime import datetime
from typing import Generator

from dagster import Field, InitResourceContext, String, resource

from ol_data_pipelines.lib.dagster_types import DagsterPath


class ResultsDir:
    def __init__(self, root_dir: str = None):
        if root_dir is None:
            self.root_dir = DagsterPath(os.getcwd())
        else:
            self.root_dir = DagsterPath(root_dir)
        self.dir_name = "results"

    def create_dir(self):
        try:
            os.makedirs(self.path)
        except FileExistsError:
            return

    def clean_dir(self):
        shutil.rmtree(self.path)

    @property
    def path(self) -> DagsterPath:
        return DagsterPath(os.path.join(self.root_dir, self.dir_name))

    @property
    def absolute_path(self) -> str:
        return str(self.path)


class DailyResultsDir(ResultsDir):
    def __init__(
        self,
        root_dir: str = None,
        date_format: str = "%Y-%m-%d",  # noqa: WPS323
        date_override: str = None,
    ):
        """Instantiate a results directory that defaults to being named according to the current date.

        :param root_dir: The base directory within which the results directory will be created
        :type root_dir: str

        :param date_format: The format string for specifying how the date will be represented in the directory name
        :type date_format: str

        :param date_override: A string representing an override of the date to be used for the generated directory.
            Primarily used for cases where a backfill process needs to occur.
        :type date_override: str
        """
        super().__init__(root_dir)
        if date_override:
            dir_date = datetime.strptime(date_override, date_format)
        else:
            dir_date = datetime.utcnow()
        self.dir_name = dir_date.strftime(date_format)


@resource(
    config_schema={
        "outputs_root_dir": Field(
            String,
            default_value="",
            is_required=False,
            description=(
                "Base directory used for creating a results folder. Should be configured to allow writing "
                "by the Dagster/Dagit user"
            ),
        ),
        "outputs_directory_date_format": Field(
            String,
            default_value="%Y-%m-%d",  # noqa: WPS323
            is_required=False,
            description="Format string for structuring the name of the daily outputs directory",
        ),
        "outputs_directory_date_override": Field(
            String,
            default_value="",
            is_required=False,
            description=(
                "Specified date object to override the default of using the current date. Intended only for "
                "purposes of backfill operations."
            ),
        ),
    }
)
def daily_dir(
    resource_context: InitResourceContext,
) -> Generator[DailyResultsDir, None, None]:
    """Create a resource definition for a daily results directory.

    :param resource_context: The Dagster context for configuring the resource instance
    :type resource_context: InitResourceContext

    :yield: An instance of a daily results directory
    """
    run_dir = os.path.join(
        os.getcwd() or resource_context.resource_config["outputs_root_dir"],
        resource_context.pipeline_run.run_id,
    )
    results_dir = DailyResultsDir(
        root_dir=run_dir,
        date_format=resource_context.resource_config["outputs_directory_date_format"],
        date_override=resource_context.resource_config[
            "outputs_directory_date_override"
        ],
    )
    results_dir.create_dir()
    yield results_dir

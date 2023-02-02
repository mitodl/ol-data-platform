import os
import shutil
from collections.abc import Generator  # noqa: TCH003
from datetime import datetime
from typing import Optional

from dagster import Field, InitResourceContext, String, resource

from ol_orchestrate.lib.dagster_types.files import DagsterPath


class ResultsDir:
    def __init__(self, root_dir: Optional[str] = None):
        if root_dir is None:
            self.root_dir = DagsterPath(os.getcwd())  # noqa: PTH109
        else:
            self.root_dir = DagsterPath(root_dir)
        self.dir_name = "results"

    def create_dir(self):
        try:
            os.makedirs(self.path)  # noqa: PTH103
        except FileExistsError:
            return

    def clean_dir(self):
        shutil.rmtree(self.path)

    @property
    def path(self) -> DagsterPath:
        return DagsterPath(os.path.join(self.root_dir, self.dir_name))  # noqa: PTH118

    @property
    def absolute_path(self) -> str:
        return str(self.path)


class DailyResultsDir(ResultsDir):
    def __init__(
        self,
        root_dir: Optional[str] = None,
        date_format: str = "%Y-%m-%d",
        date_override: Optional[str] = None,
    ):
        """Instantiate a results directory that defaults to being named according to the current date.  # noqa: E501

        :param root_dir: The base directory within which the results directory will be created  # noqa: E501
        :type root_dir: str

        :param date_format: The format string for specifying how the date will be represented in the directory name  # noqa: E501
        :type date_format: str

        :param date_override: A string representing an override of the date to be used for the generated directory.  # noqa: E501
            Primarily used for cases where a backfill process needs to occur.
        :type date_override: str
        """  # noqa: E501
        super().__init__(root_dir)
        if date_override:
            dir_date = datetime.strptime(date_override, date_format)  # noqa: DTZ007
        else:
            dir_date = datetime.utcnow()  # noqa: DTZ003
        self.dir_name = dir_date.strftime(date_format)


@resource(
    config_schema={
        "outputs_root_dir": Field(
            String,
            default_value="",
            is_required=False,
            description=(
                "Base directory used for creating a results folder. Should be configured to allow writing "  # noqa: E501
                "by the Dagster/Dagit user"
            ),
        ),
        "outputs_directory_date_format": Field(
            String,
            default_value="%Y-%m-%d",
            is_required=False,
            description="Format string for structuring the name of the daily outputs directory",  # noqa: E501
        ),
        "outputs_directory_date_override": Field(
            String,
            default_value="",
            is_required=False,
            description=(
                "Specified date object to override the default of using the current date. Intended only for "  # noqa: E501
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
    run_dir = os.path.join(  # noqa: PTH118
        os.getcwd()  # noqa: PTH109
        or resource_context.resource_config["outputs_root_dir"],
        resource_context.dagster_run.run_id,
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

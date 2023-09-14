import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field, validator

from ol_orchestrate.lib.dagster_types.files import DagsterPath


class BaseResultsDir(ConfigurableResource):
    outputs_root_dir: str = Field(
        default=None,
        description=(
            "Base directory used for creating a results folder. Should be configured to"
            " allow writing by the Dagster user"
        ),
    )

    @validator("outputs_root_dir", always=True)
    def validate_outputs_root_dir(cls, value: str) -> str:  # noqa: N805
        if value is None:
            value = Path.cwd()
        return str(value)

    def create_dir(self):
        self.path.mkdir(parents=True, exist_ok=True)

    def clean_dir(self):
        shutil.rmtree(self.path)

    @property
    def path(self) -> DagsterPath:
        return DagsterPath(Path(self.outputs_root_dir).joinpath(self.dir_name))

    @property
    def absolute_path(self) -> str:
        return str(self.path)

    def setup_for_execution(self, context: InitResourceContext) -> None:  # noqa: ARG002
        self.create_dir()

    def teardown_for_execution(
        self, context: InitResourceContext  # noqa: ARG002
    ) -> None:
        self.clean_dir()


class SimpleResultsDir(BaseResultsDir):
    dir_name: str = "results"


class DailyResultsDir(BaseResultsDir):
    date_format: str = Field(
        default="%Y-%m-%d",
        description="Format string for structuring the name of the daily outputs "
        "directory",
    )
    date_override: Optional[str] = Field(
        default=None,
        description=(
            "Specified date object to override the default of using the current"
            " date. Intended only for purposes of backfill operations."
        ),
    )

    @property
    def dir_name(self) -> str:
        if self.date_override is not None:
            dir_date = datetime.strptime(  # noqa: DTZ007
                self.date_override, self.date_format
            )
        else:
            dir_date = datetime.utcnow()  # noqa: DTZ003
        return dir_date.strftime(self.date_format)

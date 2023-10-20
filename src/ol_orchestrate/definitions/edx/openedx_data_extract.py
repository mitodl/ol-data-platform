from typing import Literal

from dagster import Definitions

from ol_orchestrate.jobs.open_edx import extract_open_edx_data_to_ol_data_platform
from ol_orchestrate.resources.openedx import OpenEdxApiClient
from ol_orchestrate.resources.outputs import DailyResultsDir


def open_edx_extract_job_config(
    open_edx_deployment: Literal["mitx", "mitxonline", "xpro"]  # noqa: ARG001
):
    return {
        "resources": {
            "openedx": {
                "config": {
                    "client_id": "",
                    "client_secret": "",
                    "lms_url": "",
                    "studio_url": "",
                    "token_type": "JWT",
                }
            }
        }
    }


openedx_data_extracts = Definitions(
    resources={
        "openedx": OpenEdxApiClient.configure_at_launch(),
        "results_dir": DailyResultsDir.configure_at_launch(),
    },
    jobs=[extract_open_edx_data_to_ol_data_platform.to_job()],
)

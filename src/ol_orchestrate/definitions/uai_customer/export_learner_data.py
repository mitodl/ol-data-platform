from dagster import (
    Definitions,
)

from ol_orchestrate.assets.uai_customer import (
    export_learner_enrollment_data,
    get_customer_course_mapping,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket

vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)


export_learner_data = Definitions(
    assets=[get_customer_course_mapping, export_learner_enrollment_data],
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": default_file_object_io_manager(
            dagster_env=DAGSTER_ENV,
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
    },
)

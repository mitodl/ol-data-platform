import os
from typing import Literal

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
)
from dagster_aws.s3.resources import s3_resource
from ol_orchestrate.jobs.retrieve_edx_exports import retrieve_edx_exports
from ol_orchestrate.resources.gcp_gcs import gcp_gcs_resource
from ol_orchestrate.resources.outputs import results_dir

dagster_env: Literal["dev", "qa", "production"] = os.environ.get(  # type: ignore
    "DAGSTER_ENVIRONMENT", "dev"
)


def weekly_edx_exports_config(
    edx_exports_directory,
    edx_irx_exports_bucket,
    tracking_log_bucket,
    course_exports_bucket,
):
    return {
        "ops": {
            "download_edx_data_exports": {
                "config": {"edx_exports_bucket": edx_exports_directory}
            },
            "upload_edx_data_exports": {
                "config": {
                    "edx_irx_exports_bucket": edx_irx_exports_bucket,
                    "tracking_log_bucket": tracking_log_bucket,
                    "course_exports_bucket": course_exports_bucket,
                }
            },
        }
    }


s3_job_def = retrieve_edx_exports.to_job(
    name="retrieve_edx_exports",
    config=weekly_edx_exports_config(
        "simeon-mitx-pipeline-main",
        "ol-devops-sandbox/pipeline-storage/",
        "ol-devops-sandbox/pipeline-storage/",
        "ol-devops-sandbox/pipeline-storage/",
    ),
)

irx_export_schedule = ScheduleDefinition(
    name="weekly_edx_sync",
    cron_schedule="0 4 * * 6",
    job=s3_job_def,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)


retrieve_edx_exports = Definitions(
    resources={
        "gcp_gcs": gcp_gcs_resource.configured(
            {
                "project_id": "ol-data-platform",
                "private_key_id": "4de005937aecbf0081fa5b6666714ffa1f7eb3c7",
                "client_email": "ol-data-platform-qa@ol-data-platform.iam.gserviceaccount.com",
                "client_id": "1.1099066448965992e+20",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/ol-data-platform-qa%40ol-data-platform.iam.gserviceaccount.com",
                "private_key": """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1UXdt8MhcxP1I
dffSrnyxzY7LEwFOgIRcEyqzmtyUsC0Fvgx6gGnmZ7AR7nDVo5Hbdctxhkd/I1VN
bMbF/pbdawFYCmUGcWujFFx9IaRPLqpjOi6QVPXjeqpyo5rDdb2VIWSnp3ZfvqOm
o9HhmerX2Tx1U7pQAARRyqzNIelVLVLQN/t47BchLdPu23tIKzHaHl1bIImr7Mgy
rhtcIar2EI9ziPg/Jn5d6mKRUkMcMY5+cehR0C7z5/dFAViMU9YGfl5ipYVEZm6x
RzSmaGijTc/vAScek0xfWQpYKJiDYSzMj4uBhw87VvFmBRmzWTMAF1VZ469DbRg3
POkal3qRAgMBAAECggEAF8RWedBCizp9Etb3xaN+j9XofkD+/lnFP8Z5ZfKKMGlJ
dYO8YgBcIUn9H9SX8MEQmOo/YSGfVJB+mXgFNCXTg7GHixFNvcL3P3inSPW0cnak
+f3B5cRlOMy28/DzE+TmlVo7tegzEYjNLcYyeDZzPJjqnlx7wwnFhy0gaoEj8ziV
sZPct6DRj35Ah/Zxw0TBgOq8IVJUYj/BmCfGvosErZIZuGFo6kCujPDlmVWM+hbx
2O+aVvkEnBKr0OIGxVh+AatKK+PENr2lQ9b/g+aACiTs7aEunl2K4p9jEbTIHQxe
Gg5NUI7VaTIDAOMPETu5nNcibZ1ccEr7DeHkVRykswKBgQDa+qkZHbgKHy17u5qT
SeEYrBGr2fP52AzOIHhUczGle6Sc9oIkE8g2gTN4qIKo2+xgdkL9CCxDworamf28
YZdiceN4JMyIo29MxjyrgcbkMLJe1H2IS8cOQTnb/9G4wFfCLokXyEFcn6PhYcBk
y6RddFU9Jlnu1n/e+ZLC9csXgwKBgQDT+NrhmL1wk5EZXBh9ftFedefyuJL6w44X
Ecv1enW20UgLRJiHYkmXGQCQaXwEpij184SpUoKZdm6+AWzJK1bWL2EbOKY4BUrf
D7uCbRcNQcAjiBY3RAjN7eWOay14yrIvtEn394zzhwUziq5ZrXAefsmkE6hxxxre
Dkxvc9g1WwKBgEMbVf7wcJoJe/LTR0ej+GuLL88ud+o1H5d7s+SNeMVx4ryHftJ3
jX/GkHOFkKUW4JWfKGBKxOXvFIZxcqTsc2wj4sXK4TNugolQpv8YQk9j7QXKsL24
G0RvEMAu4aJwr3Q+tpynLgCom5xSTJeXIMPTBtw6iIz9ByrZ6PP+R0LpAoGBAM6X
4qyxcyE/kdHHos7dRS3teI1mO0pQQjJV1BWCryvpOXBSAN8ielrbsWMOjCLz10Qv
fYKRulvbw/9H4EEJDm9eaiRfyBEdh2V0Lerma+stxBhdUFm442PhkzSFXSI3XAeG
jfkxupy3YehkJ52bnoT6SYiy6B9MUPFPukt9+qG1AoGAWR/azOWopEfTfhwPFnYa
zNyFu0kT9EjoJ8erYVNsS6veiXbA8Zj9mZjhmEo7Vb1bwGnfkD6M/own5FpeHgVI
LGWaMH5Lz59+hoeJ2jw/UeDpEEAq0d8z3n6E7daDvyxqU8ryp5AK0TF2W8RcZ4wm
xdwI+kCl/GZ8S1PPL0+Sd9w=
-----END PRIVATE KEY-----""",
            }
        ),
        "s3": s3_resource,
        "exports_dir": results_dir,
    },
    jobs=[s3_job_def],
    schedules=[irx_export_schedule],
)

import os
from typing import Literal, get_args

DagsterEnv = Literal["dev", "ci", "qa", "production"]

VALID_DAGSTER_ENVS: tuple[str, ...] = get_args(DagsterEnv)

_dagster_env = os.environ.get("DAGSTER_ENVIRONMENT", "dev")

# DAGSTER_ENV is typed as a Literal but sourced from the environment, so
# nothing enforces the contract at runtime. Every code location then indexes
# environment-keyed dicts with it (Vault addresses, Trino and StarRocks hosts,
# Slack channels), and a typo would surface as a bare `KeyError: 'productoin'`
# from whichever dict happened to be read first. Validating here -- the single
# point where the value is derived -- turns that into one clear message, for
# every code location at once.
if _dagster_env not in VALID_DAGSTER_ENVS:
    msg = (
        f"DAGSTER_ENVIRONMENT is set to {_dagster_env!r}, which is not a "
        f"recognized environment. Expected one of: "
        f"{', '.join(VALID_DAGSTER_ENVS)}."
    )
    raise ValueError(msg)

DAGSTER_ENV: DagsterEnv = _dagster_env  # type: ignore[assignment]

if DAGSTER_ENV == "dev":
    VAULT_ADDRESS = os.getenv("VAULT_ADDR", "https://vault-qa.odl.mit.edu")
else:
    VAULT_ADDRESS = os.getenv("VAULT_ADDR", f"https://vault-{DAGSTER_ENV}.odl.mit.edu")

OPENEDX_DEPLOYMENTS = ["mitx", "mitxonline", "xpro"]

EXPORT_TYPE_COMMON_CARTRIDGE = "common_cartridge"
EXPORT_TYPE_ZIP = "zip"
EXPORT_TYPE_QTI = "qti"

EXPORT_TYPE_EXTENSIONS = {
    EXPORT_TYPE_COMMON_CARTRIDGE: "imscc",
    EXPORT_TYPE_ZIP: EXPORT_TYPE_ZIP,  # Extension matches the type
    EXPORT_TYPE_QTI: EXPORT_TYPE_QTI,  # Extension matches the type
}

# List of all edxorg db_table names that are materialized
EDXORG_DB_TABLES = [
    "assessment_assessment",
    "assessment_assessmentfeedback",
    "assessment_assessmentfeedback_assessments",
    "assessment_assessmentfeedback_options",
    "assessment_assessmentfeedbackoption",
    "assessment_assessmentpart",
    "assessment_criterion",
    "assessment_criterionoption",
    "assessment_peerworkflow",
    "assessment_peerworkflowitem",
    "assessment_rubric",
    "assessment_studenttrainingworkflow",
    "assessment_studenttrainingworkflowitem",
    "assessment_trainingexample",
    "assessment_trainingexample_options_selected",
    "auth_user",
    "auth_userprofile",
    "certificates_generatedcertificate",
    "course",
    "course_groups_cohortmembership",
    "course_structure",
    "courseware_studentmodule",
    "credit_crediteligibility",
    "django_comment_client_role_users",
    "examples",
    "grades_persistentcoursegrade",
    "grades_persistentsubsectiongrade",
    "student_anonymoususerid",
    "student_courseaccessrole",
    "student_courseenrollment",
    "student_languageproficiency",
    "submissions_score",
    "submissions_scoresummary",
    "submissions_studentitem",
    "submissions_submission",
    "teams",
    "teams_membership",
    "user_api_usercoursetag",
    "user_id_map",
    "validate",
    "wiki_article",
    "wiki_articlerevision",
    "workflow_assessmentworkflow",
    "workflow_assessmentworkflowstep",
]

import os
from typing import Literal

DAGSTER_ENV: Literal["dev", "ci", "qa", "production"] = os.environ.get(  # type: ignore[assignment]
    "DAGSTER_ENVIRONMENT", "dev"
)

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

"""
Dummy asset definitions for upstream edxorg_archive outputs.

These assets are actually materialized by the process_edxorg_archive_bundle op
in the edxorg code location via AssetMaterialization events. Defining them here
as external assets allows Dagster to track dependencies properly.
"""

from dagster import (
    AssetKey,
    AssetSpec,
)
from ol_orchestrate.partitions.edxorg import course_and_source_partitions

from edxorg.assets.edxorg_archive import (
    raw_archive_asset_key,
)

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
    "courseware_studentmodule",
    "credit_crediteligibility",
    "django_comment_client_role_users",
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
    "wiki_article",
    "wiki_articlerevision",
    "workflow_assessmentworkflow",
    "workflow_assessmentworkflowstep",
]

# The edxorg_archive assets are partitioned by course_id and source_system
# We define a minimal partitions definition to match the upstream structure
edxorg_partitions = course_and_source_partitions

# Create external asset specs for all upstream db_table assets
edxorg_db_table_specs = [
    AssetSpec(
        key=AssetKey(["edxorg", "raw_data", "db_table", table]),
        description=(
            f"EdX.org {table} TSV exports from archive processing "
            "(partitioned by course/source)"
        ),
        group_name="edxorg_raw_data",
        partitions_def=edxorg_partitions,
        deps=[raw_archive_asset_key],
        metadata={
            "source": "edxorg_archive",
            "format": "TSV",
            "location": f"s3://ol-data-lake-landing-zone-production/edxorg-raw-data/edxorg/raw_data/db_table/{table}/",
        },
    )
    for table in EDXORG_DB_TABLES
]

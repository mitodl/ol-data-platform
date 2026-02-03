"""
Dagster assets that wrap dlt sources with upstream dependencies.

This module creates consolidated, non-partitioned assets that depend on the
upstream partitioned edxorg_archive assets. Each dlt asset consolidates data
from all prod courses into a single table.
"""

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Definitions,
    multi_asset,
)
from dagster_dlt import DagsterDltResource

from .loads import (
    edxorg_s3_pipeline,
    edxorg_s3_source_instance,
)

# List of all tables that will be created as assets
ALL_TABLES = [
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


# Create multi_asset that depends on all upstream partition combinations
@multi_asset(
    name="edxorg_s3_consolidated_tables",
    outs={
        table: AssetOut(
            key=AssetKey(["edxorg", "tables", table]),
            description=f"Consolidated edX.org {table} data from all prod courses",
        )
        for table in ALL_TABLES
    },
    # Depend on all upstream db_table assets (partitioned by course/source)
    deps=[AssetKey(["edxorg", "raw_data", "db_table", table]) for table in ALL_TABLES],
    # This asset is NOT partitioned - it consolidates all partitions
    partitions_def=None,
    can_subset=True,  # Allow materializing individual tables
    group_name="ingestion",
    compute_kind="dlt",
)
def edxorg_s3_consolidated_tables(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    """
    Load and consolidate EdX.org database tables from S3.

    Depends on upstream edxorg_archive assets (partitioned by course and source).
    Filters to only 'prod' source data and consolidates across all courses into
    non-partitioned Iceberg/Parquet tables.

    The dlt source automatically uses incremental loading based on file modification
    dates, so only new/modified files are processed on each run.
    """
    # Run the dlt pipeline
    # The source is already configured to only read prod files
    yield from dlt.run(
        context=context,
        dlt_source=edxorg_s3_source_instance,
        dlt_pipeline=edxorg_s3_pipeline,
    )


defs = Definitions(
    assets=[edxorg_s3_consolidated_tables],
)

"""Assets for managing instructor onboarding data in the access-forge GitHub repository.

This module pulls email addresses from the combined user course roles
dbt model and pushes them to a private GitHub repository for instructor
access management.
"""

from io import StringIO
from typing import Any

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Output,
    asset,
)
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.resources.github import GithubApiClientFactory

ACCESS_FORGE_REPO = "mitodl/access-forge"
ACCESS_FORGE_USERS_PATH = "users/production/users.csv"


@asset(
    name="instructor_onboarding_user_list",
    group_name="instructor_onboarding",
    # Built by the dbt project in lakehouse; the key is what
    # get_asset_key_for_model resolved it to there.
    deps=[AssetKey(["intermediate", "int__combined__user_course_roles"])],
    description="Generates CSV file with user emails for access-forge repository",
)
def generate_instructor_onboarding_user_list(
    context: AssetExecutionContext,
) -> Output[str]:
    """Pull unique email addresses from user course roles and prepare for GitHub upload.

    This asset reads the combined user course roles dbt model, extracts unique email
    addresses, and generates a CSV string formatted for the access-forge repository.

    The output CSV has three columns:
    - email: User's email address (from user_email field)
    - role: Set to 'ol-instructor' for all users
    - sent_invite: Set to 1 for all users

    Args:
        context: Dagster execution context

    Returns:
        Output containing CSV string content formatted for access-forge repo
    """
    # Fetch the dbt model data from Glue
    int__combined__user_course_roles = get_dbt_model_as_dataframe(
        database_name="ol_warehouse_production_intermediate",
        table_name="int__combined__user_course_roles",
    )

    # Select unique email addresses and filter out nulls
    user_data = (
        int__combined__user_course_roles.select(["user_email", "platform"])
        .filter(pl.col("user_email").is_not_null())
        .filter(pl.col("user_email").str.ends_with("@mit.edu"))
        .filter(pl.col("platform").is_in(["xPro", "MITx Online"]))
        .with_columns(email=pl.col("user_email").str.to_lowercase())
        .select(["email"])
        .unique()
        .sort("email")
    )

    # Add role and sent_invite columns with fixed values
    user_data = user_data.with_columns(
        [pl.lit("ol-instructor").alias("role"), pl.lit(1).alias("sent_invite")]
    )

    # Reorder columns: email, role, sent_invite
    user_data = user_data.select(["email", "role", "sent_invite"])

    # Collect the LazyFrame before operations that need materialization
    user_data_collected = user_data.collect()

    # Convert to CSV string using StringIO (no file I/O)
    csv_buffer = StringIO()
    user_data_collected.write_csv(csv_buffer)
    csv_content = csv_buffer.getvalue()

    context.log.info(
        "Generated CSV content with %s unique users", len(user_data_collected)
    )

    return Output(
        value=csv_content,
        metadata={
            "num_users": len(user_data_collected),
        },
    )


@asset(
    name="update_access_forge_repo",
    group_name="instructor_onboarding",
    ins={
        "instructor_onboarding_user_list": AssetIn(
            key=AssetKey(["instructor_onboarding_user_list"])
        )
    },
    description="Commits the generated user list to the access-forge default branch",
)
def update_access_forge_repo(
    context: AssetExecutionContext,
    github_api: GithubApiClientFactory,
    instructor_onboarding_user_list: str,
) -> Output[dict[str, Any]]:
    """Merge new users into the access-forge user list on its default branch.

    Existing entries are kept as they are; users not already listed are
    appended. The commit goes straight to the default branch rather than
    through a pull request, which relies on the data platform's GitHub App being
    a bypass actor on the org rulesets that require a reviewed PR there. If that
    stops being true the commit fails, rather than falling back to opening PRs
    nobody merges.

    :param context: Dagster execution context
    :param github_api: GitHub API client factory resource
    :param instructor_onboarding_user_list: CSV content from the upstream asset
    :returns: Metadata about the commit, or about the skip when nothing changed
    :rtype: Output[dict[str, Any]]
    """
    repo = github_api.get_client(token_permissions={"contents": "write"}).get_repo(
        ACCESS_FORGE_REPO
    )
    branch = repo.default_branch
    contents = repo.get_contents(ACCESS_FORGE_USERS_PATH, ref=branch)
    existing_csv = contents.decoded_content.decode("utf-8")

    existing_df = pl.read_csv(StringIO(existing_csv))
    new_df = pl.read_csv(StringIO(instructor_onboarding_user_list))
    merged_df = (
        pl.concat([existing_df, new_df])
        .unique(subset=["email"], keep="first")
        .sort("email")
    )
    csv_buffer = StringIO()
    merged_df.write_csv(csv_buffer)
    merged_csv = csv_buffer.getvalue()

    users_added = len(merged_df) - len(existing_df)
    context.log.info(
        "Merged %s generated users into %s existing: %s total (%s new)",
        len(new_df),
        len(existing_df),
        len(merged_df),
        users_added,
    )

    result: dict[str, Any] = {
        "repo": ACCESS_FORGE_REPO,
        "file_path": ACCESS_FORGE_USERS_PATH,
        "branch": branch,
        "users_added": users_added,
        "total_users": len(merged_df),
    }
    if merged_csv == existing_csv:
        context.log.info("User list unchanged; nothing to commit")
        return Output(value={**result, "commit_sha": None}, metadata=result)

    commit = repo.update_file(
        path=ACCESS_FORGE_USERS_PATH,
        message="dagster-pipeline - append new users to user list",
        content=merged_csv,
        sha=contents.sha,
        branch=branch,
    )["commit"]
    context.log.info("Committed %s to %s: %s", commit.sha, branch, commit.html_url)

    result |= {"commit_sha": commit.sha, "commit_url": commit.html_url}
    return Output(value=result, metadata=result)

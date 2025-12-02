"""Assets for managing instructor onboarding data in the access-forge GitHub repository.

This module pulls email addresses from the combined user course roles
dbt model and pushes them to a private GitHub repository for instructor
access management.
"""

from datetime import UTC, datetime
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
from dagster_dbt.asset_utils import get_asset_key_for_model
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.resources.github import GithubApiClientFactory

from lakehouse.assets.lakehouse.dbt import full_dbt_project


@asset(
    name="instructor_onboarding_user_list",
    group_name="instructor_onboarding",
    deps=[
        get_asset_key_for_model([full_dbt_project], "int__combined__user_course_roles")
    ],
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
    - role: Set to 'ol-data-analyst' for all users
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
        .filter(pl.col("platform").is_in(["xPro", "MITx Online"]))
        .select(["user_email"])
        .unique()
        .sort("user_email")
    )

    # Add role and sent_invite columns with fixed values
    user_data = user_data.with_columns(
        [pl.lit("ol-data-analyst").alias("role"), pl.lit(1).alias("sent_invite")]
    )

    # Rename column to match expected format
    user_data = user_data.rename({"user_email": "email"})

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
    description="Updates the access-forge repository with the generated user list",
)
def update_access_forge_repo(
    context: AssetExecutionContext,
    github_api: GithubApiClientFactory,
    instructor_onboarding_user_list: str,
) -> Output[dict[str, Any]]:
    """Update access-forge repository with instructor user list.

    This asset merges new users with existing users in the access-forge repository,
    avoiding duplicates and preserving existing entries.

    Args:
        context: Dagster execution context
        github_api: GitHub API client factory resource for authentication
        instructor_onboarding_user_list: CSV string content from upstream asset

    Returns:
        Output containing metadata about the commit

    Raises:
        Exception: If GitHub API call fails or authentication issues occur
    """
    repo_name = "mitodl/access-forge"
    file_path = "users/ci/users.csv"
    base_branch = "main"

    # Create unique branch name with timestamp
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
    new_branch = f"dagster/update-user-list-{timestamp}"

    commit_message = "dagster-pipeline - append new users to user list"

    try:
        github_client = github_api.get_client()
        repo = github_client.get_repo(repo_name)

        # Get the base branch reference
        base_ref = repo.get_git_ref(f"heads/{base_branch}")
        base_sha = base_ref.object.sha

        # Create new branch from base
        repo.create_git_ref(ref=f"refs/heads/{new_branch}", sha=base_sha)
        context.log.info("Created branch %s", new_branch)

        # Get existing file content
        contents = repo.get_contents(file_path, ref=base_branch)
        existing_csv = contents.decoded_content.decode("utf-8")

        # Parse existing CSV
        existing_df = pl.read_csv(StringIO(existing_csv))
        context.log.info("Found %s existing users", len(existing_df))

        # Parse new CSV
        new_df = pl.read_csv(StringIO(instructor_onboarding_user_list))
        context.log.info("Processing %s new users", len(new_df))

        # Merge: keep all existing users, add new ones not already present
        merged_df = (
            pl.concat([existing_df, new_df])
            .unique(subset=["email"], keep="first")
            .sort("email")
        )

        # Convert merged dataframe to CSV
        csv_buffer = StringIO()
        merged_df.write_csv(csv_buffer)
        merged_csv_content = csv_buffer.getvalue()

        num_added = len(merged_df) - len(existing_df)
        context.log.info(
            "Merged result: %s total users (%s new)", len(merged_df), num_added
        )

        # Update file with merged content
        repo.update_file(
            path=file_path,
            message=commit_message,
            content=merged_csv_content,
            sha=contents.sha,
            branch=new_branch,
        )
        context.log.info("Updated file %s in branch %s", file_path, new_branch)

        # Create pull request
        pr = repo.create_pull(
            title=f"Add new users to list - {timestamp}",
            body=(
                "Automated update of user list from ol-data-platform Dagster "
                f"pipeline.\n\n"
                f"- File: {file_path}\n"
                f"- New users added: {num_added}\n"
                f"- Total users: {len(merged_df)}"
            ),
            head=new_branch,
            base=base_branch,
        )

        context.log.info("Created PR #%s: %s", pr.number, pr.html_url)

        return Output(
            value={
                "repo": repo_name,
                "file_path": file_path,
                "branch": new_branch,
                "pr_number": pr.number,
                "pr_url": pr.html_url,
                "users_added": num_added,
                "total_users": len(merged_df),
            },
            metadata={
                "repository": repo_name,
                "file_path": file_path,
                "pr_number": pr.number,
                "pr_url": pr.html_url,
                "users_added": num_added,
                "total_users": len(merged_df),
            },
        )

    except Exception:
        context.log.exception("Failed to create PR in GitHub repository")
        raise

"""Assets for managing instructor onboarding data in the access-forge GitHub repository.

This module pulls email addresses from the combined user course roles
dbt model and pushes them to a private GitHub repository for instructor
access management.
"""

from datetime import UTC, datetime

import polars as pl
from dagster import AssetExecutionContext, AssetIn, AssetKey, Output, asset
from github.GithubException import UnknownObjectException
from ol_orchestrate.resources.github import GithubApiClientFactory


@asset(
    name="instructor_onboarding_user_list",
    group_name="instructor_onboarding",
    ins={
        "int__combined__user_course_roles": AssetIn(
            key=AssetKey(["int__combined__user_course_roles"])
        )
    },
    description="Generates CSV file with user emails for access-forge repository",
)
def generate_instructor_onboarding_user_list(
    context: AssetExecutionContext,
    int__combined__user_course_roles: pl.DataFrame,
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
        int__combined__user_course_roles: DataFrame from dbt model containing fields:
            platform, user_username, user_email, user_full_name, courserun_readable_id,
            organization, courseaccess_role

    Returns:
        Output containing CSV string content formatted for access-forge repo
    """
    # Select unique email addresses and filter out nulls
    user_data = (
        int__combined__user_course_roles.select(["user_email"])
        .filter(pl.col("user_email").is_not_null())
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

    # Convert to CSV string
    csv_content = user_data.write_csv()

    context.log.info("Generated CSV content with %s unique users", len(user_data))

    return Output(
        value=csv_content,
        metadata={
            "num_users": len(user_data),
            "preview": csv_content[:500],
        },
    )


@asset(
    name="update_access_forge_repo",
    group_name="instructor_onboarding",
    ins={"instructor_onboarding_user_list": AssetIn()},
    required_resource_keys={"github_api"},
    description="Updates the access-forge repository with the generated user list",
)
def update_access_forge_repository(
    context: AssetExecutionContext,
    instructor_onboarding_user_list: str,
    github_api: GithubApiClientFactory,
) -> Output[dict]:
    """Push the generated CSV content to the access-forge GitHub repository.

    This asset updates or creates a CSV file in the private mitodl/access-forge
    repository with the user list generated from the dbt model.

    Args:
        context: Dagster execution context
        instructor_onboarding_user_list: CSV string content to upload with columns:
            email, role, sent_invite
        github_api: GitHub API client factory resource for authentication

    Returns:
        Output containing metadata about the commit (repo, file path, action, SHA)

    Raises:
        Exception: If GitHub API call fails or authentication issues occur
    """
    repo_name = "mitodl/access-forge"
    file_path = "users/ci/users.csv"
    base_branch = "main"

    # Create unique branch name with timestamp
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
    new_branch = f"dagster/update-user-list-{timestamp}"

    commit_message = "dagster-pipeline - update user list from ol-data-platform"

    try:
        gh_client = github_api.get_client()
        repo = gh_client.get_repo(repo_name)

        # Get the base branch reference
        base_ref = repo.get_git_ref(f"heads/{base_branch}")
        base_sha = base_ref.object.sha

        # Create new branch from base
        repo.create_git_ref(ref=f"refs/heads/{new_branch}", sha=base_sha)
        context.log.info("Created branch %s", new_branch)

        # Try to get existing file to update it, or create new file
        try:
            contents = repo.get_contents(file_path, ref=base_branch)
            repo.update_file(
                path=file_path,
                message=commit_message,
                content=instructor_onboarding_user_list,
                sha=contents.sha,
                branch=new_branch,
            )
            context.log.info("Updated file %s in branch %s", file_path, new_branch)
            action = "updated"
        except UnknownObjectException:
            # File doesn't exist, create it
            repo.create_file(
                path=file_path,
                message=commit_message,
                content=instructor_onboarding_user_list,
                branch=new_branch,
            )
            context.log.info("Created file %s in branch %s", file_path, new_branch)
            action = "created"

        # Create pull request
        pr = repo.create_pull(
            title=f"Update user list - {timestamp}",
            body=(
                "Automated update of user list from ol-data-platform Dagster "
                f"pipeline.\n\n"
                f"- Action: {action} file\n"
                f"- File: {file_path}\n"
                f"- Users: {instructor_onboarding_user_list.count(chr(10))} entries"
            ),
            head=new_branch,
            base=base_branch,
        )

        context.log.info("Created PR #%s: %s", pr.number, pr.html_url)

        return Output(
            value={
                "repo": repo_name,
                "file_path": file_path,
                "action": action,
                "branch": new_branch,
                "pr_number": pr.number,
                "pr_url": pr.html_url,
            },
            metadata={
                "repository": repo_name,
                "file_path": file_path,
                "action": action,
                "pr_number": pr.number,
                "pr_url": pr.html_url,
            },
        )

    except Exception:
        context.log.exception("Failed to create PR in GitHub repository")
        raise

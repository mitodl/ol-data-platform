"""Dagster definitions for instructor onboarding GitHub integration.

This module defines assets and resources for syncing instructor access data from the
data warehouse to the private mitodl/access-forge GitHub repository.
"""

from dagster import Definitions

from ol_orchestrate.assets.instructor_onboarding import (
    generate_instructor_onboarding_user_list,
    update_access_forge_repository,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.utils import authenticate_vault
from ol_orchestrate.resources.github import GithubApiClientFactory

vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)

instructor_onboarding_definitions = Definitions(
    assets=[
        generate_instructor_onboarding_user_list,
        update_access_forge_repository,
    ],
    resources={
        "vault": vault,
        "github_api": GithubApiClientFactory(vault=vault),
    },
)

import os
from typing import Literal

from pydantic import BaseModel, BaseSettings, SecretStr
from pydantic_vault import vault_config_settings_source

DAGSTER_ENV: Literal["ci", "qa", "production"] = os.environ.get(  # type: ignore[assignment]
    "DAGSTER_ENVIRONMENT", "ci"
)


class VaultDBCredentials(BaseModel):
    username: str
    password: SecretStr


class VaultBaseSettings(BaseSettings):  # type: ignore[valid-type,misc]
    class Config:
        vault_url: str = f"https://vault-{DAGSTER_ENV}.odl.mit.edu"

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            # This is where you can choose which settings sources to use and their
            # priority
            return (
                vault_config_settings_source,
                init_settings,
                env_settings,
                file_secret_settings,
            )

import dataops.models.configs as cfg

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class AppSettings(BaseSettings):
    """
    Defines application settings for interacting with the portal platform and Census API.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_nested_delimiter="__", env_prefix="APP_"
    )

    user: cfg.UserConfig
    api: cfg.APIConfig
    census: cfg.CensusConfig

    # model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # census_api_key: str = Field("", env="CENSUS_API_KEY")
    # domain: str = Field("", env="DOMAIN")
    # source_id: str = Field("", env="SOURCE_ID")
    # target_id: str = Field("", env="TARGET_ID")
    # socrata_user: str = Field("", env="SOCRATA_USER")
    # socrata_pass: str = Field("", env="SOCRATA_PASS")
    # socrata_token: str = Field("", env="SOCRATA_TOKEN")

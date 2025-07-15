from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Defines application settings for interacting with the portal platform and Census API.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    census_api_key: str = Field("", env="CENSUS_API_KEY")
    domain: str = Field("", env="DOMAIN")
    source_id: str = Field("", env="SOURCE_ID")
    target_id: str = Field("", env="TARGET_ID")
    socrata_user: str = Field("", env="SOCRATA_USER")
    socrata_pass: str = Field("", env="SOCRATA_PASS")
    socrata_token: str = Field("", env="SOCRATA_TOKEN")

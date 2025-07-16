from pydantic import Field, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Annotated


class User(BaseModel):
    user: Annotated[str, Field(description="Username or Email")]
    password: Annotated[str, Field(description="Password")]
    token: Optional[str] = Field(
        default=None, description="API or Auth Token", repr=False
    )


class Socrata(BaseModel):
    domain: str = Field(
        default="https://api.census.gov/data",
        description="Domain name for data portal platform.",
    )
    source_id: str = Field(
        description="Table ID (aka 'four by four') of source data table."
    )
    target_id: Optional[str] = Field(
        default=None, description="Table ID (aka 'four by four') of target data table."
    )


class UserInput(BaseModel):
    user: User
    socrata: Socrata


class AppSettings(BaseSettings):
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


# TODO implement some socrata validation around some of those fields
# TODO rename this and include the user facing stuff here

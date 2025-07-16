from pydantic import (
    Field,
    BaseModel,
    SecretStr,
    HttpUrl,
    field_validator,
)
from typing import Optional, Annotated


class Token(BaseModel):
    """API or Auth Token"""

    token: Optional[SecretStr] = Field(
        default=None, description="API or Auth Token", repr=False
    )


class UserConfig(BaseModel):
    """Validates user-specific credentials."""

    username: Annotated[str, Field(description="Username or Email")]
    password: Annotated[SecretStr, Field(description="Password", repr=False)]
    token: Token


# this class is mostly a placeholder for future features
class CensusConfig(BaseModel):
    """Validate Census API specific details."""

    token: Token
    # endpoint?


class SocrataTableID(BaseModel):
    """Socrata Platform Unique Table Identifier."""

    id: Annotated[
        str,
        Field(description="Table ID (aka 'four by four') of source data table."),
    ]

    @field_validator("id")
    def _check_id(cls, v: str) -> str:
        overall = len(v) == 9
        v_parts = str.split(v, "-")
        fourbyfour = len(v_parts[0]) == len(v_parts[1])
        if not overall & fourbyfour:
            raise ValueError(f"{v} is not a valid socrata table id")
        return v


class APIConfig(BaseModel):
    """Validates API-specific details."""

    domain: HttpUrl = Field(
        default="https://api.census.gov/data",
        description="Domain name for data portal platform.",
    )
    source_id: SocrataTableID
    # target_id: Optional[SocrataTableID]
    target_id: Optional[SocrataTableID] = Field(default=None)

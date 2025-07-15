from dataops.models import app

# import os
from urllib.parse import urlparse, parse_qs
import requests
from typing import List, Optional, Annotated
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
    computed_field,
    ValidationError,
)

# ideas /todoish
# class APIVariable():
# class for subj, btable, dp etc...


class APIEndpoint(BaseModel):
    """
    A Pydantic model to represent, validate, and interact with a
    U.S. Census Bureau's American Community Survey API endpoint.
    """

    # Core Endpoint Components
    base_url: HttpUrl = Field(
        default="https://api.census.gov/data",
        description="The base URL for the Census ACS API.",
    )

    year: Annotated[int, Field(gt=2004, description="The survey year (e.g., 2020).")]

    dataset: Annotated[
        str, Field(description="The dataset identifier (e.g., 'acs/acs1', 'acs/acs5').")
    ]

    variables: Annotated[
        List[str],
        Field(
            min_length=1,
            description="A list of variable names to retrieve (e.g., ['NAME', 'P1_001N']).",
        ),
    ]

    geography: Annotated[
        str,
        Field(
            default="ucgid:0400000US09",
            description="The geography specification (e.g., 'state:*', 'ucgid:0400000US09').",
        ),
    ]

    api_key: Optional[str] = Field(
        repr=False,
        description="Your Census API key. If not provided, it's sourced from the CENSUS_API_KEY environment variable.",
    )

    # # TODO see if I need this if I'm just using app settings
    @model_validator(mode="before")
    @classmethod
    def set_api_key_from_env(cls, data: any) -> any:
        """Sets API key from env var if not provided."""
        if isinstance(data, dict) and not data.get("api_key"):
            data["api_key"] = app.Settings().census_api_key
        return data

    @field_validator("dataset")
    @classmethod
    def dataset_must_not_have_leading_or_trailing_slashes(cls, v: str) -> str:
        """Ensures the dataset string is clean."""
        return v.strip("/")

    # --- Computed Properties for Functionality ---
    @computed_field
    @property
    def full_url(self) -> str:
        """Constructs the complete, queryable API URL from the model's attributes."""
        get_params = ",".join(self.variables)
        url_path = f"{self.base_url}/{self.year}/{self.dataset}"
        geo_key, geo_value = self.geography.split(":", 1)
        params = {"get": get_params, geo_key: geo_value}
        if self.api_key:
            params["key"] = self.api_key
        req = requests.Request("GET", url_path, params=params)
        return req.prepare().url

    @computed_field
    @property
    def url_no_key(self) -> str:
        """Constructs the complete, queryable API URL from the model's attributes."""
        get_params = ",".join(self.variables)
        url_path = f"{self.base_url}/{self.year}/{self.dataset}"
        geo_key, geo_value = self.geography.split(":", 1)
        params = {"get": get_params, geo_key: geo_value}
        req = requests.Request("GET", url_path, params=params)
        return req.prepare().url

    # TODO: if groups add the groups to reduce api overshoot
    @computed_field
    @property
    def variable_url(self) -> str:
        """Constructs the variable API URL from the full url."""
        return f"{self.base_url}/{self.year}/{self.dataset}/variables"

    @computed_field
    @property
    def table_type(self) -> str:
        dataset_parts = self.dataset.strip("/").split("/")
        last = dataset_parts[-1]
        middle = dataset_parts[1]

        if last == middle:
            return "detailed table"
        else:
            return f"{last} table"

    # Alternative Constructor from URL
    @classmethod
    def from_url(cls, url: str) -> "APIEndpoint":
        """Parses a full Census API URL string and creates an instance."""
        try:
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            path_parts = [
                part for part in parsed_url.path.strip("/").split("/") if part
            ]
            if path_parts[0] != "data" or len(path_parts) < 3:
                raise ValueError(
                    "URL path does not match expected '/data/{year}/{dataset...}' structure."
                )
            year = int(path_parts[1])
            dataset = "/".join(path_parts[2:])
            variables = query_params.get("get", [""])[0].split(",")
            if not variables or variables == [""]:
                raise ValueError(
                    "Could not find 'get' parameter for variables in URL query."
                )
            geo_key = next(
                (key for key in ["for", "in", "ucgid"] if key in query_params), None
            )
            if not geo_key:
                raise ValueError(
                    "Could not find a recognized geography parameter ('for', 'in', 'ucgid') in URL."
                )
            geography = f"{geo_key}:{query_params[geo_key][0]}"
            api_key = query_params.get("key", [None])[0]
            return cls(
                year=year,
                dataset=dataset,
                variables=variables,
                geography=geography,
                api_key=api_key,
            )
        except (ValueError, IndexError, TypeError) as e:
            raise ValueError(f"Failed to parse URL '{url}'. Reason: {e}") from e
        except ValidationError as e:
            raise ValueError(
                f"Parsed URL components failed validation. Reason: {e}"
            ) from e

    # TODO push stuff that requires a request elsewhere
    # @computed_field
    # @property
    # def concept(self) -> str:
    #     """Endpoint concept"""

    #     if self.table_type != "not_table":
    #         return (
    #             self.fetch_variable_labels().select(pl.col("concept").unique()).item()
    #         )

    #     else:
    #         return "no_concept"

    def __repr__(self):
        return (
            f"APIEndpoint(\n\tdataset='{self.dataset}',\n"
            f"\tbase_url='{self.base_url}', \n"
            f"\ttable_type='{self.table_type}', \n"
            # f"\tconcept='{self.concept}', \n"
            f"\tyear='{self.year}', \n"
            f"\tvariables='{self.variables}', \n"
            f"\tgeography='{self.geography}', \n"
            f"\turl_no_key='{self.url_no_key}', \n"
            f"\tvariable_url='{self.variable_url}',\n)"
        )

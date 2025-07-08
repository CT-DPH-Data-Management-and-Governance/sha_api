import os
import sys
import requests
import polars as pl
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
    computed_field,
    ValidationError,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional, Annotated
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
from datetime import datetime
# import re

load_dotenv()

# TODO: convert to pydantic settings and implement
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")


class ApplicationSettings(BaseSettings):
    """
    Defines application settings for interacting with the portal platform and Census API.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    census_api_key: str = Field("census_api_key")
    domain: str = Field("domain")
    table_source: str = Field("table_source")
    table_target: str = Field("table_target")
    resource: str = Field("resource")
    user: str = Field("user")
    password: str = Field("password")
    token: str = Field("token")


class CensusAPIEndpoint(BaseModel):
    """
    A Pydantic model to represent, validate, and interact with a
    U.S. Census Bureau API endpoint.
    """

    # --- Core URL Components ---
    base_url: HttpUrl = Field(
        default="https://api.census.gov/data",
        description="The base URL for the Census API.",
    )
    year: Annotated[int, Field(gt=1999, description="The survey year (e.g., 2020).")]
    dataset: Annotated[
        str, Field(description="The dataset identifier (e.g., 'dec/dhc', 'acs/acs5').")
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
            description="The geography specification (e.g., 'state:*', 'ucgid:0400000US09')."
        ),
    ]
    api_key: Optional[str] = Field(
        default=None,
        repr=False,
        description="Your Census API key. If not provided, it's sourced from the CENSUS_API_KEY environment variable.",
    )

    # --- Alternative Constructor from URL ---
    @classmethod
    def from_url(cls, url: str) -> "CensusAPIEndpoint":
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

    # --- Pydantic Validators ---
    @model_validator(mode="before")
    @classmethod
    def set_api_key_from_env(cls, data: any) -> any:
        """Sets API key from env var if not provided."""
        if isinstance(data, dict) and not data.get("api_key"):
            data["api_key"] = os.getenv("CENSUS_API_KEY")
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
            return "not_table"
        else:
            return last

    @computed_field
    @property
    def concept(self) -> str:
        """Endpoint concept"""

        if self.table_type != "not_table":
            return (
                self.fetch_variable_labels().select(pl.col("concept").unique()).item()
            )

        else:
            return "no_concept"

    def __repr__(self):
        return (
            f"CensusAPIEndpoint(\n\tdataset='{self.dataset}',\n"
            f"\tbase_url='{self.base_url}', \n"
            f"\ttable_type='{self.table_type}', \n"
            f"\tconcept='{self.concept}', \n"
            f"\tyear='{self.year}', \n"
            f"\tvariables='{self.variables}', \n"
            f"\tgeography='{self.geography}', \n"
            f"\turl_no_key='{self.url_no_key}', \n"
            f"\tvariable_url='{self.variable_url}',\n)"
        )

    # --- Data Fetching Methods ---

    def fetch_all_variable_labels(self) -> pl.DataFrame:
        """
        Fetches all the variable labels found at
        the related api endpoint and returns it as a
        Polars DataFrame.
        """

        response = requests.get(self.variable_url, timeout=30)
        response.raise_for_status()

        data = response.json()
        data = (
            pl.from_dicts(data)
            .transpose(column_names="column_0")
            .with_columns(date_pulled=datetime.now())
        )
        return data

    def fetch_variable_labels(self) -> pl.DataFrame:
        """
        Fetches the variable labels related to the specific
        api endpoint, filters it to only the relevant variables
        and returns it as a Polars DataFrame.
        """

        response = requests.get(self.variable_url, timeout=30)
        response.raise_for_status()

        # create var search list
        vars = (
            pl.DataFrame({"vars": self.variables})
            .with_columns(
                pl.col("vars")
                .str.replace_all("\\(|\\)", " ")
                .str.strip_chars()
                .str.split(by=" ")
            )
            .select("vars")
            .to_series()
            .explode()
            .to_list()
        )

        data = response.json()

        # filter endpoint variables to only those in search list
        data = (
            pl.from_dicts(data)
            .transpose(column_names="column_0")
            .lazy()
            .filter(pl.col("name").str.contains_any(vars))
            .with_columns(date_pulled=datetime.now())
            .collect()
        )
        return data

    # TODO: fix bugs tomorrow :c
    def fetch_data_to_polars(self) -> pl.DataFrame:
        """Fetches data and returns it as a Polars DataFrame."""
        try:
            response = requests.get(self.full_url, timeout=30)
            response.raise_for_status()

            data = response.json()

        except requests.exceptions.HTTPError as http_err:
            print(
                f"HTTP error occurred for {self.dataset}: {http_err} | Content: {response.text}"
            )
            sys.exit(1)

        except Exception as e:
            print(f"An unexpected error occurred for {self.dataset}: {e}")
            sys.exit(1)

        # polars expressions
        # column selection
        geo_cols = ["geo_id", "ucgid", "geo_name"]
        all_cols = ["headers", "records", "geo_id", "ucgid", "geo_name", "date_pulled"]
        geo_expr = pl.col(geo_cols)
        all_expr = pl.col(all_cols)

        # used for capturing geo rows
        def ensure_column_exists(
            df: pl.DataFrame,
            column_name: List[str] = ["geo_id", "ucgid", "geo_name"],
            default_value: any = "unknown",
        ) -> pl.DataFrame:
            for col_name in column_name:
                if col_name not in df.columns:
                    df = df.with_columns(pl.lit(default_value).alias(col_name))

            return df

        if not data or len(data) < 2:
            print(f"Warning: API for {self.dataset} returned unexpected format.")
            df = pl.DataFrame(
                {"headers": "unknown", "records": "unknown"}
            ).with_columns(date_pulled=datetime.now())

            df = ensure_column_exists(df).select(all_expr)

            return df

        if len(data) == 2:
            # typed table data usually comes in a list of 2 LONG lists
            lf = (
                pl.DataFrame({"headers": data[0], "records": data[1]})
                .with_columns(date_pulled=datetime.now())
                .lazy()
            )
            # ensure geos are formatted the same way for everything
            geos = (
                lf.select(pl.col("headers").str.to_lowercase(), pl.col("records"))
                .filter(
                    (pl.col("headers").eq("geo_id"))
                    | (pl.col("headers").eq("name"))
                    | (pl.col("headers").eq("ucgid"))
                )
                .with_columns(
                    pl.when(pl.col("headers").eq("name"))
                    .then(pl.lit("geo_name"))
                    .otherwise(pl.col("headers"))
                    .alias("headers")
                )
                .collect()
                .transpose(column_names="headers")
            )
            geos = ensure_column_exists(geos).select(geo_expr).lazy()

            df = (
                (
                    pl.concat([lf, geos], how="horizontal")
                    .fill_null(strategy="forward")
                    .select(all_expr)
                )
                .filter(
                    (pl.col("headers").str.to_lowercase().ne("geo_id"))
                    & (pl.col("headers").str.to_lowercase().ne("name"))
                    & (pl.col("headers").str.to_lowercase().ne("ucgid"))
                )
                .collect()
            )

            return df

        if len(data) > 2:
            # other data are a list of headers, and then a list of arrays

            all_frames = []
            headers = data[0]

            for record in data[1:]:
                lf = (
                    pl.DataFrame({"headers": headers, "records": record})
                    .with_columns(date_pulled=datetime.now())
                    .lazy()
                )

                # ensure geos are formatted the same way for everything
                geos = (
                    lf.select(pl.col("headers").str.to_lowercase(), pl.col("records"))
                    .filter(
                        (pl.col("headers").eq("geo_id"))
                        | (pl.col("headers").eq("name"))
                        | (pl.col("headers").eq("ucgid"))
                    )
                    .with_columns(
                        pl.when(pl.col("headers").eq("name"))
                        .then(pl.lit("geo_name"))
                        .otherwise(pl.col("headers"))
                        .alias("headers")
                    )
                    .collect()
                    .transpose(column_names="headers")
                )

                geos = ensure_column_exists(geos).select(geo_expr).lazy()

                lf = (
                    pl.concat([lf, geos], how="horizontal")
                    .filter(
                        (pl.col("headers").str.to_lowercase().ne("geo_id"))
                        & (pl.col("headers").str.to_lowercase().ne("name"))
                        & (pl.col("headers").str.to_lowercase().ne("ucgid"))
                    )
                    .fill_null(strategy="forward")
                    .select(all_expr)
                )

                all_frames.append(lf)

            df = pl.concat(all_frames).collect()

            return df

    def fetch_tidy_data(self) -> pl.DataFrame:
        """
        Fetch a tidy, human-readable dataset
        from the census api endpoint and return
        as a polars dataframe.
        """

        labels = self.fetch_variable_labels().lazy()  # .drop("date_pulled").lazy()
        data = self.fetch_data_to_polars().lazy()  # .drop("date_pulled").lazy()

        all_cols = [
            "row_id",
            "dataset",
            "year",
            "concept",
            "geo_id",
            "ucgid",
            "geo_name",
            "variable_id",
            "variable_name",
            "value",
            "value_type",
            "full_url",
            "date_pulled",
        ]

        all_expr = pl.col(all_cols)

        # ensure data are presented same way everytime
        tidy = (
            data.join(
                labels,
                left_on="headers",
                right_on="name",
                how="left",
            )
            .with_columns(
                pl.col("label")
                .str.replace_all("!|:", " ")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase()
                .alias("variable_name"),
                pl.col("concept").str.to_lowercase(),
                pl.col("records").cast(pl.Float32, strict=False).alias("value"),
            )
            .with_columns(
                pl.col("variable_name").fill_null(strategy="forward"),
                pl.col("headers")
                .str.slice(-2)
                .str.replace_all(r"\d", "")
                .alias("value_type"),
                pl.col("headers").alias("variable_id"),
            )
            .filter(pl.col("value") > -555555555)  # drop suppressed rows
            .drop_nulls(pl.col("value"))  # drops rows like "***" or (X) post-cast
            .with_row_index(name="row_id")
            .with_columns(
                pl.col("variable_name")
                .str.replace(self.concept, "")
                .str.replace("estimates", "")
                .str.strip_chars()
                .alias("variable_name"),
                pl.when(pl.col("value_type") == pl.lit("E"))
                .then(pl.lit("estimate"))
                .when(pl.col("value_type") == pl.lit("M"))
                .then(pl.lit("margin_of_error"))
                .when(pl.col("value_type") == pl.lit("P"))
                .then(pl.lit("percent_estimate"))
                .when(pl.col("value_type") == pl.lit("PM"))
                .then(pl.lit("percent_margin_of_error"))
                .when(pl.col("value_type") == pl.lit("N"))
                .then(pl.lit("count"))
                .otherwise(pl.col("value_type"))
                .alias("value_type"),
                pl.lit(self.dataset).alias("dataset"),
                pl.lit(self.year).alias("year"),
                pl.lit(self.url_no_key).alias("full_url"),
            )
            .fill_null(strategy="forward")  # fill concept forward
            .select(all_expr)  # enforce order
            .collect()
        )
        return tidy

import os

# import asyncio
import requests
import httpx  # Added for async requests
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
from typing import List, Optional, Annotated
from urllib.parse import urlparse, parse_qs

# --- Best Practice: Load secrets like API keys from environment variables ---
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")


class CensusAPIEndpoint(BaseModel):
    """
    A Pydantic model to represent, validate, and interact with a
    U.S. Census Bureau API endpoint.

    This class can be instantiated directly with parameters or by parsing
    an existing Census API URL. It supports both synchronous (requests)
    and asynchronous (httpx) data fetching.
    """

    # --- Core URL Components ---
    base_url: HttpUrl = Field(
        default="https://api.census.gov/data",
        description="The base URL for the Census API.",
    )
    year: Annotated[int, Field(gt=2015, description="The survey year (e.g., 2020).")]
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
        # Use httpx's functionality to build the URL to be consistent
        return str(httpx.Request("GET", url_path, params=params).url)

    # --- Data Fetching Methods ---
    #  this works fine for dec but will need logic
    # to use the pull from pull.py for acs
    def fetch_data_to_polars(self) -> pl.DataFrame:
        """
        [SYNC] Fetches data using 'requests' and returns it as a Polars DataFrame.
        """
        print(f"[SYNC] Fetching data from: {self.dataset}")

        if "acs" in self.dataset:
            try:
                response = requests.get(self.full_url, timeout=30)
                response.raise_for_status()
                data = response.json()
                if not data or len(data) < 2:
                    print(f"Warning: API for {self.dataset} returned no data.")
                df = pl.DataFrame({"headers": data[0], "records": data[1]})
                return df
            except requests.exceptions.HTTPError as http_err:
                print(
                    f"HTTP error occurred for {self.dataset}: {http_err} | Content: {response.text}"
                )
            except Exception as e:
                print(f"An unexpected error occurred for {self.dataset}: {e}")
        else:
            try:
                response = requests.get(self.full_url, timeout=30)
                response.raise_for_status()
                data = response.json()
                if not data:
                    print(f"Warning: API for {self.dataset} returned no data.")
                    return pl.DataFrame()
                headers, records = data[0], data[1:]
                df = pl.DataFrame(records, schema=headers)
                # for col in df.columns:
                #     try:
                #         df = df.with_columns(df[col].cast(pl.Int64, strict=False))
                #     except pl.ComputeError:
                #         df = df.with_columns(df[col].cast(pl.Float64, strict=False))
                return df
            except requests.exceptions.HTTPError as http_err:
                print(
                    f"HTTP error occurred for {self.dataset}: {http_err} | Content: {response.text}"
                )
            except Exception as e:
                print(f"An unexpected error occurred for {self.dataset}: {e}")
            return pl.DataFrame()


# testing
user_url = "https://api.census.gov/data/2021/acs/acs1?get=NAME,B01001_001E&for=us:1"


parsed_url = CensusAPIEndpoint.from_url(user_url)

parsed_url


data = parsed_url.fetch_data_to_polars()

data


# probably better to keep this simple - just use system threads


#     async def fetch_data_async(self, client: httpx.AsyncClient) -> pl.DataFrame:
#         """
#         [ASYNC] Fetches data using 'httpx' and returns it as a Polars DataFrame.

#         Args:
#             client: An httpx.AsyncClient instance for connection pooling.
#         """
#         print(f"[ASYNC] Fetching data from: {self.dataset}")
#         try:
#             response = await client.get(self.full_url, timeout=30)
#             response.raise_for_status()
#             data = response.json()
#             if not data or len(data) < 2:
#                 print(f"Warning: API for {self.dataset} returned no data.")
#                 return pl.DataFrame()
#             headers, records = data[0], data[1:]
#             df = pl.DataFrame(records, schema=headers)
#             for col in df.columns:
#                 try:
#                     df = df.with_columns(df[col].cast(pl.Int64, strict=False))
#                 except pl.ComputeError:
#                     df = df.with_columns(df[col].cast(pl.Float64, strict=False))
#             return df
#         except httpx.HTTPStatusError as http_err:
#             print(
#                 f"HTTP error occurred for {self.dataset}: {http_err} | Content: {http_err.response.text}"
#             )
#         except Exception as e:
#             print(f"An unexpected error occurred for {self.dataset}: {e}")
#         return pl.DataFrame()


# async def main():
#     """Main async function to demonstrate concurrent fetching."""
#     print("--- Demonstrating Concurrent Data Fetching with httpx and asyncio ---")

#     urls_to_fetch = [
#         "https://api.census.gov/data/2020/dec/dhc?get=NAME,P1_001N&for=state:*",
#         "https://api.census.gov/data/2021/acs/acs1?get=NAME,B01001_001E&for=us:1",
#         "https://api.census.gov/data/2022/pep/population?get=GEO_ID,POP_2022,NAME&for=county:*&in=state:09",
#         "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1701)&ucgid=0400000US09",
#         "https://api.census.gov/data/2021/acs/acs5/profile?get=DP05_0001E,NAME&for=place:53000&in=state:06",  # Los Angeles city
#     ]

#     # 1. Create a list of endpoint objects
#     endpoints = [CensusAPIEndpoint.from_url(url) for url in urls_to_fetch]

#     # 2. Use an AsyncClient as a context manager for connection pooling
#     async with httpx.AsyncClient() as client:
#         # 3. Create a list of tasks to run concurrently
#         tasks = [endpoint.fetch_data_async(client) for endpoint in endpoints]

#         # 4. Run all tasks and gather the results
#         # return_exceptions=True prevents one failed request from stopping all others
#         results = await asyncio.gather(*tasks, return_exceptions=True)

#     print("\n--- Fetching Complete ---")
#     for endpoint, result in zip(endpoints, results):
#         if isinstance(result, Exception):
#             print(f"\nFailed to fetch {endpoint.dataset}: {result}")
#         elif not result.is_empty():
#             print(f"\nSuccessfully fetched data for '{endpoint.dataset}':")
#             print(result.head(3))
#         else:
#             print(f"\nNo data returned for '{endpoint.dataset}'.")


# if __name__ == "__main__":
#     # To run the async main function
#     asyncio.run(main())

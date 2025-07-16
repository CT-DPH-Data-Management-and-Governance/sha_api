from pydantic import (
    BaseModel,
    HttpUrl,
    Field,
    SecretStr,
    field_validator,
    model_validator,
    computed_field,
    ValidationError,
)
from urllib.parse import urlparse, parse_qs


class APIEndpointMixin:
    """A mixin to add methods to APIEndpoint."""

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

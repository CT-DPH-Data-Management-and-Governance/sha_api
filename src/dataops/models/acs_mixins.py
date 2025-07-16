from enum import Enum

import requests
from pydantic import (
    computed_field,
    field_validator,
    model_validator,
)

from dataops.models import settings


class TableType(str, Enum):
    subject = "subject"
    detailed = "detailed"
    cprofile = "cprofile"
    unknown = "unknown"


class APIEndpointMixin:
    """A mixin to add methods to APIEndpoint."""

    @model_validator(mode="before")
    @classmethod
    def set_api_key_from_env(cls, data: any) -> any:
        """Sets API key from env var if not provided."""
        if isinstance(data, dict) and not data.get("api_key"):
            data["api_key"] = settings.AppSettings().census.token.get_secret_value()
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
        if self.api_key.get_secret_value():
            params["key"] = self.api_key.get_secret_value()
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
    def variable_endpoint(self) -> str:
        """Constructs the variable API URL from the full url."""

        last_resort = f"{self.base_url}/{self.year}/{self.dataset}/variables"

        match self.table_type:
            case TableType.unknown:
                return last_resort
            case _:
                return f"{self.base_url}/{self.year}/{self.dataset}/groups/{self.group}"

    @computed_field
    @property
    def group(self) -> str:
        _variable_string = "".join(self.variables)
        _length = len(self.variables)
        _starts_with = _variable_string.startswith("group")
        _is_group = (_length < 2) & (_starts_with)

        if _is_group:
            return _variable_string.removeprefix("group(").removesuffix(")")

        else:
            return None

    @computed_field
    @property
    def table_type(self) -> str:
        dataset_parts = self.dataset.strip("/").split("/")
        last = dataset_parts[-1]
        middle = dataset_parts[1]

        # TODO refactor to use self.group
        _variable_string = "".join(self.variables)
        _length = len(self.variables)
        _starts_with = _variable_string.startswith("group")
        _maybe_detailed = last == middle

        _is_group = (_length < 2) & (_starts_with) & (_maybe_detailed)

        if _is_group:
            return TableType.detailed

        else:
            try:
                tabletype = TableType[last]
            except KeyError:
                tabletype = TableType.unknown
            finally:
                return tabletype

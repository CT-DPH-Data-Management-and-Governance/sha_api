from .models import ApplicationSettings
import polars as pl
from sodapy import Socrata


def fetch_data(
    resource: str | None = None,
    settings: ApplicationSettings | None = None,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """
    Retrieve portal data as polars dataframe.
    Environmental variables are used as defaults unless otherwise specified.
    """
    if settings is None:
        settings = ApplicationSettings()

    if resource is None:
        resource = settings.resource

    with Socrata(
        settings.domain, settings.token, settings.user, settings.password
    ) as client:
        data = client.get_all(resource)
        data = pl.LazyFrame(data)

    if not lazy:
        return data.collect()

    return data


def pull_endpoints(df: pl.DataFrame) -> list[str] | pl.DataFrame:
    """Retrieve a list of api endpoints from a dataframe."""

    if "endpoint" in df.columns:
        return df.select(pl.col("endpoint").struct.unnest()).to_series().to_list()

    return df

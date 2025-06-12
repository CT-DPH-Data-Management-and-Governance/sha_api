import polars as pl
import polars.selectors as cs
from pandas import read_html

# TODO come back and just figure out how to use the json when there is time
# relevant code from the tidycensus package
# probably worth just implementing tidycensus somewhere along the pipeline
# https://github.com/walkerke/tidycensus/blob/1b6027e8deac8a44d70728d548f40d60145e3105/R/search_variables.R#L131


def read_acs_var_html(year: int, acs_type: int) -> pl.LazyFrame:
    """
    For Specified year and acs type, pull down and save ACS
    variable list as a .csv and a .parquet.

    Args:
        year (int): The year of the ACS.
        acs_type (int): The type of ACS, either 1 or 5

    Returns:
        pl.LazyFrame: A polars lazy frame containing the ACS variables.

    """
    acs_types = [1, 5]
    if acs_type not in acs_types:
        raise ValueError("acs_type can only be 1 or 5")

    request_string = (
        f"https://api.census.gov/data/{year}/acs/acs{acs_type}/variables.html"
    )

    lf = (
        pl.from_pandas(read_html(request_string)[0])
        .filter(~(pl.col("Name").str.ends_with("variables")))
        .drop(cs.starts_with("Unnamed"))
        .lazy()
    )

    return lf


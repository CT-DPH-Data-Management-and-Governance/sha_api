import polars as pl
import pandas as pd
import polars.selectors as cs

def grab_acs_variables(year: int, acs_type: int) -> None:
    ''' 
    For Specified year and acs type, pull down and save ACS
    variable list as a .csv and a .parquet.

    Args:
        year (int): The year of the ACS.
        acs_type (int): The type of ACS, either 1 or 5
    
    Returns:
        None: Prints status to console.

    '''
    acs_types = [1,5]
    if acs_type not in acs_types:
        raise ValueError("acs_type can only be 1 or 5")
    
    request_string = \
    f"https://api.census.gov/data/{year}/acs/acs{acs_type}/variables.html"
    
    # TODO come back and just use the json when there is time
    df = (
        pl.from_pandas(
            pd.read_html(request_string)[0]
        )
        .filter(
            ~(pl.col("Name").str.ends_with("variables"))
        )
        .drop(cs.starts_with("Unnamed"))
    )

    return df
    
# # testing
import polars as pl
from src.dataops.models import CensusAPIEndpoint

user_url = "https://api.census.gov/data/2021/acs/acs1?get=NAME,B01001_001E&for=us:1"
# user_url = "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1501)&ucgid=0400000US09"
# user_url = "https://api.census.gov/data/2023/acs/acs5/profile?get=group(DP05)&ucgid=0400000US09"

# user_url = "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1701)&ucgid=0400000US09"

parsed_url = CensusAPIEndpoint.from_url(user_url)

parsed_url

parsed_url.variable_url

parsed_url.fetch_variable_labels()


labels = parsed_url.fetch_variable_labels()

data = parsed_url.fetch_data_to_polars()


joined_data = (
    data.join(labels, left_on="headers", right_on="name", how="left")
    .with_columns(
        pl.col("label")
        .str.strip_chars()
        .str.replace_all("!|:", " ")
        .str.replace_all(r"\s+", " ")
        .str.to_lowercase()
        .alias("new_label"),
        pl.col("concept").str.to_lowercase().alias("new_concept"),
    )
    .with_columns(
        pl.concat_str(
            [pl.col("new_concept"), pl.col("new_label")], separator=" "
        ).alias("variable_name")
    )
)

joined_data


df = parsed_url.fetch_data_to_polars()

df

cd = parsed_url.fetch_data()
cd

cd.variable_metadata

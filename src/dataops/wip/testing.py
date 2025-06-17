# # testing
import polars as pl
from src.dataops.models import CensusAPIEndpoint

# user_url = "https://api.census.gov/data/2021/acs/acs1?get=NAME,B01001_001E&for=us:1"
user_url = "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1501)&ucgid=0400000US09"
# user_url = "https://api.census.gov/data/2023/acs/acs5/profile?get=group(DP05)&ucgid=0400000US09"

# user_url = "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1701)&ucgid=0400000US09"

parsed_url = CensusAPIEndpoint.from_url(user_url)

parsed_url

parsed_url.variable_url

parsed_url.fetch_variable_labels()
parsed_url.fetch_all_variable_labels()


test1 = "https://api.census.gov/data/2021/acs/acs1?get=NAME,B01001_001E&for=us:1"
test1 = CensusAPIEndpoint.from_url(test1)
test1_labels = test1.fetch_variable_labels()

test1_data = test1.fetch_data_to_polars()


joined_data = (
    test1_data.join(test1_labels, left_on="headers", right_on="name", how="left")
    .with_columns(
        pl.col("label")
        .str.replace_all("!|:", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_lowercase()
        .alias("new_label"),
        pl.col("concept").str.to_lowercase().alias("new_concept"),
    )
    .with_columns(
        pl.concat_str(
            [pl.col("new_concept"), pl.col("new_label")], separator=" "
        ).alias("variable_name")
    )
    .select(pl.col("headers"), pl.col("records"), pl.col("variable_name"))
)

joined_data


test2 = "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1501)&ucgid=0400000US09"
test2 = CensusAPIEndpoint.from_url(test2)
test2_labels = test2.fetch_variable_labels()
test2_data = test2.fetch_data_to_polars()

joined_data2 = (
    test2_data.join(test2_labels, left_on="headers", right_on="name", how="left")
    .with_columns(
        pl.col("label")
        .str.replace_all("!|:", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_lowercase()
        .alias("new_label"),
        pl.col("concept").str.to_lowercase().alias("new_concept"),
    )
    .with_columns(
        pl.concat_str(
            [pl.col("new_concept"), pl.col("new_label")], separator=" "
        ).alias("variable_name")
    )
    .select(
        pl.col("variable_name"),
        pl.col("records"),
        pl.col("headers"),
    )
    .drop_nulls(pl.col("records"))
)

joined_data2

joined_data2.write_parquet("joined2-nulls-dropped.parquet")


pl.read_parquet("joined2-nulls-dropped.parquet").write_csv("sample.csv")


test3 = "https://api.census.gov/data/2023/acs/acs5/profile?get=group(DP05)&ucgid=0400000US09"
test3 = CensusAPIEndpoint.from_url(test3)
test3_labels = test3.fetch_variable_labels()
test3_data = test3.fetch_data_to_polars()

joined_data3 = (
    test3_data.join(test3_labels, left_on="headers", right_on="name", how="left")
    .with_columns(
        pl.col("label")
        .str.replace_all("!|:", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_lowercase()
        .alias("new_label"),
        pl.col("concept").str.to_lowercase().alias("new_concept"),
    )
    .with_columns(
        pl.concat_str(
            [pl.col("new_concept"), pl.col("new_label")], separator=" "
        ).alias("variable_name")
    )
    .select(
        pl.col("variable_name"),
        pl.col("records"),
        pl.col("headers"),
    )
    .drop_nulls(pl.col("records"))
)

joined_data3


joined_data3.with_columns(
    pl.when(pl.col("headers") == "NAME")
    .then(pl.lit("name"))
    .when(pl.col("headers") == "GEO_ID")
    .then(pl.lit("geoid"))
    .otherwise(pl.col("variable_name"))
    .alias("variable_name"),
    pl.col("records").cast(pl.Float32, strict=False).alias("record_value"),
).filter(pl.col("record_value") > -555555555).drop_nulls(pl.col("record_value"))


df = parsed_url.fetch_data_to_polars()

df

cd = parsed_url.fetch_data()
cd

cd.variable_metadata

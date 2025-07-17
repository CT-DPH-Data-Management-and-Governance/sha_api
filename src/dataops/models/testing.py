from datetime import datetime as dt
from dataops.models.acs import APIEndpoint as ae
from dataops.models.acs import APIData as ad
from dataops.portals import _get

import polars as pl

url2 = "https://api.census.gov/data/2023/acs/acs5/subject?get=group(S2201)&ucgid=pseudo(0400000US09$0600000)"
url1 = "https://api.census.gov/data/2022/acs/acs1?get=group(B19013I)&ucgid=0400000US09"

endpoint = ae.from_url(url1)
data = ad(endpoint=endpoint)

var_labels = data._var_labels
shotgun_aprch = _get(
    #  "https://api.census.gov/data/2023/acs/acs5/subject/variables", "s_table"
    "https://api.census.gov/data/2023/acs/acs5/variables",
    "b_tables",
)

var_labels
the_okay = pl.from_dicts(shotgun_aprch).transpose(column_names="column_0")
# targeted stays in dicts
var_labels.keys()
var_labels.get("variables").keys()
var_labels.get("variables").get("B19013I_001EA")
# this works - kinda :)
wide = pl.from_dicts(var_labels.get("variables")).drop(["GEO_ID", "NAME"])


long = wide.with_row_index().unpivot(index="index").drop("index")

# this is great - much more info - more clear - I just don't
# know if I can always ensure this approach
ideal = long.with_columns(pl.col("value").struct.unnest()).drop("value")


ideal.head(1)
ideal.columns
the_okay.head(1)
the_okay.columns

the_okay.with_columns()

# for s tables
# we get concept per question with targeted approach and with shotgun appch

# for b tables
# we get concept per question with shotgun aprch
#

# this came back as a dict - are the targeted
# endpoints always a dict and the rest a list?

endpoint_vars = (
    pl.LazyFrame({"vars": ad.endpoint.variables})
    .with_columns(
        pl.col("vars")
        .str.replace_all("\\(|\\)", " ")
        .str.strip_chars()
        .str.split(by=" ")
    )
    .select("vars")
    .collect()
    .to_series()
    .explode()
    .to_list()
)

pl.from_dicts(data).transpose(column_names="column_0").filter(
    pl.col("name").str.contains_any(endpoint_vars)
).with_columns(date_pulled=dt.now())

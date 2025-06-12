import polars as pl
import requests

# df = acs.grab_file_targets()


# TODO  - account for multi domains
def get_domain_and_endpoints(df) -> tuple[str, list[str]]:
    """Extract the unique domain and list of endpoints from the dataframe."""
    domain = (
        df.unique(pl.col("domain")).select(pl.col("domain")).item()
    )  # test as a list
    endpoints = df.unique(pl.col("end")).select(pl.col("end")).to_series().to_list()
    return domain, endpoints


def fetch_json(session, url) -> list | None:
    """Fetch JSON data from a URL using the provided session."""
    try:
        response = session.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
    except ValueError:
        print(f"Could not decode JSON from {url}")
    return None


def json_to_wide(json) -> pl.DataFrame | None:
    """Convert JSON response to a wide Polars DataFrame."""
    if json and len(json) == 2:
        x = dict(zip(json[0], json[1]))
        return pl.from_dict(x)
    return None


def json_to_long(json) -> pl.DataFrame | None:
    """Convert JSON response to a long Polars DataFrame."""
    if json and len(json) == 2:
        return pl.DataFrame({"var_name": json[0], "var_value": json[1]})
    return None


# WIP
# TODO: account for multiple domains
def fetch_all_data(
    domain: str | list, endpoints: str | list, format: str = "wide"
) -> dict:
    """Fetch data from all endpoints and return a dict of wide DataFrames."""
    all_data = {}
    with requests.Session() as session:
        for endpoint in endpoints:
            url = f"{domain}{endpoint}"
            json = fetch_json(session, url)

            if format == "long":
                long = json_to_long(json)
                long = long.with_columns(pl.lit(url).alias("url"))
                if long is not None:
                    all_data[url] = long
            elif format == "wide":
                wide = json_to_wide(json)
                if wide is not None:
                    all_data[url] = wide
    return all_data


# # testing
# endpoints = endpoints[0]

# # quick req
# resp = requests.get(f"{domain}{endpoints}")
# resp.raise_for_status()

# json = resp.json()

# # if we want wide

# # works for single column of values
# x = dict(zip(json[0], json[1]))
# x

# wide = pl.from_dict(x)
# wide


# # otherwise keep long and add call ids?

# long = pl.DataFrame(
#     {"var_name": json[0], "var_value": json[1], "call_id": "programmatic_id"}
# )

# long


# # hmm faster to have a list of wide dfs and loop through and
# # fire off the data somewhere?
# # or
# # keep em long with the call_id?
# # I guess it matter if the data is always gonna be one stupidly wide row


# # assume 1 domain - multi endpoints


# # load_dotenv()

# # API_TOKEN = os.getenv("API_TOKEN")

# df = acs.grab_file_targets()
# domain = df.unique(pl.col("domain")).select(pl.col("domain")).item()
# endpoints = df.unique(pl.col("end")).select(pl.col("end")).to_series().to_list()

# all_data = {}

# # create session
# with requests.Session() as session:
#     # session.headers.update({"Authorization": API_TOKEN})

#     for endpoint in endpoints:
#         url = f"{domain}{endpoint}"
#         try:
#             response = session.get(url)
#             response.raise_for_status()
#             json = response.json()
#             x = dict(zip(json[0], json[1]))
#             wide = pl.from_dict(x)
#             all_data[url] = wide
#         except requests.exceptions.RequestException as e:
#             print(f"Error fetching data from {url}: {e}")
#         except ValueError:
#             print(f"Could not decode JSON from {url}")

# for url, data in all_data.items():
#     print(f"\nData from {url}:")
#     print(data)

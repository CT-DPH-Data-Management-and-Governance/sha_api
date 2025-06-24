# def fetch_data(self) -> CensusData:  # Changed return type
#     """[SYNC] Fetches data and returns it as a CensusData object."""
#     print(f"[THREAD] Fetching data from: {self.dataset}")
#     try:
#         response = requests.get(self.full_url, timeout=30)
#         response.raise_for_status()
#         data = response.json()
#         if not data or len(data) < 2:
#             print(f"Warning: API for {self.dataset} returned no data.")
#             return CensusData(pl.DataFrame())
#         headers, records = data[0], data[1:]
#         df = pl.DataFrame(records, schema=headers, orient="row")
#         # Add a source column to identify where the data came from
#         df = df.with_columns(pl.lit(self.dataset).alias("source_dataset"))
#         return CensusData(df)  # Return the wrapped object
#     except requests.exceptions.HTTPError as e:
#         print(f"HTTP error for {self.dataset}: {e}")
#     except Exception as e:
#         print(f"An unexpected error for {self.dataset}: {e}")
#     return CensusData(pl.DataFrame())


# class CensusData:
#     """
#     A class to hold and provide an intelligent interface to the data
#     returned from a Census API call.

#     It wraps a Polars DataFrame and automatically parses column headers
#     into usable metadata.
#     """

#     _VARIABLE_SUFFIX_MAP = {
#         "E": "Estimate",
#         "M": "Margin of Error",
#         "P": "Percent Estimate",
#         "PM": "Percent Margin of Error",
#         "N": "Count",  # For some decennial tables
#     }

#     # Map for table prefixes to table types
#     _TABLE_TYPE_MAP = {
#         "B": "Detailed",
#         "C": "Collapsed",
#         "S": "Subject",
#         "DP": "Profile",
#     }

#     # Regex to capture different parts of a standard ACS/etc variable name
#     _VARIABLE_REGEX = re.compile(
#         r"^(?P<table>[A-Z]+\d+)_?(?P<column>C\d+)?_?(?P<line>\d{3})(?P<suffix>[A-Z]{1,2})$"
#     )

#     def __init__(self, data: pl.DataFrame):
#         if not isinstance(data, pl.DataFrame):
#             raise TypeError("Input data must be a Polars DataFrame.")
#         self.data = data
#         self.variable_metadata = self._parse_headers()

#     def _parse_headers(self) -> pl.DataFrame:
#         """Parses the DataFrame column headers into a metadata DataFrame."""
#         records = []
#         for col_name in self.data.columns:
#             # Initialize with new table_type field
#             meta = {
#                 "original_name": col_name,
#                 "type": "Identifier",
#                 "table_id": None,
#                 "table_type": None,
#                 "value_type": None,
#             }

#             match = self._VARIABLE_REGEX.match(col_name)
#             if match:
#                 parts = match.groupdict()
#                 table_id = parts["table"]
#                 meta["type"] = "Variable"
#                 meta["table_id"] = table_id
#                 meta["value_type"] = self._VARIABLE_SUFFIX_MAP.get(
#                     parts["suffix"], "Unknown"
#                 )

#                 if table_id:
#                     # Check for two-letter prefixes first (like DP)
#                     if table_id[:2] in self._TABLE_TYPE_MAP:
#                         meta["table_type"] = self._TABLE_TYPE_MAP[table_id[:2]]
#                     # Fallback to single-letter prefixes
#                     elif table_id[0] in self._TABLE_TYPE_MAP:
#                         meta["table_type"] = self._TABLE_TYPE_MAP[table_id[0]]
#                     else:
#                         meta["table_type"] = "Other"

#             # Simple check for common geo identifiers
#             elif col_name in ["NAME", "GEO_ID"] or col_name.startswith(
#                 ("state", "county", "us", "ucgid")
#             ):
#                 meta["type"] = "Geography"

#             records.append(meta)
#         return pl.DataFrame(records, orient="row")

#     def get_estimates(self) -> pl.DataFrame:
#         """Returns a view of the data containing only estimate columns."""
#         estimate_cols = self.variable_metadata.filter(
#             pl.col("value_type") == "Estimate"
#         )["original_name"].to_list()

#         geo_cols = self.variable_metadata.filter(pl.col("type") == "Geography")[
#             "original_name"
#         ].to_list()

#         return self.data.select(geo_cols + estimate_cols)

#     def __repr__(self) -> str:
#         return f"<CensusData: {self.data.shape[0]} rows, {self.data.shape[1]} columns>\n{self.data.__repr__()}"

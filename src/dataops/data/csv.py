from pathlib import Path

import polars as pl


def csv_wrangle(targetfile: Path | str) -> pl.LazyFrame:
    """Wrangle starter .csv for creating api endpoints and features.

    Args:
        target_file: The path to the file to be processed.

    Returns:
        A polars LazyFrame.

    Raises:
        FileNotFoundError: If `target_file` does not exist.
        IOError: For general I/O related errors.

    """
    if not isinstance(targetfile, Path):
        targetfile = Path(targetfile)

    if not Path.exists(targetfile):
        raise FileNotFoundError(f"File path does not exist: {targetfile}")

    if not targetfile.suffix == ".csv":
        raise IOError(f"Expected a csv file. Found file type: {targetfile.suffix}")

    # regex patterns
    group_pattern = r"\((.*)\)"
    ucgid_pattern = r"&ucgid=(.*)$"
    domain_pattern = r"^(.+gov)"

    targets = (
        pl.scan_csv(targetfile)
        .with_columns(
            pl.col("url").str.slice(28, 4).cast(pl.Int16).alias("year"),
            pl.col("url").str.slice(40, 1).cast(pl.Int16).alias("acs_type"),
            pl.col("url").str.extract(group_pattern).alias("group"),
            pl.col("url").str.extract(ucgid_pattern).alias("ucgid"),
            pl.col("url").str.extract(domain_pattern).alias("domain"),
            pl.col("url").str.replace(domain_pattern, "").alias("end"),
        )
        .with_columns(
            pl.when(pl.col("group").str.starts_with("B"))
            .then(pl.lit("detailed"))
            .when(pl.col("group").str.starts_with("C"))
            .then(pl.lit("collapsed"))
            .when(pl.col("group").str.starts_with("S"))
            .then(pl.lit("subject"))
            .when(pl.col("group").str.starts_with("DP"))
            .then(pl.lit("data-profile"))
            .otherwise(pl.lit("unknown"))
            .alias("table_type"),
        )
    )

    return targets

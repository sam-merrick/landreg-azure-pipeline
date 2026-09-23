"""
Profile the HM Land Registry Price Paid source files.

Confirms the column schema against the raw files (the published guidance
and the report builder output disagree, so the files are treated as the
source of truth) and profiles record status, transaction ID behaviour,
nullability, categorical values, volume and data quality.

Findings feed the design decisions recorded in docs/architecture.md.

Expects the following in data/ (untracked):
    pp-complete.csv         complete file, 1995 to present
    pp-monthly-2026-07.csv  monthly change-only file

Usage:
    python src/inspect_source_data.py
"""

from pathlib import Path

import duckdb

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
PP_MONTHLY_PATH = DATA_DIR / "pp-monthly-2026-07.csv"
PP_COMPLETE_PATH = DATA_DIR / "pp-complete.csv"

COLUMNS = {
    "transaction_id": "VARCHAR",
    "price": "VARCHAR",
    "date_of_transfer": "VARCHAR",
    "postcode": "VARCHAR",
    "property_type": "VARCHAR",
    "old_or_new": "VARCHAR",
    "duration": "VARCHAR",
    "paon": "VARCHAR",
    "saon": "VARCHAR",
    "street": "VARCHAR",
    "locality": "VARCHAR",
    "town_city": "VARCHAR",
    "district": "VARCHAR",
    "county": "VARCHAR",
    "category_type": "VARCHAR",
    "record_status": "VARCHAR"
}

CATEGORY_COLUMNS = ("property_type", "old_or_new", "duration", "category_type")

def heading(text: str) -> None:
    """Print a labelled heading above a result set."""
    print(f"\n=== {text} ===")

def get_connection() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection."""
    return duckdb.connect()


def register_source(con: duckdb.DuckDBPyConnection, path: Path, view_name: str) -> None:
    """Read a headerless CSV as raw text and register it as a named view."""
    con.read_csv(str(path), header=False, columns=COLUMNS).create_view(view_name)


def preview_rows(con: duckdb.DuckDBPyConnection, view_name: str, limit: int = 5) -> None:
    """Print the first rows of the given view."""
    heading(f"Preview: {view_name}")
    query = f"SELECT * FROM {view_name} LIMIT ?"
    con.sql(query, params=[limit]).show()


def profile_record_status(con: duckdb.DuckDBPyConnection, view_name: str) -> None:
    """Report distinct values in record_status, displaying counts and percentages"""
    heading(f"Record status: {view_name}")
    query = f"""
    SELECT
        record_status,
        COUNT(*) AS count,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM {view_name}) , 2) AS pct
    FROM {view_name}
    GROUP BY record_status
    ORDER BY count DESC
    """
    con.sql(query).show()


def profile_transaction_id(con: duckdb.DuckDBPyConnection, view_name: str) -> None:
    """Report whether the transaction_id is unique within the file."""
    heading(f"Transaction ID uniqueness: {view_name}")
    query = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT transaction_id) AS distinct_transaction_ids
    FROM {view_name}
    """
    con.sql(query).show()


def profile_id_overlap(con: duckdb.DuckDBPyConnection, change_view: str, snapshot_view: str) -> None:
    """Report, per record status, how many change-file IDs exist in the snapshot."""
    heading(f"ID overlap: {change_view} against {snapshot_view}")
    query = f"""
    SELECT
        c.record_status,
        COUNT(*) AS change_rows,
        COUNT(s.transaction_id) AS found_in_snapshot,
        COUNT(*) - COUNT(s.transaction_id) AS not_in_snapshot
    FROM {change_view} AS c
    LEFT JOIN {snapshot_view} AS s 
        ON s.transaction_id = c.transaction_id
    GROUP BY c.record_status
    ORDER BY c.record_status
    """
    con.sql(query).show()


def profile_nullability(con: duckdb.DuckDBPyConnection, view_name: str, columns: list[str]) -> None:
    """Report NULL counts and percentages for each column."""
    heading(f"Nullability: {view_name}")
    null_counts = ",\n".join(
        f"COUNT(*) FILTER (WHERE {column} IS NULL) AS {column}"
for column in columns
    )
    query = f"""
    WITH counts AS (
        SELECT
            COUNT(*) AS _total_rows,
            {null_counts}
        FROM {view_name}
    )
    SELECT
        "column",
        null_cnt,
        printf('%.2f%%', 100.0 * null_cnt / NULLIF(_total_rows, 0)) AS null_pct
    FROM (
        UNPIVOT counts
        ON COLUMNS(* EXCLUDE (_total_rows))
        INTO NAME "column" VALUE null_cnt
    )
    """
    con.sql(query).show()


def profile_category(con: duckdb.DuckDBPyConnection, view_name: str, column: str) -> None:
    """Report distinct values and counts for a categorical column."""
    heading(f"Category values: {column} ({view_name})")
    query = f"""
    SELECT
        {column},
        COUNT(*) AS count
    FROM {view_name}
    GROUP BY {column}
    ORDER BY count DESC
    """
    con.sql(query).show()


def profile_year_distribution(con: duckdb.DuckDBPyConnection, view_name: str) -> None:
    """Report total row count and row count grouped by year of date_of_transfer."""
    heading(f"Rows per year: {view_name}")
    query = f"""
    SELECT
        YEAR(CAST(date_of_transfer AS TIMESTAMP)) AS year,
        COUNT(*) AS row_count
    FROM {view_name}
    GROUP BY year
    ORDER BY year
    """
    con.sql(query).show()
    con.sql(f"""SELECT COUNT(*) AS total_rows FROM {view_name}""").show()


def profile_price_outliers(con: duckdb.DuckDBPyConnection, view_name: str) -> None:
    """Report rows that are either zero, implausibly low or implausibly high."""
    heading(f"Price outliers: {view_name}")
    query = f"""
    SELECT
        COUNT(*) FILTER (WHERE CAST(price AS INTEGER) <= 100) AS implausibly_low,
        COUNT(*) FILTER (WHERE CAST(price AS INTEGER) >= 100000000) AS implausibly_high
    FROM {view_name}
    """
    con.sql(query).show()


def profile_date_outliers(con: duckdb.DuckDBPyConnection, view_name: str) -> None:
    """Report any dates that are outside a sensible range - before 1995 or in the future."""
    heading(f"Date outliers: {view_name}")
    query = f"""
    SELECT
        COUNT(*) FILTER (
            WHERE YEAR(CAST(date_of_transfer AS TIMESTAMP)) < 1995
        ) AS before_1995,
        COUNT(*) FILTER (
            WHERE CAST(date_of_transfer AS TIMESTAMP) > CURRENT_DATE
        ) AS in_future
    FROM {view_name}
    """
    con.sql(query).show()


def profile_postcode(con: duckdb.DuckDBPyConnection, view_name: str) -> None:
    """Report postcodes that are null or do not match a plausible UK format."""
    heading(f"Postcode format: {view_name}")
    query = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(*) FILTER (WHERE postcode IS NULL) AS null_postcode,
        COUNT(*) FILTER (
            WHERE postcode IS NOT NULL 
            AND NOT regexp_matches(
                UPPER(TRIM(postcode)),
                '^[A-Z]{{1,2}}[0-9][0-9A-Z]?[ ]?[0-9][A-Z]{{2}}$'
            )
        ) AS malformed_postcodes
    FROM {view_name}
    """
    malformed_query = f"""
    SELECT postcode, town_city, county
    FROM complete
    WHERE postcode IS NOT NULL
    AND NOT regexp_matches(UPPER(TRIM(postcode)), '^[A-Z]{{1,2}}[0-9][0-9A-Z]?[ ]?[0-9][A-Z]{{2}}$')
    """
    con.sql(query).show()
    con.sql(malformed_query).show()

def profile_address(con: duckdb.DuckDBPyConnection, view_name: str, columns: tuple[str]) -> None:
    """Report row counts where town, county or district is 'UNKNOWN'."""
    heading(f"UNKNOWN check: {view_name}")
    sentinel_counts = ",\n".join(
        f"COUNT(*) FILTER (WHERE {column} = 'UNKNOWN') AS {column}"
for column in columns
    )
    query = f"""
    SELECT
        {sentinel_counts}
    FROM {view_name}
    """
    con.sql(query).show()


def main() -> None:
    con = get_connection()
    register_source(con, PP_MONTHLY_PATH, "monthly")
    register_source(con, PP_COMPLETE_PATH, "complete")

    preview_rows(con, "monthly")
    preview_rows(con, "complete")

    profile_record_status(con, "monthly")
    profile_record_status(con, "complete")

    profile_transaction_id(con, "monthly")
    profile_transaction_id(con, "complete")
    profile_id_overlap(con, "monthly", "complete")

    profile_nullability(con, "monthly", COLUMNS)
    profile_nullability(con, "complete", COLUMNS)

    for column in CATEGORY_COLUMNS:
        profile_category(con, "complete", column)

    profile_year_distribution(con, "complete")
    profile_price_outliers(con, "complete")
    profile_date_outliers(con, "complete")
    profile_postcode(con, "complete")
    profile_address(con, "complete", ("town_city", "county", "district"))


if __name__ == "__main__":
    main()
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

def get_connection() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection."""
    return duckdb.connect()


def register_source(con: duckdb.DuckDBPyConnection, path: Path, view_name: str) -> None:
    """Read a headerless CSV as raw text and register it as a named view."""
    con.read_csv(str(path), header=False, columns=COLUMNS).create_view(view_name)


def preview_rows(con: duckdb.DuckDBPyConnection, view_name: str, limit: int = 5) -> None:
    """Print the first rows of the given view."""
    query = f"SELECT * FROM {view_name} LIMIT ?"
    con.sql(query, params=[limit]).show()


def profile_nullability(con: duckdb.DuckDBPyConnection, view_name: str) -> None:
    """Report NULL vs empty-string counts for the SAON column."""
    query = f"""
    SELECT
        COUNT(*) FILTER (WHERE saon IS NULL) AS nulls,
        COUNT(*) FILTER (WHERE saon = '') AS empties
    FROM {view_name}
    """
    con.sql(query).show()


def profile_record_status(con: duckdb.DuckDBPyConnection, view_name: str) -> None:
    """Report distinct values in record_status, displaying counts and percentages"""
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
    query = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT transaction_id) AS distinct_transaction_ids
    FROM {view_name}
    """
    print(view_name)
    con.sql(query).show()


def profile_id_overlap(con: duckdb.DuckDBPyConnection, change_view: str, snapshot_view: str) -> None:
    """Report, per record status, how many change-file IDs exist in the snapshot."""
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
    print(f"ID overlap: {change_view} against {snapshot_view}")
    con.sql(query).show()


def nullability_checks(con: duckdb.DuckDBPyConnection, view_name: str, columns: dict) -> None:
    """Report how many rows are NULL for each column."""

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


def main() -> None:
    con = get_connection()
    register_source(con, PP_MONTHLY_PATH, "monthly")
    register_source(con, PP_COMPLETE_PATH, "complete")
    '''
    preview_rows(con, "monthly")
    profile_nullability(con, "monthly")
    profile_record_status(con, "monthly")
    register_source(con, PP_COMPLETE_PATH, "complete")
    preview_rows(con, "complete")
    profile_record_status(con, "complete")
    profile_transaction_id(con, "monthly")
    profile_transaction_id(con, "complete")
    profile_id_overlap(con, "monthly", "complete")
    '''
    nullability_checks(con, "monthly", COLUMNS)


if __name__ == "__main__":
    main()
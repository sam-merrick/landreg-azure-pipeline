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
PP_COMPLETE_PATH = DATA_DIR / "pp-monthly-2026-07.csv"
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


def preview_rows(con: duckdb.DuckDBPyConnection, view_name: str, limit: int = 10) -> None:
    """Print the first rows of the given view."""
    query = f"SELECT paon, saon, street, locality FROM {view_name} LIMIT ?"
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
    

def main() -> None:
    con = get_connection()
    register_source(con, PP_MONTHLY_PATH, "monthly")
    preview_rows(con, "monthly")
    profile_nullability(con, "monthly")


if __name__ == "__main__":
    main()
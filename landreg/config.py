"""
Configuration for the Land Registry pipeline.

Catalog is resolved from the LANDREG_ENV environment variable and both
the catalog and the checkpoint paths derive from it. Defaults to dev.
"""

import os

# --- Catalog and schemas -------------------------------------------------

ENVIRONMENT = os.environ.get("LANDREG_ENV", "dev")
CATALOG = f"landreg_{ENVIRONMENT}"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
OPS_SCHEMA = "ops"

# --- Storage -------------------------------------------------------------

STORAGE_ACCOUNT = "stlandregdevuks"

LANDING_CONTAINER = "landing"
BRONZE_CONTAINER = "bronze"
METADATA_CONTAINER = "metadata"


def abfss(container: str, path: str = "") -> str:
    """Build an ADLS Gen2 URL for a container and optional path."""
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


# --- Source paths --------------------------------------------------------

PRICE_PAID_ROOT = "price-paid"
COMPLETE_PATH = abfss(LANDING_CONTAINER, f"{PRICE_PAID_ROOT}/complete/")
MONTHLY_PATH = abfss(LANDING_CONTAINER, f"{PRICE_PAID_ROOT}/monthly/")

# --- Checkpoints ---------------------------------------------------------

CHECKPOINT_ROOT = abfss(METADATA_CONTAINER, f"{ENVIRONMENT}/checkpoints/")


def table(schema: str, name: str) -> str:
    """Build a fully qualified Unity Catalog table name."""
    return f"{CATALOG}.{schema}.{name}"
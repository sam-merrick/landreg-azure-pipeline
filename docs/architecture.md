# Architecture

## Naming conventions

Resources follow the Microsoft Cloud Adoption Framework pattern:

`<resource-type>-<workload>-<environment>-<region>`

- **workload**: `landreg`
- **environment**: `dev` / `prod`
- **region**: `uks` (UK South)

| Resource | Abbrev. | Dev name |
|---|---|---|
| Resource group | `rg` | `rg-landreg-dev-uks` |
| Storage account | `st` | `stlandregdevuks` |
| Key Vault | `kv` | `kv-landreg-dev-uks` |
| Data Factory | `adf` | `adf-landreg-dev-uks` |
| Databricks workspace | `dbw` | `dbw-landreg-dev-uks` |

Storage account names drop the hyphens: Azure restricts them to 3–24 lowercase 
alphanumeric characters.

## Storage layout

| Container | Purpose |
|---|---|
| landing | Raw source files, as downloaded. Immutable. |
| bronze | Delta. Parsed, typed, and unfiltered. |
| silver | Delta. Cleaned, deduplicated, conformed. |
| gold | Delta, dimensional model. |
| metadata | Control tables, run log, auto loader checkpoints. |

Hierarchical namespace is enabled, making this ADLS Gen2 rather than flat
blob storage. This provides true directories (enabling directory-scoped
access control and atomic renames) and improves Spark performance when
reading partitioned data.

`landing` and `bronze` are deliberately separate. Landing holds source
files byte-for-byte as received and is never modified, so it serves as an
immutable replay source: if a parsing or typing bug is found in bronze
later, bronze can be rebuilt from landing without re-fetching from the
Land Registry. Bronze is disposable; landing is not.

Each monthly change file must be archived in `landing` on arrival under a
dated path. HM Land Registry publishes a single monthly file and replaces
it, so a file not captured when published cannot be retrieved later. The
replay guarantee depends on this.

## Source data

### Schema

The column order published in the GOV.UK guidance was verified against 
the raw monthly file. The price paid report builder emits different column 
order and set (swaps PAON and SAON, substitutes a linked-data URI for a 
record_status), so it is not a valid reference for the bulk files. Schema 
is verified against the files rather than taken from the documentation.

### Null handling

Empty fields arrive in the source as quoted empty strings, which DuckDB's 
reader normalises to NULL. This is reader behaviour rather than a property 
of the file, so it must be re-verified when ingesting with Spark.

## Design decisions

### Handling deleted records

Source monthly files carry a record status of A (addition), C (change) or
D (deletion). Deletions are a small minority of rows but must be handled
explicitly.

**Options considered:** physically remove the row from silver, or retain
it with an `is_deleted` flag.

**Decision:** soft delete.

**Reasoning:** physical deletion loses the record of the deletion itself: 
if a transaction is removed and later re-registered under the same identifier,
a soft delete preserves that sequence where a hard delete makes it indistinguishable 
from a first-time insert. Land Registry deletions frequently reflect corrections 
rather than genuine removals, so this is a plausible real scenario.

Soft delete is also the reversible choice — records can be physically
purged later if required, but deleted rows cannot be recovered.

**Consequences:** gold layer views must filter on `is_deleted` so deleted
transactions do not appear in reporting. Silver row counts will exceed the
count of live transactions. A purge process may be needed if retention
policy later requires it.

### Handling changed records

**Decision:** merge on transaction identifier — update where the row
exists, insert where it does not.

**Reasoning:** a change record for an identifier not present in silver
should not fail or be discarded. Treating it as an insert makes the load
idempotent and tolerant of gaps in the change file sequence. This is the
standard upsert semantic and maps directly onto Delta Lake's MERGE.

**Consequences:** a high rate of changes arriving for unknown identifiers
would indicate an incomplete backfill, so this is worth monitoring rather
than silently absorbing.
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

Storage account names drop the hyphens: Azure restricts them to 3–24 lowercase alphanumeric characters.

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
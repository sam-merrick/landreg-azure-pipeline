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
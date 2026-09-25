# Environment setup

Steps to provision the Azure and Databricks environment from scratch.

## Azure resources

1. Resource group `rg-landreg-dev-uks` in UK South, tagged
   `project=landreg`, `environment=dev`.
2. Storage account `stlandregdevuks` — Standard, LRS, Hot tier,
   **hierarchical namespace enabled** (cannot be changed after creation).
   Disable anonymous blob access, require secure transfer, TLS 1.2.
3. Containers: `landing`, `bronze`, `silver`, `gold`, `metadata`.
4. Key Vault `kv-landreg-dev-uks`, RBAC permission model, 7-day soft
   delete, purge protection disabled.
5. Access Connector for Azure Databricks `ac-landreg-dev-uks`.
6. Grant the access connector **Storage Blob Data Contributor** on the
   storage account (Access Control (IAM) → Add role assignment → Managed
   identity).
7. Grant yourself the same role to use Storage Explorer.

Note: the subscription must be pay-as-you-go. Free trial subscriptions
with a spending limit cannot provision VMs, so Databricks clusters fail
with a capacity error.

## Databricks

1. Workspace `dbw-landreg-dev-uks`, **Premium** tier, **Hybrid** workspace
   type (serverless workspaces cannot be converted to hybrid later).
2. Unity Catalog is enabled automatically with a metastore provisioned.
3. Storage credential from the access connector's resource ID.
4. External locations, one per container:
   `abfss://<container>@stlandregdevuks.dfs.core.windows.net/`
   Force create when the file events warning appears.
5. Catalog `landreg_dev` with a managed location.
6. Schemas `bronze`, `silver`, `gold`, `ops`, each with a storage location
   at `abfss://<container>@.../dev/`.
7. Cluster: single node, latest LTS runtime, Photon off,
   **terminate after 10 minutes**.

## Git integration

1. Databricks → Settings → Linked accounts → link GitHub.
2. Workspace → Users → your user → Create → Git folder, pointing at the
   repo URL.
3. Switch to the working branch via the branch control on the Git folder.

## Local data

Source files are not in version control. Download from GOV.UK Price Paid
Data and upload to `landing` via Azure Storage Explorer:

    price-paid/complete/pp-complete-2026-07.csv
    price-paid/monthly/year=2026/month=07/pp-monthly-2026-07.csv

## Known issues

- Databricks puts the **Git folder root** on `sys.path`, not `src/`. The
  package must be at the repo root for imports to resolve in notebooks.
- Quota errors ("no available compute capacity") on a converted
  subscription may be stale portal state. Try creating the cluster before
  requesting a quota increase.
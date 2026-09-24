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

The complete file also has 16 columns and aligns with the monthly file. In the
complete file the record_status is uniformly "A" because a snapshot has no change
semantics to express.

### Null handling

Empty fields arrive in the source as quoted empty strings, which DuckDB's 
reader normalises to NULL. This is reader behaviour rather than a property 
of the file, so it must be re-verified when ingesting with Spark.

### Transaction identifier

`transaction_id` is unique and never null in both files. C (change) records carry
existing IDs which indicates the ID is stable across corrections. Strong evidence
from one file; this is to be confirmed against the next monthly release.

### Snapshot timing

The complete file reflects the state after the latest monthly file has been applied.
Additions and changes from July are present; July's deletions are absent rather than
flagged. Established by an ID overlap check: additions and changes matched, deletions
did not.

### Nullability

Columns fall into three groups:

**Never null** — `transaction_id`, `price`, `date_of_transfer`,
`property_type`, `old_or_new`, `duration`, `town_city`, `district`,
`county`, `category_type`, `record_status`. Consistent across both files.

**Structurally optional** — `saon` (88% null) and `locality` (38% null).
Null here means "not applicable" rather than "missing": SAON only applies
to sub-divided properties such as flats.

**Occasionally missing** — `postcode` (0.16%), `street` (1.6%),
`paon` (0.01%). Genuine gaps in otherwise expected data.

The distinction matters: the second group should never be treated as a
quality failure, while the third represents real absence.

### Categorical values

`property_type` (T, S, D, F, O), `old_or_new` (Y, N) and `category_type`
(A, B) match the published guidance.

`duration` contains a third value, `U`, on 532 rows. This is not
documented in the GOV.UK guidance, which lists only F (freehold) and
L (leasehold). At 0.0017% of rows it is numerically negligible, but its
existence confirms that categorical values must be validated against a
known set rather than trusted.

### Volume and distribution

31,525,946 rows in the complete file, spanning 1995 to the present.
Annual volume is broadly stable at roughly 0.8–1.3 million rows per year.
The current year is partial, reflecting both the incomplete year and the
two-week to two-month lag between sale completion and registration.

### Data quality

`price` casts cleanly to integer across all rows — no non-numeric values
are present. `date_of_transfer` contains no dates before 1995 or in the
future, and arrives as a timestamp with a zero time component.

Price outliers: 709 rows at £100 or below, 371 rows at £100 million or
above. Both are plausible real transactions rather than errors — nominal
transfers between related parties at the low end, large commercial or
portfolio transactions at the high end.

Postcodes are well-formed: one row in 31.5 million fails a UK postcode
pattern, carrying the literal string `UNKNOWN`. A check confirmed this
sentinel does not appear in `town_city`, `district` or `county`. The check
targeted this specific sentinel in uppercase; a broader scan for other
placeholder conventions was not performed.

## Design decisions

### Source reader

**Decision:** a single reader handles both the complete file and the
monthly change files.

**Reasoning:** both carry the same 16 columns in the same order, so no
schema reconciliation is needed between the backfill and incremental
paths. Record status is present in both, uniformly "A" in the complete
file.

**Consequences:** a future change to either file's layout would break
both paths. Column count and order should be asserted at read time rather
than assumed.

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
policy later requires it. The deletion audit trail begins at the backfill date, and 
transactions deleted before it are absent with no record.

### Merge behaviour

**Decision:** merge on `transaction_id` — with behaviour dependent on
record status.

| Status | Matched | Not matched |
|---|---|---|
| A | update | insert |
| C | update | insert |
| D | set `is_deleted` | ignore and log count |

**Reasoning:** inserting on no-match makes additions and changes
idempotent and tolerant of gaps in the change file sequence. Deletions
are the exception: a deletion for an unknown identifier must never be
inserted, since that would create a row the source says should not
exist. These are logged rather than silently discarded, because they
indicate either a re-applied file or a load sequence that is out of sync.
Additions are treated as updates when matched so that re-applying a file
already reflected in the target is a no-op rather than an error.

**Consequences:** a high rate of changes arriving for unknown identifiers
would indicate an incomplete backfill, so this is worth monitoring rather
than silently absorbing.

### Record status in silver

Record status describes how a row was delivered rather than a property of
the transaction itself. As noted above, it is uniformly "A" in the complete file.

**Decision:** record status is consumed during the silver load — it drives
whether a row is inserted, updated or flagged deleted — but is not retained
as a silver column. Its effect persists as `is_deleted`.

**Also considered:** deriving a last-operation or correction-count field to
track how often a transaction has been amended. Decided against it as the gold
model does not require change history.

### Backfill and incremental start

The complete file is the backfill, representing state as of the latest monthly 
release. The first new incremental load is the following month. Re-applying the 
already-included monthly file is expected to be a no-op, and serves as the first 
idempotency test.

### Not-null constraints

**Decision:** enforce not-null on the eleven columns observed to be
complete in both files. `saon`, `locality`, `postcode`, `street` and
`paon` remain nullable.

**Reasoning:** these constraints encode an observed property of the source
rather than an assumption. A null arriving in one of them indicates the
source has changed shape, which should surface as a quality failure rather
than pass through silently.

**Consequences:** a genuine source change would quarantine rows rather
than corrupt downstream data. The constraint set should be revisited if
the source schema changes.

### Sentinel value handling

**Decision:** normalise the literal string `UNKNOWN` in `postcode` to NULL
during the silver load. Do not quarantine the row.

**Reasoning:** the transaction itself is valid; only the postcode is
unknown. Leaving the sentinel in place would mean "missing" is represented
two ways — NULL and a magic string — and any cleaning that handles only
NULL would let it through into gold as though it were a real postcode.

**Consequences:** scoped to `postcode`, as the sentinel was not found
elsewhere. Should be revisited if other placeholder conventions appear.

### Outlier handling

**Decision:** flag price outliers rather than quarantine them.

**Reasoning:** a transaction with an unusual price is still a valid
transaction. Removing nominal transfers and very large commercial sales
would silently distort any average-price analysis in gold. Flagging leaves
the exclusion decision to the consumer.

**Consequences:** gold consumers must choose whether to filter on the flag.
Aggregations that do not exclude outliers will include genuine but atypical
transactions.

### Categorical validation

**Decision:** validate `property_type`, `old_or_new`, `duration` and
`category_type` against a known set of codes. Rows with unrecognised values
are flagged, not rejected.

**Reasoning:** `duration` already contains an undocumented value, so the
published code lists are demonstrably incomplete. Rejecting unknown codes
would discard valid transactions; flagging surfaces them for review while
letting the load proceed.

**Consequences:** the known-code sets need updating as new values appear.
A rising flag rate indicates the source has introduced codes not yet
accounted for.

### Partitioning

**Decision:** partition silver and gold by year of `date_of_transfer`.

**Reasoning:** annual volume is stable at roughly one million rows per
year, so year-based partitions are evenly sized and avoid the stragglers
that skewed partitions cause in Spark. Year is also the most common filter
predicate for property price analysis, so partition pruning will be
effective.

**Consequences:** roughly 32 partitions, growing by one per year. The
current year's partition is smaller than the rest until the year completes.

### Storage access

**Decision:** Databricks authenticates to ADLS via an Access Connector
managed identity, granted Storage Blob Data Contributor on the storage
account. No account keys or SAS tokens are used.

**Reasoning:** managed identity means no secret exists to be rotated,
leaked, or committed. The alternative — a storage account key in a
notebook or secret scope — creates a credential that must be managed and
that grants full access to the account if exposed.

**Consequences:** access is granted at storage account scope rather than
per container, so the connector can read and write every layer. Narrower
per-container role assignments would be appropriate in production.

### External locations

**Decision:** a separate Unity Catalog external location per container
rather than one at the storage account root.

**Reasoning:** external locations are the unit of permission granting in
Unity Catalog. Per-container locations allow different grants per layer —
for example read-only on `landing`, or restricting `gold` to a reporting
group — without restructuring later. A single root location would make
every layer share one permission boundary.

**Consequences:** five objects to maintain rather than one. New containers
need a corresponding external location before they can be used.

### Auto Loader file discovery

**Decision:** directory listing rather than file events.

**Reasoning:** file notification requires granting the access connector
Storage Account Contributor and Event Grid permissions, which are
considerably broader than the Storage Blob Data Contributor needed to read
and write data. At one file per month in a partitioned path, listing cost
is negligible and the optimisation does not justify the additional
privilege.

**Consequences:** ingestion would need revisiting if file volume grew by
orders of magnitude.
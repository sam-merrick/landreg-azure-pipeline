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

Empty fields arrive in the source as quoted empty strings. Both DuckDB and
Spark normalise these to NULL on read. This is reader behaviour rather than
a property of the file, so bronze stores NULL and no empty-string handling
is needed downstream.

### Transaction identifier

`transaction_id` is unique and never null in both files. C (change) records carry
existing IDs which indicates the ID is stable across corrections. Strong evidence
from one file. Confirmed against the August 2026 file: 370 change records reference
transaction identifiers also present in the July file, so identifiers are
stable across corrections rather than reissued.

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
transactions deleted before it are absent with no record. Verified against the August 
file: 1,488 of 1,514 deletion records matched existing silver rows and set the flag. 
The remaining 26 referenced identifiers not present in silver and were ignored, as designed.

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

### Catalog and environment separation

**Decision:** one catalog per environment (`landreg_dev`, `landreg_prod`),
with medallion layers as schemas beneath. Each schema has its own managed
storage location under a `dev/` or `prod/` path within the matching
container.

**Reasoning:** promoting code between environments changes only the catalog
name, leaving every table reference unchanged. Path-level separation keeps
dev and prod writes physically apart within the same storage account.

**Consequences:** separate storage accounts per environment would give
stronger isolation — independent firewall rules, access policies and cost
attribution — and would be the production choice. Path separation is
sufficient here given a single developer and one subscription.

### Bronze layer design

1. Two bronze tables — bronze.price_paid_complete and bronze.price_paid_monthly. 
   Different ingestion mechanics (one-off batch read versus a streaming directory 
   watch), different lifecycles, and the table name carries provenance without 
   needing a flag.

2. Three ingestion metadata columns — source filename, ingestion timestamp, and 
   a run ID generated once per execution and stamped on every row that run writes. 
   The run ID is what makes "undo run X" possible and links bronze rows to the run log.
   `ingestion_timestamp` is UTC, matching the cluster session timezone. This avoids DST 
   ambiguity when comparing or ordering loads.

3. No partitioning — bronze is append-only, monthly writes are ~100k rows, and it is 
   not queried by transfer date. Partitioning would create tiny files per partition 
   for no benefit.

4. Append — bronze never overwrites. Duplicates are tolerable because silver merges on 
   transaction ID, which makes re-runs safe. This does mean silver must deduplicate 
   before merging, since Delta errors when a MERGE matches the same target row twice.

5. The batch loader is not idempotent — re-running it appends the source
   again. This is acceptable because the backfill runs once, and silver
   merges on transaction ID so duplicates resolve. The stream loader is
   idempotent via its Auto Loader checkpoint.

### Metadata-driven ingestion

**Decision:** source configuration lives in a control table
(`ops.control`) rather than in code. Loaders take a source name, look up
the row, and derive their source path and target table from it.

**Reasoning:** adding a source becomes an INSERT rather than a new
function and notebook. The two loaders are generic — one handles any
batch source, the other any streaming source — so the code does not grow
with the number of sources.

Paths and table names are stored relative (`price-paid/complete/`,
`price_paid_complete`) rather than fully qualified. The storage account
and catalog are environment-specific and come from configuration, so the
same control table rows are valid in both dev and prod.

**Consequences:** a misconfigured row fails at lookup rather than at load,
which is the intended behaviour — `get_source_config` raises if the source
is absent or disabled rather than silently doing nothing. The control
table is currently seeded manually via a setup notebook.

### Run logging

**Decision:** every pipeline execution writes a row to `ops.run_log` at
start and updates it on completion, recording run ID, source, start and
end timestamps, rows written, status and error detail.

**Reasoning:** the run ID stamped on bronze rows joins back to this table,
so any row can be traced to the execution that wrote it. Logging the start
separately from the completion means an execution that dies outright —
cluster failure, killed job — leaves a row stuck at `running`, which is
distinguishable from one that failed and recorded why.

Loaders wrap their work in try/except: a failure updates the log to
`failed` with the exception message, then re-raises so the job itself
fails rather than appearing to succeed.

**Consequences:** the log is updated in place, so Delta rewrites files on
each completion. Acceptable at this frequency; a higher-volume pipeline
would use append-only state transitions instead.

Row counts are taken from Delta's `operationMetrics` for the batch loader
and from the streaming query's progress records for the stream loader,
rather than counting the DataFrame. Counting would trigger a second full
read of the source. The metrics approach assumes no concurrent writer to
the target table, which holds for a single-writer pipeline.

### Ingestion metadata as two functions

**Decision:** provenance columns are added by two functions —
`add_ingestion_metadata` for run ID and timestamp, `add_source_filename`
for the file path.

**Reasoning:** `_metadata.file_path` only resolves on file-based
DataFrames, so a function using it cannot be unit tested against a
DataFrame built in memory. Splitting it out leaves `add_ingestion_metadata`
genuinely pure and testable, and isolates the file dependency in a single
line documented as requiring a file-based source.

**Consequences:** loaders call both. The filename function is verified by
running the pipeline rather than by unit test.

### Silver layer design

**Tables:** `silver.price_paid_transactions` holds the merged current state,
partitioned by `transfer_year` (a Delta generated column derived from
`date_of_transfer`). `silver.price_paid_quarantine` holds rows that failed a
fatal rule, with all source columns retained as strings.

**Typing:** `price` to BIGINT and `date_of_transfer` to DATE. Everything else
stays string. `try_cast` is used rather than `cast` — Databricks runs in ANSI
mode, where a plain cast raises on malformed input and would fail the entire
load on one bad value. The original strings are preserved as `price_raw` and
`date_of_transfer_raw` so a quarantined row retains what actually arrived,
since by definition its cast value is null.

**Fatal rules:** a row is quarantined when `transaction_id`, `price` or
`date_of_transfer` is null after casting. These three cannot be absent from a
usable transaction: the first is the merge key, and the other two are the
record. Every other quality problem is flagged rather than rejected.

**Metadata:** `first_seen` and `last_seen` are tracked separately. A merge
updates `last_seen` and deliberately leaves `first_seen` untouched, so a row
retains when it was first observed regardless of how often it is corrected.

### Deduplication before merge

**Decision:** the merge source is reduced to one row per `transaction_id`,
keeping the latest by `ingestion_timestamp`.

**Reasoning:** Delta raises an error when a merge matches the same target row
more than once. Bronze accumulates every monthly file, so a transaction
corrected across consecutive months appears multiple times. Observed in
practice: the July and August files share 650 transaction identifiers.

**Consequences:** rows sharing an ingestion timestamp have no deterministic
order. This does not arise today, since `transaction_id` is unique within each
source file, but it would if a single file ever contained duplicates.

### Incremental scope

**Known limitation:** each silver run reads the whole of
`bronze.price_paid_monthly` rather than only rows added since the last run.
Deduplication makes this correct but not efficient — the merge processed
191,817 source rows for two months of changes, and that figure grows by
roughly 90,000 each month.

Filtering bronze by `run_id` or `ingestion_timestamp` against the last
successful silver run would restrict each merge to new rows. Not implemented:
at current volume the merge completes in under 90 seconds and the complexity
is not yet justified.

### Gold dimensional model

**Grain:** `fact_transaction` holds one row per property transaction.
`dim_property` holds one row per property per version of its tracked
attributes. `dim_date` holds one row per calendar date.

**Fact versus dimension:** a column describing the *event* belongs on the
fact, one describing the *thing* belongs on a dimension. `old_or_new`
records whether a sale was of a newly built property, so it is a property
of the sale rather than of the building and sits on the fact.
`property_type` and `duration` describe the building and sit on the
dimension.

**Degenerate dimension:** `transaction_id` is retained on the fact. It is
the natural key of the event itself with no attributes of its own, so it
needs no dimension table.

**Denormalised date:** `date_of_transfer` is carried on the fact alongside
`date_key`. This duplicates data held in `dim_date`, but allows partition
pruning and simple date filters without a join. `date_key` is computed
from the date rather than resolved by joining `dim_date`, since the value
is derivable and a join at 31.5 million rows would be wasteful.

### Surrogate keys

**Decision:** surrogate keys are generated with `xxhash64` over the
natural key and `valid_from`.

**Reasoning:** the hash is deterministic, so reprocessing produces the
same key for the same version and existing fact rows remain valid.
`monotonically_increasing_id()` is not stable across runs — a
reprocessed dimension would reissue different keys and orphan every fact
row referencing the old ones.

**Consequences:** a 64-bit hash carries a collision probability of roughly
one in four billion at this cardinality. This was accepted over a wider
`sha2` hash for smaller keys and faster joins.

Address components are coalesced to empty strings before concatenation.
`concat_ws` drops nulls entirely, so a property with no SAON produced the
same natural key as one with no PAON. Fixing this separated 276 properties
that had been sharing a key and therefore a single dimension version.

### SCD Type 2 on dim_property

**Decision:** `property_type` and `duration` are tracked as Type 2 —
a change closes the current version and opens a new one. Address fields
are Type 1 and updated in place.

**Reasoning:** a hybrid avoids versioning on changes that say nothing
about the building. Address fields are derivable from the postcode and
drift in the source through respelling and administrative reorganisation,
so tracking them as Type 2 would create versions recording no meaningful
change.

**Implementation:** a Delta merge cannot both update a matched row and
insert a new one for the same match. The merge source therefore emits
changed properties twice: once with `merge_key` set to the natural key,
which matches and closes the existing version, and once with a NULL
`merge_key`, which cannot match and falls through to the insert clause as
the new version.

**Consequences:** the fact stores the surrogate key of the version current
at the transaction date, not the latest version. A transaction from 2005
therefore reports the property as it was recorded in 2005, which is the
historical accuracy the pattern exists to preserve.

### Version dating

**Decision:** a property's first version opens at a sentinel date of
1900-01-01 rather than its earliest transaction date.

**Reasoning:** the fact resolves `property_sk` by finding the version
whose date range contains the transaction date. Dating the first version
from an observed transaction would leave any earlier transaction
unresolvable — including corrections to historical sales, which arrive
regularly in the monthly change files.

**Consequences:** `valid_from` on a first version does not correspond to
anything observed. It means "no earlier bound" rather than a date the
property was first seen.

### Deleted transactions in gold

**Decision:** deleted transactions are carried into the fact with their
`is_deleted` flag, and the property dimension is built from all
transactions including deleted ones.

**Reasoning:** consumers filter on the flag rather than having the
decision made for them. The dimension cannot exclude deleted transactions
while the fact includes them: 270 properties have no surviving live
transaction, and excluding them would leave their fact rows unable to
resolve a surrogate key.

**Consequences:** a property whose only transaction was withdrawn carries
attributes derived from that withdrawn record.

### Fact load strategy

**Decision:** `fact_transaction` is rebuilt in full with an overwrite on
each run. `dim_property` is merged. `dim_date` is regenerated.

**Reasoning:** the fact is derived entirely from silver, so a full rebuild
is always consistent and needs no merge logic or watermark. At 31.6
million rows the rebuild is acceptable; an incremental merge keyed on
`transaction_id` would be the next step if it stopped being so.

**Consequences:** the whole fact is rewritten even when a single month of
changes arrives upstream.

### Dimension partitioning

**Decision:** `dim_property` is not partitioned.

**Reasoning:** it is joined on `property_sk`, a hashed high-cardinality
key that distributes randomly and would produce millions of tiny
partitions. `is_current` would give two badly unbalanced partitions, and
any address-derived column would skew between dense urban and sparse
rural areas. Z-ordering on `property_sk` is the appropriate lever if join
performance becomes a problem.
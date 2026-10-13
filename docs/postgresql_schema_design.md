# PostgreSQL OLTP Schema Design

## 1. Scope and source ownership

Database: PostgreSQL 16, `cx_intelligence`. This is a design only; it creates no
tables, migrations, loaders, or pipeline changes.

The design follows [data_storage_architecture.md](data_storage_architecture.md),
[synthetic_data_schema.md](synthetic_data_schema.md), the seven existing generators,
and `preprocessing/validate_raw.py`.

| Schema | Table | Initial records | Responsibility |
| --- | --- | ---: | --- |
| source | customers | 500 | Original customer registry |
| source | product_releases | 20 | Original software release registry |
| source | customer_journey_events | 15,000 | Original session activity |
| source | mobile_application_logs | 5,000 | Original mobile diagnostics |
| source | survey_responses | 1,000 | Original customer feedback |
| pipeline | ingestion_batches | One initial batch | Loading and processing coordination |

The five business tables contain **21,520 structured records** initially. SeaweedFS
`cx-raw` independently holds 1,500 support tickets and 500 call transcripts. Do not
create PostgreSQL tables for either S3 dataset. Airflow's metadata database remains
separate from `cx_intelligence`.

Source tables retain original names, emails, and text. PII masking happens after
dual-source extraction in Airflow, before publication to `cx-curated`. Legacy raw
S3 copies of the five structured datasets are not authoritative inputs or fallbacks.

## 2. Shared conventions

- Preserve supplied string identifiers exactly; do not replace them with serial
  numbers or UUIDs. Business IDs use their existing prefixes without a fixed width.
- `Required` means `NOT NULL`; `Nullable` permits SQL `NULL`. Conditional requirements
  are enforced with row-level checks. Missing or null optional JSON fields map to
  SQL `NULL`; export may omit them or emit JSON null as the contract permits.
- Use `TEXT` for identifiers, categories, versions, and free text; `INTEGER` for
  integral scores/counts; `TIMESTAMPTZ` for timestamps; and `JSONB` for nested values.
  No current business field requires a `BOOLEAN` column.
- Parse ISO 8601 timestamps as timezone-aware values, use UTC connections, and export
  UTC ISO 8601 with `Z`. `TIMESTAMPTZ` preserves the instant, not the original timestamp
  spelling. See the [PostgreSQL timestamp documentation](https://www.postgresql.org/docs/16/datatype-datetime.html).
- Do not invent business-field defaults. Preserve empty `known_issues` arrays and
  nested properties. JSONB preserves their structure, not JSON whitespace or key order.
  See [PostgreSQL JSON types](https://www.postgresql.org/docs/16/datatype-json.html).
- Primary keys and unique constraints supply their own indexes. Use foreign keys
  only to PostgreSQL tables, with `ON DELETE RESTRICT` and `ON UPDATE RESTRICT`.
  Referencing columns still need workload-appropriate indexes. See
  [PostgreSQL constraints](https://www.postgresql.org/docs/16/ddl-constraints.html).
- Reject empty identifier strings, including optional IDs when supplied. Session
  and device IDs are correlation keys; no session/device tables are introduced.

## 3. Business tables

### 3.1 source.customers

| Column | Type | Nullability | Meaning / constraint |
| --- | --- | --- | --- |
| customer_id | TEXT | Required | Primary key; `cus_` prefix |
| full_name | TEXT | Required | Original fictional customer name |
| email | TEXT | Required | Unique original synthetic `example.com` address |
| created_at | TIMESTAMPTZ | Required | Account creation instant |
| country_code | TEXT | Required | ISO 3166-1 alpha-2 country code |
| preferred_language | TEXT | Required | Language tag, such as `en-AU` |
| customer_segment | TEXT | Required | `new`, `regular`, `premium` |
| account_status | TEXT | Required | `active`, `inactive`, `closed` |

Constraints: primary key `customer_id`; unique `email`; checks on the segment and
status vocabularies, nonempty name/language/email, and `country_code ~ '^[A-Z]{2}$'`.
Validate country membership, language tags, and synthetic email format in the loader;
the two-letter check alone does not establish a valid ISO country.

### 3.2 source.product_releases

| Column | Type | Nullability | Meaning / constraint |
| --- | --- | --- | --- |
| release_id | TEXT | Required | Primary key; `rel_` prefix |
| version | TEXT | Required | Semantic version, unique within its platform |
| platform | TEXT | Required | `android`, `ios`, `web` |
| released_at | TIMESTAMPTZ | Required | Release availability instant |
| release_notes | TEXT | Required | Original release description |
| known_issues | JSONB | Required | Array of strings; empty array allowed |
| rollout_percentage | INTEGER | Required | Integer from 1 through 100 |

Constraints: primary key `release_id`; unique `(platform, version)`; platform check;
`rollout_percentage BETWEEN 1 AND 100`; nonempty version; and
`jsonb_typeof(known_issues) = 'array'`. Validate each array element as a string and
validate full semantic-version syntax in Python. Do not constrain issues to three
codes: the contract also permits issue summaries.

### 3.3 source.customer_journey_events

| Column | Type | Nullability | Meaning / constraint |
| --- | --- | --- | --- |
| event_id | TEXT | Required | Primary key; `evt_` prefix |
| customer_id | TEXT | Required | FK to `source.customers.customer_id` |
| occurred_at | TIMESTAMPTZ | Required | Event instant |
| session_id | TEXT | Required | Existing session correlation ID |
| device_id | TEXT | Required | Existing device correlation ID |
| channel | TEXT | Required | `web`, `mobile` |
| event_type | TEXT | Required | Event vocabulary below |
| outcome | TEXT | Required | `success`, `failure` |
| error_code | TEXT | Nullable | Required for failure; null for success |
| release_id | TEXT | Nullable | FK to releases; required for mobile |
| ticket_id | TEXT | Nullable | External S3 ticket reference; no SQL FK |
| properties | JSONB | Nullable | Nested context object |

Constraints: primary key `event_id`; customer and release foreign keys; channel and
outcome checks; event type in `session_started`, `login_attempt`, `product_view`,
`checkout_started`, `payment_attempt`, `app_crash`, `support_contact`,
`journey_completed`.

Additional checks:

- `channel <> 'mobile' OR release_id IS NOT NULL`.
- Failure requires a non-null, nonempty `error_code`; success requires null.
- `app_crash` requires `outcome='failure'`; `journey_completed` requires success.
- `properties IS NULL OR jsonb_typeof(properties) = 'object'`.

The generator currently supplies releases for web events too, but web release links
remain nullable under the contract. It currently omits `ticket_id`; the column is
retained for the documented optional relationship.

### 3.4 source.mobile_application_logs

| Column | Type | Nullability | Meaning / constraint |
| --- | --- | --- | --- |
| log_id | TEXT | Required | Primary key; `log_` prefix |
| customer_id | TEXT | Required | FK to `source.customers.customer_id` |
| occurred_at | TIMESTAMPTZ | Required | Log instant |
| session_id | TEXT | Required | Existing application session |
| device_id | TEXT | Required | Existing mobile device |
| release_id | TEXT | Required | FK to `source.product_releases.release_id` |
| platform | TEXT | Required | `android`, `ios` |
| os_version | TEXT | Required | Original operating system version string |
| log_level | TEXT | Required | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| log_type | TEXT | Required | `payment_failure`, `app_crash`, `login_problem`, `activity` |
| message | TEXT | Required | Original diagnostic message |
| event_id | TEXT | Nullable | FK to `source.customer_journey_events.event_id` |
| error_code | TEXT | Nullable | Required for the three failure log types |
| stack_trace | TEXT | Nullable | Original synthetic crash trace |

Constraints: primary key `log_id`; customer, release, and event foreign keys;
platform, level, and type vocabulary checks; nonempty `os_version`; and a check
requiring non-null, nonempty `error_code` when `log_type <> 'activity'`.
`stack_trace` remains optional, including for crashes. Although current generated
logs always include `event_id`, the documented column remains nullable.

### 3.5 source.survey_responses

| Column | Type | Nullability | Meaning / constraint |
| --- | --- | --- | --- |
| survey_id | TEXT | Required | Primary key; `sur_` prefix |
| customer_id | TEXT | Required | FK to `source.customers.customer_id` |
| submitted_at | TIMESTAMPTZ | Required | Response submission instant |
| survey_type | TEXT | Required | `post_support`, `post_journey`, `general` |
| csat_score | INTEGER | Required | Integer from 1 through 5 |
| comment | TEXT | Nullable | Original feedback; generator currently supplies it |
| ticket_id | TEXT | Nullable | External S3 ticket reference; no SQL FK |
| call_id | TEXT | Nullable | External S3 call reference; no SQL FK |
| event_id | TEXT | Nullable | FK to `source.customer_journey_events.event_id` |
| session_id | TEXT | Nullable | Referenced source session, when available |
| device_id | TEXT | Nullable | Referenced source device, when available |

Constraints: primary key `survey_id`; customer and event foreign keys; survey type
check; `csat_score BETWEEN 1 AND 5`; and these conditional checks:

- `survey_type <> 'post_support' OR ticket_id IS NOT NULL OR call_id IS NOT NULL`.
- `survey_type <> 'post_journey' OR event_id IS NOT NULL`.
- `session_id IS NULL OR device_id IS NOT NULL`.

General surveys require no contact/event links; the current generator uses only
the customer relationship for them. Any optional link that is supplied must still
be validated. Both a ticket and call may be present on a post-support survey.

### 3.6 Validation boundary

Use `NOT NULL` together with checks: PostgreSQL CHECK expressions that evaluate
to null do not reject a row. Conditional error checks must explicitly test
`IS NOT NULL`. Do not put cross-row queries inside CHECK constraints.

The future loader and combined-source Python validator must additionally enforce:

- Exact JSON types before coercion, including rejecting booleans as integer CSAT.
- All ticket/call references exist in the batch or completed earlier S3 batches.
- Related customer-bearing records have the same customer; supplied correlation
  IDs agree across SQL and S3 records.
- A session has one customer, one device, and one supplied release; a device has
  a consistent platform. A mobile log's linked event is mobile and shares its
  customer, session, device, and release.
- Mobile releases are Android/iOS and match log platform; linked web releases
  are web. Events/logs follow account creation and release availability.
- Surveys follow their linked event, call end, or ticket opening, and follow
  closure when evaluating a resolved case. Initial synthetic timestamps respect
  the generators' 2026-10-11 cutoff; do not permanently impose that date on future batches.
- Nested JSON content follows the contract, and journey sequences remain coherent.

Single-column SQL foreign keys enforce existence, not these additional consistency
rules. `validate_raw.py` already contains useful validation rules but currently
loads all seven datasets from S3. Dual-source extraction/validation needs future
implementation; it is not enabled by this document.

## 4. Recommended indexes

| Table | Additional B-tree indexes | Purpose |
| --- | --- | --- |
| customers | `(created_at)` | Account-time filtering |
| product_releases | `(released_at)` | Release-time filtering |
| customer_journey_events | `(customer_id, occurred_at)`, `(session_id, occurred_at)`, `(release_id)`, `(occurred_at)` | Customer/session chronology, release references, time windows |
| mobile_application_logs | `(customer_id, occurred_at)`, `(session_id, occurred_at)`, `(release_id)`, `(event_id)` where non-null, `(occurred_at)` | Correlation, referenced events/releases, time windows |
| survey_responses | `(customer_id, submitted_at)`, `(session_id, submitted_at)` where non-null, `(event_id)` where non-null, `(submitted_at)` | Customer/session feedback, linked events, time windows |
| ingestion_batches | `(ready_at, created_at)` where status is ready; `(lease_expires_at)` where status is processing | Eligible batch discovery and abandoned-claim detection |

Primary-key indexes already cover `customer_id`, `release_id`, and `event_id` in
their owning tables. Composite indexes above cover referencing `customer_id` or
`session_id` as their leading column. Unique `(platform, version)` and `producer_key`
also have automatic indexes. Do not duplicate these indexes. External ticket/call
indexes can wait until a demonstrated query needs them.

For 21,520 initial records, ordinary tables and B-tree indexes are sufficient.
No partitioning, materialized analytics tables, JSONB GIN indexes, or additional
infrastructure is needed. Keep the existing laptop-oriented PostgreSQL memory
limit; measure query behavior before adding indexes or changing memory settings.

## 5. pipeline.ingestion_batches

One row represents one logical delivery across both primary sources. Metadata
does not add fields to business payloads. SQL batch membership lives in
`source_manifest`, not in business timestamps or an added business `batch_id` column.

| Column | Type | Nullability / default | Purpose |
| --- | --- | --- | --- |
| batch_id | UUID | Required | Primary key; producer allocates once and reuses on retry |
| producer_key | TEXT | Required | Unique stable delivery key |
| batch_kind | TEXT | Required | `initial` or `incremental` |
| schema_version | TEXT | Required | Input contract version, initially `v1` |
| processing_version | TEXT | Required | Masking/preprocessing version for reproducible retries |
| status | TEXT | Required; default `loading` | Batch lifecycle state |
| created_at | TIMESTAMPTZ | Required; default current timestamp | Registration instant |
| ready_at | TIMESTAMPTZ | Nullable | Both sources verified and input membership frozen |
| started_at | TIMESTAMPTZ | Nullable | Most recent processing attempt start |
| completed_at | TIMESTAMPTZ | Nullable | Verified curated publication recorded |
| source_manifest | JSONB | Required; default `{}` | SQL IDs, S3 keys, counts, checksums, readiness, dependencies |
| output_manifest | JSONB | Nullable | Curated keys, counts, checksums, completion-marker URI |
| claim_token | UUID | Nullable | Current processing ownership token |
| airflow_run_id | TEXT | Nullable | Airflow run owning the processing attempt |
| lease_expires_at | TIMESTAMPTZ | Nullable | Current ownership lease deadline |
| attempt_count | INTEGER | Required; default `0` | Increment on each successful processing claim |
| failed_stage | TEXT | Nullable | Failed stage, such as loading, validation, or publication |
| error_summary | TEXT | Nullable | Sanitized failure summary without PII or credentials |

Constraints:

- Primary key `batch_id`; unique `producer_key`; nonempty producer/version strings.
- `batch_kind IN ('initial', 'incremental')`.
- `status IN ('loading', 'ready', 'processing', 'completed', 'failed')`.
- `attempt_count >= 0`; JSONB manifests must be objects when present.
- Ready, processing, and completed states require `ready_at`.
- Processing requires `started_at`, `claim_token`, `airflow_run_id`, and
  `lease_expires_at`, with lease expiry after the attempt start.
- Completed requires `started_at`, `completed_at`, and `output_manifest`.
- When present, `ready_at >= created_at`, `started_at >= ready_at`, and
  `completed_at >= started_at`. A failed loading batch may have no readiness/start.

Lifecycle transitions and manifest content require transactional application
logic; CHECK constraints alone cannot enforce transitions against an old row.
Clear active claim/lease fields when completing or failing an attempt. Keep
`airflow_run_id`, attempt count, and sanitized failure information for diagnosis;
this single row is current state, not a complete per-attempt audit history.

### Source and output manifests

For every batch, declare all seven dataset names, including zero-record entries.
For each SQL dataset, enumerate exact primary IDs, record count, and SHA-256 of
deterministically serialized records in a defined ID order. Commit membership with
the SQL rows. For the small initial dataset, JSONB ID arrays are adequate; do not
introduce membership tables prematurely.

For the two S3 datasets, record bucket, exact key, count, and SHA-256 of object bytes.
Include dependency batch UUIDs and both branches' readiness evidence. Python
validates JSON-contained IDs and dependencies; they are not SQL foreign keys.
Only changed incremental datasets need objects, but zero counts must be explicit.

Freeze source membership and content after `ready`. The output manifest records
batch/schema/processing versions, source fingerprints, curated dataset keys,
counts, SHA-256 values, and the final S3 completion marker. An input/content/version
change requires a new batch/version prefix rather than altering a published batch.

## 6. Entity relationships

```text
PostgreSQL cx_intelligence

source.customers (customer_id PK)
    | 1
    +------< source.customer_journey_events (event_id PK, customer_id FK)
    +------< source.mobile_application_logs (log_id PK, customer_id FK)
    +------< source.survey_responses (survey_id PK, customer_id FK)

source.product_releases (release_id PK)
    | 1
    +------< source.customer_journey_events (release_id FK, optional for web)
    +------< source.mobile_application_logs (release_id FK, required)

source.customer_journey_events (event_id PK)
    | 1
    +------< source.mobile_application_logs (event_id FK, optional)
    +------< source.survey_responses (event_id FK, conditional)

pipeline.ingestion_batches (batch_id PK, producer_key UNIQUE)
    ..... source_manifest: SQL record membership + S3 objects + dependencies

SeaweedFS cx-raw (external records; dotted links are Python-validated)
    support_tickets ..... SQL customers / releases / event-survey ticket_id
    call_transcripts .... SQL customers / survey call_id / S3 ticket_id

Solid 1-to-many links: PostgreSQL foreign keys.
Dotted links: logical relationships, not database foreign keys.
```

## 7. Creation and initial insertion order

Future table creation order:

1. Create schemas `source` and `pipeline`.
2. Create `source.customers` and `source.product_releases`.
3. Create `source.customer_journey_events`.
4. Create `source.mobile_application_logs` and `source.survey_responses`.
5. Create independent `pipeline.ingestion_batches` and the additional indexes.

Initial loading process:

1. Generate the seven related datasets in existing dependency order: customers
   and releases, then events, logs/tickets, calls, and surveys. Validate their
   exact identifiers, types, and relationships before insertion.
2. Register a stable initial batch with a unique producer key and `loading` status.
3. In one business-loading transaction, insert 500 customers and 20 releases,
   then 15,000 events, 5,000 logs, and 1,000 surveys. Commit the exact SQL manifest
   membership and counts with these **21,520 rows**. Use parameterized, reasonably
   sized insert batches; do not construct SQL from free text.
4. Independently upload 1,500 tickets and 500 calls to their primary S3 keys.
   SQL survey/ticket references do not require S3 foreign-key enforcement at insertion.
5. Verify both committed SQL content and S3 content against the manifest. Mark the
   batch `ready` in a short transaction only after both branches succeed.

There is no transaction spanning PostgreSQL and S3. A failed branch leaves the
batch unready and is retried using the same delivery. Preserve successful source
data; do not delete legacy S3 copies or regenerate unrelated IDs.

### Duplicate prevention

Use targeted conflict handling, such as `INSERT ... ON CONFLICT (customer_id)
DO NOTHING`, with the equivalent primary-key target for each business table.
Then verify that every existing conflicting row matches the intended typed
payload; fail if the same ID carries different content. Do not silently overwrite
immutable source rows with `DO UPDATE`, or suppress conflicts on unrelated unique
constraints such as email or `(platform, version)`.

For metadata, `ON CONFLICT (producer_key) DO NOTHING` followed by reading the
existing row reuses a delivery's batch ID. Verify the manifest and versions agree
before resuming it. Primary-key/unique constraints prevent duplicate rows; content
verification establishes a safe retry. See the
[PostgreSQL INSERT reference](https://www.postgresql.org/docs/16/sql-insert.html).

## 8. Incremental processing and recovery

Incremental batches are append-only deliveries with globally unique new record
IDs. They may reference existing customers/releases or other records in completed
batches. Do not restart generator ID sequences for increments. Use manifest
membership rather than business-time watermarks so late-arriving records are
included without selecting previously completed records again.

Current development remains manually triggered with `schedule=None`. The future
batch-aware DAG should poll every **5 minutes**, select only ready batches whose
dependencies are completed, and finish successfully without processing when none
are eligible. Completed batches are excluded from discovery.

Atomically claim an eligible row in a short transaction using
`FOR UPDATE SKIP LOCKED`, then set `processing`, a fresh claim token, Airflow run ID,
start/lease timestamps, and increment `attempt_count`. Commit before extraction.
Every renewal and status update must match the current token and processing state.
Use this ownership protocol alongside `max_active_runs=1` and task parallelism two;
do not hold database locks throughout Airflow processing. See the
[PostgreSQL SELECT locking reference](https://www.postgresql.org/docs/16/sql-select.html).

Extract the five SQL datasets by manifest IDs and the two S3 datasets by manifest
keys; verify fingerprints, resolve earlier completed dependencies, validate jointly,
mask PII, and preprocess. Publish all seven initial datasets to curated storage.
For increments, publish changed datasets and a manifest declaring all seven counts;
the baseline plus completed increments keeps every dataset available.

Recommended object keys:

```text
Raw initial:      cx-raw/synthetic/initial/v1/{support_tickets,call_transcripts}.jsonl
Curated initial:  cx-curated/synthetic/initial/v1/<dataset>.jsonl
Initial marker:   cx-curated/synthetic/initial/v1/manifest.json
Raw increment:    cx-raw/synthetic/incremental/v1/batch_id=<uuid>/<s3-dataset>.jsonl
Curated increment: cx-curated/synthetic/incremental/v1/batch_id=<uuid>/<dataset>.jsonl
Increment marker: cx-curated/synthetic/incremental/v1/batch_id=<uuid>/manifest.json
```

Write and verify curated objects before writing the completion marker last; then
record `output_manifest` and transition to `completed`. Consumers use committed
manifests, not object listings. Stable keys overwrite incomplete attempts without
creating duplicate output copies or replacing the initial baseline with a delta.

Recovery rules:

- Loading failure: keep the same batch/producer key, repair the failed branch,
  transition back to loading if needed, and mark ready only after verification.
- Validation/preprocessing/publication failure: record `failed_stage` and a sanitized
  error, mark failed, and do not publish a completion marker. After explicit repair,
  retry the same frozen inputs through ready with a new ownership token.
- Expired processing lease: confirm the old worker stopped before releasing or
  reclaiming ownership. Renew leases during long work; stale tokens cannot update
  metadata. Coordinate stopping the old worker to prevent conflicting S3 writes.
- Marker published but database completion update failed: verify source/output
  fingerprints and the marker, then reconcile metadata to completed without
  publishing another prefix.
- Changed source content invalidates the frozen batch; use a new batch/version
  rather than changing retry inputs. Updates/deletions need a future revision design.

These rules provide repeatable, idempotent publication with potentially repeated
attempts, not a distributed exactly-once transaction. This design adds no analytics
tables and does not implement loading, scheduling, masking, or recovery code.

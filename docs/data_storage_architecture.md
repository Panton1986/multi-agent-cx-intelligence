# Parallel-Source Data Storage Architecture

## 1. Architecture and implementation status

PostgreSQL 16 (`cx_intelligence`, service `cx-postgres`) and SeaweedFS S3 (`cx-raw`)
are both primary data sources. Synthetic generation loads them through separate
branches. Apache Airflow extracts from both, processes their related records, and
publishes all seven datasets to `s3://cx-curated/`.

This document describes the revised design. The current implementation still uses
`validate_initial_data -> upload_to_seaweedfs -> validate_raw_data -> curate_raw_data`:
the uploader, validator, and curator currently read or write all seven raw datasets
through S3. PostgreSQL loading, dual-source extraction, and batch tracking are not
implemented. Existing S3 objects remain available during the transition.

The current `cx-raw` bucket contains legacy copies of all seven datasets from
earlier development. In the revised architecture, only `support_tickets` and
`call_transcripts` are primary S3 inputs. The five structured datasets must be
extracted from PostgreSQL; their legacy S3 copies are retained but must not be
included as duplicate inputs or used as an automatic fallback.

The design follows [synthetic_data_schema.md](synthetic_data_schema.md) and the
existing seven generators. Airflow metadata remains in its separate database;
business datasets and ingestion metadata belong in `cx_intelligence`.

```text
                  Synthetic Generator
                 /                   \
                v                     v
      PostgreSQL 16                SeaweedFS S3
      cx_intelligence              cx-raw
      5 structured datasets        tickets + transcripts
                |                     |
                v                     v
      Airflow: extract SQL     Airflow: extract S3
                 \                   /
                  v                 v
               Validate combined records
                          |
                          v
                      PII Masking
                          |
                          v
                     Preprocessing
                          |
                          v
                  SeaweedFS cx-curated
                  All seven datasets

      ingestion_batches in cx_intelligence coordinates readiness,
      processing ownership, retries, and publication of both sources.
```

## 2. Dataset ownership

| Dataset | Primary source | Initial records | Primary ID |
| --- | --- | ---: | --- |
| customers | PostgreSQL | 500 | customer_id |
| product_releases | PostgreSQL | 20 | release_id |
| customer_journey_events | PostgreSQL | 15,000 | event_id |
| mobile_application_logs | PostgreSQL | 5,000 | log_id |
| survey_responses | PostgreSQL | 1,000 | survey_id |
| support_tickets | SeaweedFS cx-raw | 1,500 | ticket_id |
| call_transcripts | SeaweedFS cx-raw | 500 | call_id |

PostgreSQL stores structured records, including their optional text and nested
properties. SeaweedFS stores tickets and conversation transcripts as UTF-8 JSONL.
Curated storage contains `<dataset>.jsonl` for every dataset listed above, preserving
nested objects and transcript turns. The counts describe the initial generation,
not a fixed requirement for incremental batches.

## 3. Initial generation and loading

1. Generate customers and releases once with `seed=42`. Generate events from those
   records, logs from events/releases, tickets from customers/events, calls from
   customers/tickets, and surveys from customers/tickets/calls/events.
2. Validate the generated records against the documented schema and shared IDs.
   Local JSONL exports may serve as development artifacts; Airflow's revised source
   extraction reads PostgreSQL and S3 directly.
3. Register one logical initial batch in `ingestion_batches` with status `loading`.
   Load the five structured datasets into PostgreSQL, and upload only tickets and
   transcripts to the new primary S3 branch. These branches may load independently.
4. Record the exact PostgreSQL record membership and S3 keys, counts, and checksums
   in the batch manifest. Commit SQL records before announcing SQL readiness; verify
   completed S3 writes before announcing S3 readiness.
5. Mark the batch `ready` only when both sources match the manifest. Freeze its
   input rows and objects after readiness. No distributed transaction across
   PostgreSQL and S3 is assumed; the readiness gate prevents partial extraction.

Preserve existing IDs throughout loading. References within PostgreSQL can use
database constraints. References to S3 tickets/calls require application-level
validation; a PostgreSQL foreign key cannot enforce an S3 relationship.

## 4. Airflow processing and shared relationships

The intended flow is:

```text
Extract both sources -> Validate -> PII Masking -> Preprocessing -> cx-curated
```

### Current development phase

The existing DAG uses `schedule=None` and manual triggering, with `catchup=False`,
`max_active_runs=1`, and LocalExecutor parallelism of two. It still runs the
existing S3-only pipeline; automatic batch detection is not implemented.
During development of the revised flow, ready-batch discovery can first be
invoked manually without adding a periodic schedule.

### Target production-like execution

Recommend automatic incremental batch detection through periodic polling of
PostgreSQL `ingestion_batches` every **5 minutes** in the future implementation.
Each polling run selects only batches with `status='ready'` whose dependencies
are completed. If no eligible new batches exist, finish successfully without
extracting data, preprocessing records, or writing curated objects. Exclude
completed batches from every poll.

Retain `catchup=False`, `max_active_runs=1`, and task parallelism of two. Prevent
duplicate concurrent processing through atomic PostgreSQL batch claims, leases,
and owner-token checks as described below; DAG concurrency limits alone are not
the batch ownership mechanism. This polling recommendation does not change the
current DAG or enable scheduling now.

For either execution mode, SQL and S3 extraction tasks may run concurrently;
combined validation waits for both to succeed. Pass batch IDs and object
references between tasks rather than full datasets in XCom.

### Validation and shared relationships

Validate required fields, JSON objects, unique primary IDs, customer consistency,
CSAT, event outcomes, and session/device/release agreement across both sources.
Use manifest counts for the batch. For increments, resolve references against
the current batch and completed earlier batches, rather than requiring every
referenced customer or release to be newly ingested.

- Every ticket, call, event, log, and survey resolves `customer_id` to PostgreSQL.
- `release_id` resolves to PostgreSQL releases wherever supplied; mobile events
  and logs require a compatible mobile release.
- Call `ticket_id` and event/survey `ticket_id` resolve to S3 tickets. Survey
  `call_id` resolves to S3 calls; survey/log `event_id` resolves to SQL events.
- `post_support` surveys require a ticket or call; `post_journey` requires an event.
  Optional relationships remain optional, but every supplied reference must resolve.
- Linked customer-bearing records share the same customer. A session has one
  customer, device, and supplied release; optional session IDs require device IDs.
  Session/device IDs are correlation keys, not newly invented entities.

Apply the existing masking helpers to customer names/emails, ticket text,
conversation turn text, survey comments, mobile messages/traces, release notes
and issues, and textual journey properties. Preserve all primary/foreign IDs,
timestamps, numeric values, speakers, and offsets. Masking is demonstration-level.
Preprocessing then checks output structure and serializes UTF-8 JSONL without
changing relationships. Customer stitching and analytics tables are out of scope.

The existing masking helpers can be reused. The current S3-only validator and
curator need future source-independent adapters; they must not be treated as
already supporting PostgreSQL extraction.

## 5. Incremental batches and ingestion metadata

Start with append-only batches of new records. Allocate globally unique IDs;
rerunning the current generators from their first IDs is not incremental generation.
Reuse existing customer, release, session, and device IDs when relationships require
them. A batch may contain only some datasets; its manifest explicitly declares zero
new records for the others.

Use durable batch membership rather than `occurred_at`, `opened_at`, or other
business timestamps as the ingestion watermark. Late-arriving records with old
timestamps still enter a new batch. In-place updates and deletions require a later
record-revision strategy; they are not captured by this append-only design.

Recommend one `ingestion_batches` row per logical batch in `cx_intelligence`:

| Proposed field | PostgreSQL type | Purpose |
| --- | --- | --- |
| batch_id | UUID, primary key | Stable identity shared by both loading branches |
| producer_key | text, unique | Reuse one batch for retries of the same producer delivery |
| batch_kind | text | initial or incremental |
| schema_version | text | Payload contract, initially v1 |
| processing_version | text | Masking/preprocessing contract used for deterministic retries |
| status | text | loading, ready, processing, completed, or failed |
| created_at, ready_at | timestamptz | Batch creation and both-source readiness |
| started_at, completed_at | timestamptz | Processing lifecycle |
| source_manifest | jsonb | SQL ID membership, S3 keys, counts, checksums, dependencies |
| output_manifest | jsonb | Curated keys, counts, checksums, and completion marker |
| claim_token, airflow_run_id | UUID, text | Processing owner and Airflow run correlation |
| lease_expires_at | timestamptz | Detect abandoned processing claims |
| attempt_count | integer | Retry history |
| failed_stage, error_summary | text | Failure location and sanitized diagnostic summary |

For this small initial system, the source manifest can enumerate PostgreSQL primary
IDs per dataset. Commit this membership with the corresponding loaded records.
Keep batch metadata outside the business payload schema. SQL input rows and S3
objects are immutable after `ready`; extraction uses the recorded membership and
verifies checksums. An altered source invalidates the batch rather than silently
changing the data seen by a retry.

In the planned batch-aware flow, Airflow selects `ready` batches whose dependency
batches are completed, either on a manual development invocation or on a future
5-minute polling run. Atomically claim a batch and transition it to `processing` in a short
transaction using PostgreSQL row locking, such as `FOR UPDATE SKIP LOCKED`; do not
hold that transaction open throughout preprocessing. PostgreSQL documents this
locking mechanism in its [SELECT reference](https://www.postgresql.org/docs/16/sql-select.html).
Maintain a lease and require the claim token for status changes. Completed batches
are excluded; no ready batches means a successful no-op. Failed batches require
an explicit retry transition after their cause is addressed.

## 6. S3 keys and curated publication

Recommended initial keys preserve the existing naming convention:

```text
s3://cx-raw/synthetic/initial/v1/support_tickets.jsonl
s3://cx-raw/synthetic/initial/v1/call_transcripts.jsonl
s3://cx-curated/synthetic/initial/v1/<dataset>.jsonl
s3://cx-curated/synthetic/initial/v1/manifest.json
```

Recommended incremental keys include one stable batch ID:

```text
s3://cx-raw/synthetic/incremental/v1/batch_id=<uuid>/support_tickets.jsonl
s3://cx-raw/synthetic/incremental/v1/batch_id=<uuid>/call_transcripts.jsonl
s3://cx-curated/synthetic/incremental/v1/batch_id=<uuid>/<dataset>.jsonl
s3://cx-curated/synthetic/incremental/v1/batch_id=<uuid>/manifest.json
```

Only changed datasets need incremental objects; the manifest records all seven
dataset names, including zero-count entries. The initial baseline plus published
increments keeps all seven datasets available in curated storage. Never replace
a complete baseline file with a partial incremental file.

Write curated dataset objects first and `manifest.json` last as the completion
marker. Include batch/schema/processing versions, source fingerprints, output
keys, counts, and SHA-256 checksums. Consumers use committed manifests rather than
interpreting an object listing as a complete batch. A manifest references only
fully written and verified output objects. Revised inputs or transformation
contracts require a new batch/version prefix, not changes to published inputs.

## 7. Idempotency and failure recovery

- Reuse the producer key, batch ID, SQL primary IDs, and S3 destination keys on
  retries. For SQL inserts, use conflict handling and verify existing payloads
  match; conflicting content under the same ID fails instead of being overwritten.
  PostgreSQL provides this capability through
  [ON CONFLICT](https://www.postgresql.org/docs/16/sql-insert.html).
- A failed loading branch leaves the batch unready. Retry that branch against the
  same immutable delivery; do not regenerate different IDs or discard valid data.
- Validation or preprocessing failure prevents publication of the completion
  manifest and leaves the batch uncompleted. Partial curated objects are not
  consumable and can be overwritten at the same keys during retry.
- If the manifest was published but the final PostgreSQL status update failed,
  verify the manifest and checksums and then mark the batch completed. Avoid
  duplicate output prefixes or relying solely on Airflow task status.
- Recover an expired claim only after confirming the previous worker stopped;
  issue a new claim token and resume the same batch. Long-running work renews its
  lease, and stale owners cannot update batch status.
- Processing may be attempted more than once. Stable inputs, deterministic masking,
  idempotent writes, and a final manifest provide repeatable publication without
  claiming a distributed exactly-once transaction.

Retain original sources for recovery. Do not delete existing S3 objects as part
of this architecture change. Keep credentials in environment configuration and
log batch IDs, dataset names, counts, and sanitized errors without original PII.
This document creates no loaders, DAG changes, database tables, or data migrations.

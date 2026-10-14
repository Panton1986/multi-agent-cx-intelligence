# RAG Indexing Architecture

## 1. Scope and current baseline

Design a local retrieval index over verified, PII-masked curated records. PostgreSQL
and SeaweedFS remain independent primary sources; Qdrant is a derived search index,
not a replacement for either source or curated storage.

The reported deployment has processed the initial **23,520 records** and runs Qdrant
v1.19.2 locally. Those runtime results were supplied by the project owner, not
reverified while writing this document. The inspected Compose configuration uses
`qdrant/qdrant:v1.19.2`, `127.0.0.1:6333:6333`, and the persistent
`qdrant-storage:/qdrant/storage` volume. Port 6334 is not published to Windows.

References inspected: [data_storage_architecture.md](data_storage_architecture.md),
[synthetic_data_schema.md](synthetic_data_schema.md),
[postgresql_schema_design.md](postgresql_schema_design.md), the batch DAG, all
existing preprocessing modules, and Docker Compose configuration.

The implemented `cx_batch_pipeline` is manually triggered (`schedule=None`), with
`claim_batch -> process_batch`. Processing extracts both sources, validates, masks,
exports, and completes the batch. Automatic retries are disabled. Incremental
batches are currently blocked at validation until historical lookup is implemented.
Some earlier architecture documentation describes the previous S3-only stage;
this indexing design follows the current batch modules and actual output manifest.

## 2. Complete architecture

```text
                       Synthetic generation / future producers
                              /                  \
                             v                    v
                 PostgreSQL cx_intelligence    SeaweedFS cx-raw
                 source.customers              support_tickets
                 source.product_releases       call_transcripts
                 source.customer_journey_events
                 source.mobile_application_logs
                 source.survey_responses
                              \                  /
                               v                v
                      Airflow cx_batch_pipeline (manual)
                 claim -> extract both -> validate -> PII mask
                                      |
                                      v
                         export seven curated JSONL objects
                                      |
                           verify and complete ETL batch
                         /                            \
                        v                              v
             pipeline.ingestion_batches         SeaweedFS cx-curated
             status=completed                    batches/<batch_id>/
             output_manifest                     <processing_version>/*.jsonl
                        |                              |
                        +---- future indexing worker --+
                              discover manifest keys
                              verify SHA-256 + JSONL
                              privacy gate + text selection
                              normalize + token-aware chunking
                              local CPU embeddings (384 dimensions)
                                      |
                                      v
                           Qdrant cx_knowledge_v1
                           deterministic UUID points
                           sanitized text + provenance
                                      |
                        future indexing receipt / visibility gate
                                      |
                                      v
                          Future LangGraph retrieval agent
                         query embedding + authorized filters
                         evidence chunks -> cited grounded answer

             Future SQL agent --------> authorized read-only PostgreSQL access
                    structured counts, joins, and timelines remain in SQL
```

Host endpoints: Qdrant REST `http://127.0.0.1:6333`, dashboard
`http://127.0.0.1:6333/dashboard`. A future worker on the existing Compose network
uses `http://qdrant:6333`; localhost inside a container refers to that container.
Keep the existing local binding and CPU image; no GPU dependency is required.

## 3. Collection and embedding contract

| Setting | Initial choice |
| --- | --- |
| Collection | `cx_knowledge_v1` |
| Dense embedding model | `BAAI/bge-small-en-v1.5` |
| Dimensions | 384 |
| Distance | Cosine |
| Device | CPU |
| Initial chunking version | `v1` |
| Initial index version | `v1` |
| Model/tokenizer revision | Pin the resolved revision during future implementation |

The model is English-focused and has a 512-token sequence limit. Use normalized
embeddings; apply the model's retrieval query instruction to queries, not indexed
passages: `Represent this sentence for searching relevant passages: `.
See the [BGE model card](https://huggingface.co/BAAI/bge-small-en-v1.5).
Record the instruction, pooling, normalization, tokenizer revision, and inference
backend in the index configuration so retrieval uses the same embedding contract.
Do not silently substitute another model or mix multilingual embeddings into v1.

On a future create-or-open operation, verify the existing collection's vector size
and distance before inserting. Reject mismatches rather than recreate or clear an
existing collection. Qdrant documents vector configuration in its
[collection reference](https://qdrant.tech/documentation/manage-data/collections/).

## 4. Exact source text and SQL ownership

Index these four datasets from their curated JSONL objects only:

| Dataset | Primary record ID | Exact text fields | Safe context / metadata |
| --- | --- | --- | --- |
| support_tickets | `ticket_id` | `subject`, `description`, optional `resolution` | `issue_type`, `status`, `priority`, `escalation_level`, `channel`, `opened_at`, optional `closed_at`, `customer_id`, optional `session_id`, `device_id`, `release_id` |
| call_transcripts | `call_id` | Each `transcript` turn's `text`; retain its `speaker` and `offset_seconds` | `issue_type`, `outcome`, `started_at`, `ended_at`, `duration_seconds`, `customer_id`, optional `ticket_id`, `session_id`, `device_id` |
| survey_responses | `survey_id` | Optional `comment` | `survey_type`, `csat_score`, `submitted_at`, `customer_id`, optional `ticket_id`, `call_id`, `event_id`, `session_id`, `device_id` |
| product_releases | `release_id` | `release_notes` and individual strings in `known_issues` | `version`, `platform`, `released_at`, `rollout_percentage` |

Build deterministic labeled text from these existing fields; labels are derived
presentation, not new source columns. For tickets, distinguish subject, problem,
and resolution. For releases, distinguish notes from known issues. An empty
`known_issues` array adds no issue text. Missing/null/blank survey comments produce
zero chunks; do not invent feedback from a CSAT score. Never treat a call's
`transcript` as a flat string or fabricate `body`, `content`, or `summary` columns.

The initial four datasets contain at most **3,020 source records** before skipping
empty text. Point count depends on chunking, not the 23,520 total ETL record count.
All seven datasets remain in curated storage; customers, journey events, and mobile
logs are excluded from initial vector indexing.

Keep all five structured PostgreSQL source tables available for future SQL agents.
Surveys and releases may serve both SQL analysis and text retrieval. Do not move
tickets/calls into SQL or build analytics tables as part of indexing. Source tables
contain original PII, so future SQL agents need authorized read-only queries and
column/output controls before exposing results to an LLM.

## 5. Completed-batch discovery and integrity

1. Read only `pipeline.ingestion_batches` rows with `status='completed'`, ordered by
   `completed_at`, `created_at`, and `batch_id`. ETL completion is the eligibility
   gate; ready/processing/failed/loading batches are never indexing inputs.
2. Read `batch_id`, `processing_version`, and `output_manifest`. Verify the manifest
   matches the row and declares `bucket='cx-curated'` and all seven datasets.
3. Use the four selected dataset entries' `key`, `count`, and `sha256`. Current keys
   are `batches/<batch_id>/<processing_version>/<dataset>.jsonl`. Resolve locations
   from the manifest and verify this permitted format; do not derive input files
   from object listings, business timestamps, or legacy prefix conventions.
4. Download each selected object and calculate SHA-256 over its exact bytes. Reject
   mismatches before text extraction or embedding. ETags are not SHA-256. Close all
   S3 response bodies; do not silently fall back to raw or earlier curated objects.
5. Parse UTF-8 JSONL, rejecting malformed/non-object records, duplicate JSON keys,
   nonfinite numbers, duplicate primary IDs, and manifest count mismatches. Empty
   zero-count objects are valid. Verify necessary text/nested field types.
6. Snapshot the manifest and confirm the batch is still completed with unchanged
   metadata before recording a successful indexing receipt.

The current exporter publishes seven objects and returns a compact manifest;
`complete_batch` verifies the objects and stores that manifest in PostgreSQL.
It does not create an S3 `manifest.json`. Indexing must use the database manifest
and completed state, not require a nonexistent S3 completion marker.

Never read `cx-raw` or local `data/generated` during indexing. A checksum establishes
content integrity, not PII safety. Apply the privacy gate below independently.

## 6. Privacy gate, preprocessing, and chunking

The existing masking helpers cover customer pseudonyms, emails, Australian phone
numbers, Luhn-valid card numbers, selected free text, and nested transcript turns.
`mask_batch` masks nested journey string values without trusting arbitrary key
names. These are baseline regex rules and do not guarantee that person names,
addresses, identifiers embedded in prose, or every sensitive format were removed.

Before Qdrant publication, review the selected curated fields and implement a
separate privacy gate over the final text and allowlisted payload. Reuse baseline
masking for supported patterns; quarantine/reject suspected residual PII instead
of embedding it. Future real-world data needs stronger local detection and review
for names and other unsupported PII. Do not claim the current regex layer meets
that requirement. Do not copy entire source records, customer names/emails, contact
details, credentials, raw exception messages, or original transcripts into payloads.

For approved sanitized text:

- Normalize line endings and Unicode consistently; trim incidental whitespace,
  preserve meaningful punctuation, redaction placeholders, labels, and turn order.
  Define normalization in `chunking_version`; do not remove placeholders or create
  inferred personal information.
- Use the pinned model tokenizer, not character counts. Start with a target of
  300 tokens and a hard limit of 400 for the final passage including context labels
  and tokenizer special tokens. This leaves margin below the model limit; reject
  accidental overlength input instead of relying on silent truncation.
- Prefer field, paragraph, and sentence boundaries. For long ordinary text, use
  up to 40 tokens of overlap. Never combine different source records into one chunk.
  Keep short ticket/release/survey text as one chunk when it fits.
- For calls, format each turn as a speaker-labeled line and keep adjacent turns
  together. Repeat a compact sanitized issue/outcome context in each chunk. Overlap
  one preceding turn only when it fits the budget. Split an oversized turn at
  sentence/token boundaries while repeating its speaker and original offset.
- Store original turn-index and offset ranges for call chunks. Offsets remain
  relative to `started_at`; do not fabricate precise per-sentence timing.
- Assign contiguous zero-based `chunk_index` values in stable source-field/turn
  order. Record zero chunks for absent text. Do not create vectors for placeholders
  alone or empty content; track such skips in the indexing receipt.

## 7. Qdrant payload and deterministic point identity

Use a strict payload allowlist:

| Payload field | Meaning |
| --- | --- |
| batch_id | Completed ETL batch UUID string |
| dataset | One of the four indexed dataset names |
| source_record_id | Original `ticket_id`, `call_id`, `survey_id`, or `release_id` |
| chunk_index | Zero-based integer within the source record |
| processing_version | Value from the completed PostgreSQL batch |
| chunking_version | Frozen text/normalization/chunking contract, initially `v1` |
| index_version | Index configuration identity, initially `v1` |
| embedding_model | `BAAI/bge-small-en-v1.5` |
| embedding_revision | Pinned model revision |
| text | Sanitized chunk text, identical to the passage embedded |
| text_sha256 | SHA-256 of exact UTF-8 chunk text |
| source_object_key, source_object_sha256 | Curated provenance from output_manifest |
| customer_id | Optional approved pseudonymous relationship ID; never a name/email |
| Relevant timestamp fields | Original `opened_at`, `closed_at`, `started_at`, `ended_at`, `submitted_at`, or `released_at`, where present; UTC ISO 8601 with Z |
| Selected context fields | Existing categories/numbers from the dataset table above, when needed |
| turn_start_index, turn_end_index, offset_start_seconds, offset_end_seconds | Derived call provenance, only for call chunks |

Preserve original identifiers and safe timestamps without adding customer records
to Qdrant. Treat customer IDs as linkable pseudonyms, not guaranteed anonymous
values. Include them only for authorized customer-scoped retrieval. Optional safe
ticket/call/event/release/session/device links may support citations; do not infer
missing links or put claim tokens and database credentials in the payload.

Define a fixed project UUID namespace as
`uuid5(NAMESPACE_URL, 'urn:cx-multiagent:rag:cx_knowledge_v1')`. Generate each point ID
with UUIDv5 over a compact JSON array, avoiding ambiguous string concatenation:

```text
[index_version, batch_id, processing_version, chunking_version,
 embedding_model, embedding_revision, dataset, source_record_id, chunk_index]
```

Same immutable inputs and versions produce the same UUID. Verify text/source
checksums against the indexing receipt; changed content under the same identity
must fail rather than silently produce a different indexed record.
Qdrant supports UUID point IDs and idempotent upserts of the same IDs; an upsert
can replace existing point content, so application checks remain necessary.
See [Qdrant points](https://qdrant.tech/documentation/manage-data/points/).

Create payload indexes only for actual filters: initially `dataset`, `batch_id`,
and `index_version`; optionally `customer_id`, `issue_type`, `platform`, and the
timestamps used in queries. Do not index every payload field or the entire text
by default. See [Qdrant payload indexing](https://qdrant.tech/documentation/manage-data/indexing/).

## 8. Idempotent indexing, visibility, and incremental batches

ETL `completed` and RAG `indexed` are separate states. Do not reuse ETL claim tokens,
change completed ingestion status, or overwrite `output_manifest` to track indexing.
Recommend a future small indexing ledger keyed by `(batch_id, index_version)` with
configuration/source fingerprints, indexing status, expected/verified point counts,
skipped-record counts, owner/lease information, and a sanitized error summary.
This is a proposed coordination mechanism, not a table created by this document.

Start with one manually invoked indexing worker. Select completed ETL batches
without a successful receipt for the requested index version. No eligible batch
means a successful no-op. Process records in deterministic order, embed bounded
chunks, upsert stable point IDs with operation completion awaited, and verify
expected IDs, payload fingerprints, and counts before marking the receipt indexed.
Bounded exact-ID retrieval is sufficient for this small corpus.

Retries reuse the same IDs and configuration. Partial Qdrant writes are retried
without duplicate points; a failure leaves the indexing receipt uncompleted.
If Qdrant writes finish but the receipt commit fails, reconcile verified points
before recording success. PostgreSQL and Qdrant do not provide a shared transaction.
Concurrent indexing later needs its own claim/lease protocol; DAG concurrency alone
does not establish ownership.

Future retrieval must filter to batches with successful indexing receipts for the
active index version, so partially indexed batches are not served. Initially pass
the small approved batch-ID set as a Qdrant filter. Do not treat arbitrary points
present in a collection as published. A scalable visibility mechanism can be
designed later if the ledger/filter set grows.

For future append-only increments, discover completed batches by durable ledger
membership rather than record timestamps, verify their own manifest keys, and index
only their new text-bearing records. Globally unique source IDs preserve historical
relationships. The present ETL DAG blocks increments; historical validation must
be implemented before enabling this flow. Do not regenerate initial IDs, reindex
every completed baseline on each poll, or assume updates/deletions are captured.
Record revisions, erasure, and cross-batch replacement need a later explicit policy.

## 9. Model and chunking changes

Freeze v1 model revision, normalization, query instruction, tokenizer, pooling,
dimension, distance, and chunking settings. A change to those settings, source
text selection, or masking contract requires a new index version and rebuild from
verified completed curated batches.

Build a new collection such as `cx_knowledge_v2`, with its own receipts and stable
IDs, validate it, then switch future retrieval configuration to it. Keep v1 until
the migration is approved. Even another 384-dimensional model defines a different
embedding space; matching dimensions do not make vectors interchangeable.
Chunking changes must not leave old surplus chunks searchable. Separate collections
avoid mixing stale and new points; do not drop a live collection automatically.

## 10. Future LangGraph retrieval flow

1. Route semantic support/feedback/release questions to a Qdrant retrieval tool;
   route counts, rates, joins, and exact timelines to an authorized SQL tool.
2. Sanitize the query and apply caller permissions before customer-scoped search.
   Use the same pinned query embedding contract as indexing.
3. Query `cx_knowledge_v1` using cosine similarity and approved indexed-batch,
   dataset, customer, platform, or time filters. Start with approximately eight
   candidates; tune retrieval depth using example questions rather than a universal
   similarity threshold.
4. Deduplicate overlapping chunks, retain speaker/context for calls, and return
   sanitized evidence with dataset, record ID, chunk index, and curated provenance.
5. The agent generates an evidence-grounded answer with citations and acknowledges
   missing evidence. Retrieved text is untrusted data, never agent instructions.
   No retrieval fallback reads raw PII-containing SQL columns or S3 objects.

LangGraph, an LLM, and the retrieval tools are future work; none is implemented or
installed by this design. Exact customer metrics should come from SQL, not counts
of vector-search hits.

## 11. CPU implementation recommendation

For the Ryzen 5 7530U, 16 GB RAM, and no NVIDIA GPU, start with a single local CPU
embedding process using Sentence Transformers and the specified small model.
Approve dependency versions/Python 3.13 wheels and the one-time model download in
a separate implementation task; cache the pinned model locally for subsequent
offline runs. Prefer an isolated local worker environment rather than downloading
models during Airflow DAG parsing or loading a model in every task process.

Initial tuning suggestions, to be measured on this laptop:

- Embedding batch size 16; increase toward 32 only after checking memory/latency.
- Two to four CPU inference threads and one indexing worker to leave capacity for
  Airflow, PostgreSQL, SeaweedFS, Qdrant, and Windows.
- Qdrant upsert batches of 64-128 points; process one curated dataset at a time.
- Reserve roughly 1-2 GB as an initial embedding-worker budget and measure actual
  peak use; avoid retaining every tokenized chunk/vector simultaneously.
- Keep default Qdrant dense indexing initially. No GPU runtime, paid embedding API,
  large reranker, or extra service is required. Evaluate ONNX CPU inference later
  only if measured throughput justifies another backend/configuration version.

Each float32 vector has 384 x 4 = 1,536 bytes of raw values: 10,000 vectors use about
15.4 MB before graph, payload, model, and service overhead. This is not a total RAM
estimate. Measure chunk count, memory, and indexing duration instead of assuming
the hardware can sustain production workloads.

## 12. Step-by-step implementation plan

1. Review this design and freeze v1 text selection, privacy gate, tokenizer/chunk
   budgets, payload allowlist, embedding revision, and deterministic ID contract.
2. Implement a completed-batch manifest reader and checksum-verifying curated
   object reader, independent of legacy raw extractors. Exercise empty-text and
   malformed/mismatched manifest cases in a separately authorized testing task.
3. Implement dataset-specific sanitized text builders and turn-aware token chunking
   under the existing modular package structure. Verify identifiers, timestamps,
   source order, and nested transcript context remain intact.
4. Strengthen the privacy gate before real customer data; reject residual PII and
   verify logs/payloads contain no sensitive values. Use synthetic examples first.
5. Approve and install a CPU embedding backend, download/cache the pinned BGE model,
   and verify finite 384-dimensional normalized vectors and query compatibility.
6. Create `cx_knowledge_v1` with Cosine only in an explicit future task; verify an
   existing collection instead of deleting/recreating it. Add required payload indexes.
7. Implement bounded upserts, deterministic UUIDs, point reconciliation, and a
   separate durable indexing ledger/visibility gate. Keep ETL completion metadata intact.
8. Index the reported completed initial batch, reconcile source/skip/chunk/point
   counts, rerun it to confirm no duplicates, and verify retrieval citations.
9. Add a separate manually triggered indexing workflow after successful ETL, with
   metadata-only XCom and no model/network operations at import. Do not alter the
   current DAG until explicitly requested.
10. Implement historical ETL validation before enabling incremental generation and
    indexing. Later add polling, controlled concurrency, and recovery deliberately.
11. Implement future LangGraph Qdrant/SQL tools with authorization and evaluate
    payment failures, crashes, login problems, escalations, negative CSAT, and
    successful recoveries. Validate runtime behavior before claiming production readiness.

This task creates documentation only: no collection, model download, package
installation, SQL table, service change, indexing run, or automated test is performed.

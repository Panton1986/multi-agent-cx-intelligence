"""Seed original structured JSONL data into the parallel PostgreSQL source."""

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv
from sqlalchemy import URL, Column, DateTime, Integer, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID, insert
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_BATCH_SIZE = 500
_PRODUCER_KEY = "synthetic-initial-v1"
type Record = dict[str, Any]


@dataclass(frozen=True)
class _Dataset:
    """Column contract for one structured source dataset."""

    name: str
    primary_key: str
    fields: tuple[str, ...]
    timestamp: str
    optional: tuple[str, ...] = ()
    json_fields: tuple[str, ...] = ()
    integer_fields: tuple[str, ...] = ()


_DATASETS = (
    _Dataset(
        "customers",
        "customer_id",
        (
            "customer_id",
            "full_name",
            "email",
            "created_at",
            "country_code",
            "preferred_language",
            "customer_segment",
            "account_status",
        ),
        "created_at",
    ),
    _Dataset(
        "product_releases",
        "release_id",
        (
            "release_id",
            "version",
            "platform",
            "released_at",
            "release_notes",
            "known_issues",
            "rollout_percentage",
        ),
        "released_at",
        json_fields=("known_issues",),
        integer_fields=("rollout_percentage",),
    ),
    _Dataset(
        "customer_journey_events",
        "event_id",
        (
            "event_id",
            "customer_id",
            "occurred_at",
            "session_id",
            "device_id",
            "channel",
            "event_type",
            "outcome",
            "error_code",
            "release_id",
            "ticket_id",
            "properties",
        ),
        "occurred_at",
        optional=("error_code", "release_id", "ticket_id", "properties"),
        json_fields=("properties",),
    ),
    _Dataset(
        "mobile_application_logs",
        "log_id",
        (
            "log_id",
            "customer_id",
            "occurred_at",
            "session_id",
            "device_id",
            "release_id",
            "platform",
            "os_version",
            "log_level",
            "log_type",
            "message",
            "event_id",
            "error_code",
            "stack_trace",
        ),
        "occurred_at",
        optional=("event_id", "error_code", "stack_trace"),
    ),
    _Dataset(
        "survey_responses",
        "survey_id",
        (
            "survey_id",
            "customer_id",
            "submitted_at",
            "survey_type",
            "csat_score",
            "comment",
            "ticket_id",
            "call_id",
            "event_id",
            "session_id",
            "device_id",
        ),
        "submitted_at",
        optional=("comment", "ticket_id", "call_id", "event_id", "session_id", "device_id"),
        integer_fields=("csat_score",),
    ),
)


def _reject_constant(value: str) -> None:
    """Reject nonstandard JSON numbers without including their source content."""
    raise ValueError("Nonstandard JSON numeric constant")


def _unique_object(pairs: list[tuple[str, Any]]) -> Record:
    """Reject duplicate JSON keys instead of silently discarding values."""
    result: Record = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


def _read_dataset(spec: _Dataset) -> list[Record]:
    """Validate a local JSONL file and normalize its typed SQL payloads."""
    path = _PROJECT_ROOT / "data" / "generated" / f"{spec.name}.jsonl"
    if not path.is_file():
        raise FileNotFoundError(f"Missing input file: {spec.name}.jsonl")
    records: list[Record] = []
    identifiers: set[str] = set()
    try:
        with path.open(encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                try:
                    raw = json.loads(
                        line, parse_constant=_reject_constant, object_pairs_hook=_unique_object
                    )
                    if not isinstance(raw, dict) or set(raw) - set(spec.fields):
                        raise ValueError("Expected an object containing only documented fields")
                    record: Record = {}
                    for field in spec.fields:
                        value = raw.get(field)
                        if value is None:
                            if field not in spec.optional:
                                raise ValueError("Missing required field")
                        elif field == spec.timestamp:
                            if not isinstance(value, str):
                                raise ValueError("Invalid timestamp type")
                            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            if value.tzinfo is None or value.utcoffset() is None:
                                raise ValueError("Timestamp requires a timezone")
                            value = value.astimezone(UTC)
                        elif field in spec.integer_fields:
                            if type(value) is not int:
                                raise ValueError("Expected an integer")
                        elif field in spec.json_fields:
                            if field == "properties" and not isinstance(value, dict):
                                raise ValueError("Expected properties object")
                            if field == "known_issues" and (
                                not isinstance(value, list)
                                or any(not isinstance(issue, str) for issue in value)
                            ):
                                raise ValueError("Expected known issues string array")
                        elif not isinstance(value, str):
                            raise ValueError("Expected a string")
                        if field.endswith("_id") and value is not None and not value.strip():
                            raise ValueError("Empty identifier")
                        record[field] = value
                    identifier = record[spec.primary_key]
                    if identifier in identifiers:
                        raise ValueError("Duplicate primary identifier")
                    identifiers.add(identifier)
                    records.append(record)
                except (ValueError, TypeError, OverflowError, RecursionError):
                    raise ValueError(
                        f"Invalid {spec.name} JSONL record at line {line_number}"
                    ) from None
    except (OSError, UnicodeError):
        raise ValueError(f"Cannot read UTF-8 input: {spec.name}.jsonl") from None
    if not records:
        raise ValueError(f"Input file is empty: {spec.name}.jsonl")
    return records


def _canonical_record(record: Record) -> str:
    """Serialize SQL values consistently for equality and manifest fingerprints."""
    normalized = {
        key: value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        if isinstance(value, datetime)
        else value
        for key, value in record.items()
    }
    return json.dumps(
        normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _postgres_manifest(datasets: dict[str, list[Record]]) -> Record:
    """Describe exact SQL membership and canonical UTF-8 JSONL SHA-256 hashes."""
    entries: Record = {}
    for spec in _DATASETS:
        records = sorted(datasets[spec.name], key=lambda record: record[spec.primary_key])
        digest = hashlib.sha256()
        for record in records:
            digest.update((_canonical_record(record) + "\n").encode("utf-8"))
        entries[spec.name] = {
            "table": f"source.{spec.name}",
            "primary_key": spec.primary_key,
            "ids": [record[spec.primary_key] for record in records],
            "count": len(records),
            "sha256": digest.hexdigest(),
        }
    return {
        "status": "loaded",
        "serialization": "UTF-8 JSONL; sorted IDs/keys; compact JSON; UTC microseconds; null optionals",
        "datasets": entries,
    }


def _source_table(spec: _Dataset, metadata: MetaData) -> Table:
    """Declare SQLAlchemy types without creating or modifying database tables."""
    columns = []
    for field in spec.fields:
        column_type = (
            DateTime(timezone=True)
            if field == spec.timestamp
            else JSONB(none_as_null=True)
            if field in spec.json_fields
            else Integer()
            if field in spec.integer_fields
            else Text()
        )
        columns.append(
            Column(
                field,
                column_type,
                primary_key=field == spec.primary_key,
                nullable=field in spec.optional,
            )
        )
    return Table(spec.name, metadata, *columns, schema="source")


def _verify_dataset(
    connection: Connection, table: Table, spec: _Dataset, records: list[Record], *, write: bool
) -> None:
    """Insert by primary key if allowed, then compare every stored payload."""
    primary_key = table.c[spec.primary_key]
    statement = insert(table).on_conflict_do_nothing(index_elements=[primary_key])
    for offset in range(0, len(records), _BATCH_SIZE):
        batch = records[offset : offset + _BATCH_SIZE]
        if write:
            connection.execute(statement, batch)
        stored = (
            connection.execute(
                select(table)
                .where(primary_key.in_([row[spec.primary_key] for row in batch]))
                .with_for_update()
            )
            .mappings()
            .all()
        )
        indexed = {row[spec.primary_key]: dict(row) for row in stored}
        for record in batch:
            existing = indexed.get(record[spec.primary_key])
            if existing is None or _canonical_record(existing) != _canonical_record(record):
                raise ValueError(f"Stored content does not match source dataset: {spec.name}")


def load_initial_structured_data() -> dict[str, int]:
    """Load and verify five original datasets in one PostgreSQL transaction.

    Read workspace-local JSONL files before any database writes. Load the local
    .env at invocation without overriding existing environment variables. Reuse
    the initial delivery's batch ID, preserve any existing SeaweedFS manifest,
    and leave a new SeaweedFS branch pending. Never mark the batch ready.

    Matching loading batches may be retried. Advanced batches are verified only,
    without inserting or changing their manifests/status. Failed batches require
    explicit recovery outside this seeding function. All errors roll back writes.
    Database errors are sanitized to avoid disclosing parameters or credentials.
    """
    datasets = {spec.name: _read_dataset(spec) for spec in _DATASETS}
    postgres = _postgres_manifest(datasets)
    load_dotenv(dotenv_path=_PROJECT_ROOT / ".env", override=False)
    password = os.environ.get("CX_POSTGRES_PASSWORD")
    if not password:
        raise ValueError("Set CX_POSTGRES_PASSWORD before loading PostgreSQL data")
    url = URL.create(
        "postgresql+psycopg",
        username="cx_app",
        password=password,
        host="127.0.0.1",
        port=5433,
        database="cx_intelligence",
    )
    metadata = MetaData()
    tables = {spec.name: _source_table(spec, metadata) for spec in _DATASETS}
    batches = Table(
        "ingestion_batches",
        metadata,
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("producer_key", Text()),
        Column("batch_kind", Text()),
        Column("schema_version", Text()),
        Column("processing_version", Text()),
        Column("status", Text()),
        Column("source_manifest", JSONB(none_as_null=True)),
        schema="pipeline",
    )
    pending_s3 = {
        "status": "pending",
        "datasets": {
            name: {
                "status": "pending",
                "bucket": "cx-raw",
                "key": f"synthetic/initial/v1/{name}.jsonl",
            }
            for name in ("support_tickets", "call_transcripts")
        },
    }
    engine = None
    try:
        engine = create_engine(
            url, echo=False, hide_parameters=True, connect_args={"connect_timeout": 10}
        )
        with engine.begin() as connection:
            connection.execute(
                insert(batches)
                .values(
                    batch_id=uuid4(),
                    producer_key=_PRODUCER_KEY,
                    batch_kind="initial",
                    schema_version="v1",
                    processing_version="v1",
                    status="loading",
                    source_manifest={},
                )
                .on_conflict_do_nothing(index_elements=[batches.c.producer_key])
            )
            batch = (
                connection.execute(
                    select(batches).where(batches.c.producer_key == _PRODUCER_KEY).with_for_update()
                )
                .mappings()
                .one()
            )
            if any(
                batch[field] != value
                for field, value in (
                    ("batch_kind", "initial"),
                    ("schema_version", "v1"),
                    ("processing_version", "v1"),
                )
            ):
                raise ValueError("Existing initial batch has incompatible delivery versions")
            if batch["status"] not in {"loading", "ready", "processing", "completed"}:
                raise ValueError("Existing initial batch requires explicit recovery before loading")
            manifest = batch["source_manifest"]
            if not isinstance(manifest, dict):
                raise ValueError("Existing initial source manifest must be an object")
            previous = manifest.get("postgresql")
            if previous is not None and previous != postgres:
                raise ValueError("Initial PostgreSQL manifest conflicts with this delivery")
            write = batch["status"] == "loading"
            if not write and previous != postgres:
                raise ValueError("Cannot change membership of an initial batch after readiness")
            for spec in _DATASETS:
                _verify_dataset(
                    connection, tables[spec.name], spec, datasets[spec.name], write=write
                )
            if write:
                manifest = dict(manifest)
                manifest["postgresql"] = postgres
                manifest.setdefault("dependencies", [])
                manifest.setdefault("seaweedfs", pending_s3)
                connection.execute(
                    batches.update()
                    .where(batches.c.batch_id == batch["batch_id"])
                    .values(source_manifest=manifest)
                )
    except SQLAlchemyError:
        raise RuntimeError(
            "PostgreSQL loading failed; transaction rolled back. Check connection, migration, "
            "and source constraints. Database details are omitted to protect sensitive data."
        ) from None
    finally:
        if engine is not None:
            engine.dispose()
    counts = {spec.name: len(datasets[spec.name]) for spec in _DATASETS}
    for name, count in counts.items():
        logger.info("PostgreSQL dataset=%s records=%d status=verified", name, count)
    logger.info("Initial structured loading complete; batch readiness unchanged")
    return counts

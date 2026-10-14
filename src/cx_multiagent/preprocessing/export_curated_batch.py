"""Publish deterministic masked batch datasets without completing the batch."""

import hashlib
import json
import logging
import os
import re
from contextlib import ExitStack, closing
from datetime import UTC, datetime
from typing import Any
from uuid import UUID as PythonUUID

from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import URL, Column, DateTime, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from cx_multiagent.storage.s3_client import create_s3_client

logger = logging.getLogger(__name__)
_BUCKET = "cx-curated"
_PRIMARY_KEYS = {
    "customers": "customer_id",
    "product_releases": "release_id",
    "customer_journey_events": "event_id",
    "mobile_application_logs": "log_id",
    "survey_responses": "survey_id",
    "support_tickets": "ticket_id",
    "call_transcripts": "call_id",
}


def _uuid_string(value: str, field: str) -> PythonUUID:
    """Parse a UUID string without exposing its supplied value in errors."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a UUID string")
    try:
        return PythonUUID(value)
    except ValueError:
        raise ValueError(f"{field} must be a UUID string") from None


def _connection_url() -> URL:
    """Read required environment settings and build a password-safe psycopg URL."""
    names = (
        "CX_POSTGRES_HOST",
        "CX_POSTGRES_PORT",
        "CX_POSTGRES_DB",
        "CX_POSTGRES_USER",
        "CX_POSTGRES_PASSWORD",
    )
    settings = {name: os.environ.get(name, "") for name in names}
    if any(not value.strip() for value in settings.values()):
        raise ValueError("Set all five CX_POSTGRES connection environment variables")
    try:
        port = int(settings["CX_POSTGRES_PORT"])
    except ValueError:
        raise ValueError("CX_POSTGRES_PORT must be an integer between 1 and 65535") from None
    if not 1 <= port <= 65535:
        raise ValueError("CX_POSTGRES_PORT must be an integer between 1 and 65535")
    return URL.create(
        "postgresql+psycopg",
        username=settings["CX_POSTGRES_USER"],
        password=settings["CX_POSTGRES_PASSWORD"],
        host=settings["CX_POSTGRES_HOST"],
        port=port,
        database=settings["CX_POSTGRES_DB"],
    )


def _processing_version(
    connection: Connection, table: Table, batch_id: PythonUUID, claim_token: PythonUUID
) -> str:
    """Verify current ownership and return the trusted database processing version."""
    batch = (
        connection.execute(select(table).where(table.c.batch_id == batch_id))
        .mappings()
        .one_or_none()
    )
    if batch is None:
        raise ValueError("Batch does not exist")
    if batch["status"] != "processing" or batch["claim_token"] != claim_token:
        raise ValueError("Batch is not processing under the supplied claim token")
    expiry = batch["lease_expires_at"]
    if (
        not isinstance(expiry, datetime)
        or expiry.tzinfo is None
        or expiry.utcoffset() is None
        or expiry <= datetime.now(UTC)
    ):
        raise ValueError("Batch processing lease has expired or is invalid")
    version = batch["processing_version"]
    if not isinstance(version, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version) is None:
        raise ValueError("Batch processing_version must be a safe nonempty S3 path component")
    return version


def _json_value(value: Any) -> Any:
    """Copy JSON values recursively, converting only aware datetime objects."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Naive datetime is not supported")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings")
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if value is None or type(value) in (str, int, float, bool):
        return value
    raise ValueError("Unsupported JSON value type")


def _serialize_dataset(name: str, records: Any) -> tuple[bytes, int, str]:
    """Serialize unique records in primary-ID order and hash exact output bytes."""
    if not isinstance(records, list):
        raise ValueError(f"{name}: records must be a list")
    primary_key = _PRIMARY_KEYS[name]
    identifiers: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            raise ValueError(f"{name}: every record must be an object")
        identifier = record.get(primary_key)
        if not isinstance(identifier, str) or not identifier.strip():
            raise ValueError(f"{name}: missing primary identifier")
        if identifier in identifiers:
            raise ValueError(f"{name}: duplicate primary identifier")
        identifiers.add(identifier)
    try:
        lines = [
            json.dumps(
                _json_value(record),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
            for record in sorted(records, key=lambda record: record[primary_key])
        ]
        payload = "".join(lines).encode("utf-8")
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(f"{name}: invalid JSON data or naive datetime") from None
    # A zero-record dataset is an empty object body, not a blank JSONL record.
    return payload, len(records), hashlib.sha256(payload).hexdigest()


def _stored_checksum(client: BaseClient, key: str) -> str | None:
    """Hash stored object bytes; only a missing object is treated as absent."""
    try:
        response = client.get_object(Bucket=_BUCKET, Key=key)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise
    digest = hashlib.sha256()
    with closing(response["Body"]) as body:
        for chunk in body.iter_chunks(chunk_size=65536):
            digest.update(chunk)
    return digest.hexdigest()


def export_curated_batch(
    batch_id: str,
    claim_token: str,
    masked_records: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Publish seven masked datasets and return a small verified output manifest.

    Require validation and masking before calling. Serialize every dataset before
    any upload, using UTF-8, sorted IDs/keys, compact JSON, UTC microseconds, and a
    newline after each record. Empty datasets produce empty JSONL object bodies.
    Obtain processing_version only from the actively owned database batch.

    Reuse identical existing S3 objects, reject differing content, and verify new
    writes by hashing downloaded bytes. ETags are not used as SHA-256 checksums.
    Get/check/put is not an atomic conditional write: complete protection against
    concurrent S3 writers requires the batch ownership/lease protocol, a single
    active writer, and stopping stale workers before recovery. Ownership is checked
    before publication, before each upload, and after publication; no lease renewals
    or automatic rollback/deletion of previously published objects are performed.

    Write only batches/<batch_id>/<processing_version>/<dataset>.jsonl in cx-curated.
    Legacy objects are untouched. Return counts/keys/checksums, never records or
    payloads. A separate completion module must persist this manifest and mark
    the batch completed; this exporter updates no PostgreSQL metadata and writes
    no completion marker. No extraction, masking, or external I/O occurs at import.

    Raises:
        ValueError: For invalid inputs, ownership, serialization, version metadata,
            or conflicting/mismatched stored objects.
        RuntimeError: For sanitized PostgreSQL or S3 errors.
    """
    identifier = _uuid_string(batch_id, "batch_id")
    token = _uuid_string(claim_token, "claim_token")
    if not isinstance(masked_records, dict) or set(masked_records) != set(_PRIMARY_KEYS):
        raise ValueError("masked_records must contain exactly the seven expected datasets")
    serialized = {name: _serialize_dataset(name, masked_records[name]) for name in _PRIMARY_KEYS}
    url = _connection_url()
    batches = Table(
        "ingestion_batches",
        MetaData(),
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("status", Text()),
        Column("claim_token", UUID(as_uuid=True)),
        Column("processing_version", Text()),
        Column("lease_expires_at", DateTime(timezone=True)),
        schema="pipeline",
    )
    publication: dict[str, str] = {}
    entries: dict[str, Any] = {}
    try:
        with ExitStack() as resources:
            engine = create_engine(
                url,
                echo=False,
                hide_parameters=True,
                isolation_level="READ COMMITTED",
                connect_args={"connect_timeout": 10},
            )
            resources.callback(engine.dispose)
            with engine.connect() as connection:
                connection = connection.execution_options(postgresql_readonly=True)
                with connection.begin():
                    version = _processing_version(connection, batches, identifier, token)
                    prefix = f"batches/{identifier}/{version}/"
                    try:
                        client = create_s3_client()
                    except ValueError:
                        raise ValueError(
                            "Check required SeaweedFS environment configuration"
                        ) from None
                    with closing(client):
                        for name, (payload, count, checksum) in serialized.items():
                            key = f"{prefix}{name}.jsonl"
                            existing = _stored_checksum(client, key)
                            if existing is not None:
                                if existing != checksum:
                                    raise ValueError(
                                        f"{name}: existing curated object has conflicting content"
                                    )
                                publication[name] = "reused"
                            else:
                                if (
                                    _processing_version(connection, batches, identifier, token)
                                    != version
                                ):
                                    raise ValueError(
                                        "Batch processing_version changed during publication"
                                    )
                                client.put_object(
                                    Bucket=_BUCKET,
                                    Key=key,
                                    Body=payload,
                                    ContentType="application/x-ndjson; charset=utf-8",
                                )
                                if _stored_checksum(client, key) != checksum:
                                    raise ValueError(
                                        f"{name}: uploaded object checksum verification failed"
                                    )
                                publication[name] = "uploaded_verified"
                            entries[name] = {"key": key, "count": count, "sha256": checksum}
                    if _processing_version(connection, batches, identifier, token) != version:
                        raise ValueError("Batch processing_version changed during publication")
    except SQLAlchemyError:
        raise RuntimeError(
            "PostgreSQL publication ownership check failed; details omitted"
        ) from None
    except (BotoCoreError, ClientError):
        raise RuntimeError("Curated S3 publication failed; storage details omitted") from None
    for name, entry in entries.items():
        logger.info("dataset=%s records=%d publication=%s", name, entry["count"], publication[name])
    return {
        "batch_id": str(identifier),
        "processing_version": version,
        "bucket": _BUCKET,
        "datasets": entries,
    }

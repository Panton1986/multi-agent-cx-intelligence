"""Read and verify the two primary S3 datasets for an actively owned batch."""

import hashlib
import json
import logging
import math
import os
import re
from contextlib import ExitStack, closing
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import UUID as PythonUUID

from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import URL, Column, DateTime, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from cx_multiagent.storage.s3_client import create_s3_client

logger = logging.getLogger(__name__)
type Record = dict[str, Any]
_PRIMARY_KEYS = {"support_tickets": "ticket_id", "call_transcripts": "call_id"}


def _uuid_string(value: str, field: str) -> PythonUUID:
    """Validate an identifier without exposing the supplied value in errors."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a UUID string")
    try:
        return PythonUUID(value)
    except ValueError:
        raise ValueError(f"{field} must be a UUID string") from None


def _connection_url() -> URL:
    """Construct a psycopg URL from required PostgreSQL environment settings."""
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


def _owned_branch(
    connection: Connection, batches: Table, batch_id: PythonUUID, claim_token: PythonUUID
) -> Record:
    """Read current ownership and return a snapshot of the loaded SeaweedFS branch."""
    batch = (
        connection.execute(select(batches).where(batches.c.batch_id == batch_id))
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
    manifest = batch["source_manifest"]
    branch = manifest.get("seaweedfs") if isinstance(manifest, dict) else None
    if not isinstance(branch, dict) or branch.get("status") != "loaded":
        raise ValueError("SeaweedFS manifest branch is not loaded")
    if not isinstance(branch.get("datasets"), dict):
        raise ValueError("SeaweedFS dataset manifest is missing")
    return deepcopy(branch)


def _dataset_entry(name: str, entry: Any) -> Record:
    """Validate the existing single-object dataset manifest contract."""
    if not isinstance(entry, dict) or entry.get("status") != "loaded":
        raise ValueError(f"SeaweedFS dataset manifest is not loaded: {name}")
    if any(
        not isinstance(entry.get(field), str) or not entry[field].strip()
        for field in ("bucket", "key")
    ):
        raise ValueError(f"SeaweedFS object location is missing: {name}")
    count = entry.get("count")
    if type(count) is not int or count < 0:
        raise ValueError(f"Invalid SeaweedFS manifest count: {name}")
    checksum = entry.get("sha256")
    if not isinstance(checksum, str) or re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
        raise ValueError(f"Invalid SeaweedFS manifest checksum: {name}")
    return entry


def _unique_object(pairs: list[tuple[str, Any]]) -> Record:
    """Reject duplicate JSON keys, including keys inside nested objects."""
    record: Record = {}
    for key, value in pairs:
        if key in record:
            raise ValueError("Duplicate JSON object key")
        record[key] = value
    return record


def _reject_constant(value: str) -> None:
    """Reject NaN and Infinity rather than accepting nonstandard JSON constants."""
    raise ValueError("Nonstandard JSON numeric constant")


def _finite_float(value: str) -> float:
    """Reject numeric overflow that would otherwise produce a nonfinite float."""
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("JSON number is outside the finite float range")
    return number


def _extract_dataset(client: BaseClient, name: str, entry: Record) -> list[Record]:
    """Download the referenced object, hash its exact bytes, and validate its JSONL."""
    response = client.get_object(Bucket=entry["bucket"], Key=entry["key"])
    payload = bytearray()
    digest = hashlib.sha256()
    with closing(response["Body"]) as body:
        for chunk in body.iter_chunks(chunk_size=65536):
            digest.update(chunk)
            payload.extend(chunk)
    if digest.hexdigest() != entry["sha256"]:
        raise ValueError(f"SeaweedFS object checksum mismatch: {name}")
    try:
        lines = payload.decode("utf-8").split("\n")
    except UnicodeError:
        raise ValueError(f"SeaweedFS object is not valid UTF-8: {name}") from None
    if lines and lines[-1] == "":
        lines.pop()
    records: list[Record] = []
    identifiers: set[str] = set()
    for line_number, line in enumerate(lines, 1):
        try:
            record = json.loads(
                line,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
                parse_float=_finite_float,
            )
            if not isinstance(record, dict):
                raise ValueError("JSONL record must be an object")
            identifier = record.get(_PRIMARY_KEYS[name])
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValueError("Missing primary identifier")
            if identifier in identifiers:
                raise ValueError("Duplicate primary identifier")
            identifiers.add(identifier)
            records.append(record)
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise ValueError(
                f"Invalid SeaweedFS JSONL record: {name}, line {line_number}"
            ) from None
    if len(records) != entry["count"]:
        raise ValueError(f"SeaweedFS extracted record count mismatch: {name}")
    return records


def extract_seaweedfs_batch(batch_id: str, claim_token: str) -> dict[str, list[dict[str, Any]]]:
    """Extract original tickets and calls for a currently owned processing batch.

    Use the existing manifest's bucket, key, count, and SHA-256 for each dataset.
    Preserve nested JSON, nulls, identifiers, timestamps, and source record order.
    Verify active ownership before and after S3 reads, comparing the SeaweedFS
    manifest snapshot against fresh PostgreSQL metadata under READ COMMITTED.
    PostgreSQL transactions are read-only; no S3 writes or listings are performed.

    Consume this result inside a future processing task. Do not return the records
    from an Airflow TaskFlow task or store them in XCom. Subsequent stages must
    also check ownership; this function neither claims batches nor renews leases.

    Raises:
        ValueError: For invalid inputs, configuration, ownership, manifests,
            checksums, JSONL records, identifiers, or record counts.
        RuntimeError: For sanitized PostgreSQL or S3 failures.
    """
    identifier = _uuid_string(batch_id, "batch_id")
    token = _uuid_string(claim_token, "claim_token")
    url = _connection_url()
    batches = Table(
        "ingestion_batches",
        MetaData(),
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("status", Text()),
        Column("claim_token", UUID(as_uuid=True)),
        Column("lease_expires_at", DateTime(timezone=True)),
        Column("source_manifest", JSONB(none_as_null=True)),
        schema="pipeline",
    )
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
                    branch = _owned_branch(connection, batches, identifier, token)
                    entries = {
                        name: _dataset_entry(name, branch["datasets"].get(name))
                        for name in _PRIMARY_KEYS
                    }
                    try:
                        client = create_s3_client()
                    except ValueError:
                        raise ValueError(
                            "Check required SeaweedFS environment configuration"
                        ) from None
                    with closing(client):
                        extracted = {
                            name: _extract_dataset(client, name, entry)
                            for name, entry in entries.items()
                        }
                    if _owned_branch(connection, batches, identifier, token) != branch:
                        raise ValueError("SeaweedFS batch manifest changed during extraction")
    except SQLAlchemyError:
        raise RuntimeError(
            "PostgreSQL batch verification failed; database details omitted"
        ) from None
    except (BotoCoreError, ClientError):
        raise RuntimeError("SeaweedFS batch extraction failed; storage details omitted") from None
    for name, records in extracted.items():
        logger.info("dataset=%s records=%d verification=passed", name, len(records))
    return extracted

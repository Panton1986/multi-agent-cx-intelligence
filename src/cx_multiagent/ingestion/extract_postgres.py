"""Extract original PostgreSQL records belonging to a currently owned batch."""

import hashlib
import logging
import os
import re
from contextlib import ExitStack
from datetime import UTC, datetime
from typing import Any
from uuid import UUID as PythonUUID

from sqlalchemy import URL, Column, DateTime, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from cx_multiagent.database.load_initial_postgres import (
    _DATASETS,
    _canonical_record,
    _Dataset,
    _source_table,
)

logger = logging.getLogger(__name__)
_CHUNK_SIZE = 500
type Record = dict[str, Any]


def _uuid_string(value: str, field: str) -> PythonUUID:
    """Validate a UUID string without including its supplied value in errors."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a UUID string")
    try:
        return PythonUUID(value)
    except ValueError:
        raise ValueError(f"{field} must be a UUID string") from None


def _connection_url() -> URL:
    """Build a psycopg connection URL from required environment variables."""
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


def _owned_manifest(
    connection: Connection, batches: Table, batch_id: PythonUUID, claim_token: PythonUUID
) -> Record:
    """Read the manifest only for a processing batch with matching active ownership."""
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
    if not isinstance(manifest, dict):
        raise ValueError("Batch source manifest must be an object")
    branch = manifest.get("postgresql")
    if not isinstance(branch, dict) or branch.get("status") != "loaded":
        raise ValueError("PostgreSQL manifest branch is not loaded")
    if not isinstance(branch.get("datasets"), dict):
        raise ValueError("PostgreSQL dataset membership is missing")
    return branch


def _membership(spec: _Dataset, entry: Any) -> tuple[list[str], str]:
    """Validate fixed table identity, unique membership, count, and checksum metadata."""
    if not isinstance(entry, dict):
        raise ValueError(f"Missing dataset manifest: {spec.name}")
    if entry.get("table") != f"source.{spec.name}" or entry.get("primary_key") != spec.primary_key:
        raise ValueError(f"Unexpected table or primary key in manifest: {spec.name}")
    identifiers = entry.get("ids")
    count = entry.get("count")
    checksum = entry.get("sha256")
    if not isinstance(identifiers, list) or any(
        not isinstance(identifier, str) or not identifier.strip() for identifier in identifiers
    ):
        raise ValueError(f"Invalid dataset membership IDs: {spec.name}")
    if len(set(identifiers)) != len(identifiers):
        raise ValueError(f"Duplicated dataset membership IDs: {spec.name}")
    if type(count) is not int or count < 0 or count != len(identifiers):
        raise ValueError(f"Dataset membership count mismatch: {spec.name}")
    if not isinstance(checksum, str) or re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
        raise ValueError(f"Invalid dataset checksum in manifest: {spec.name}")
    return sorted(identifiers), checksum


def _extract_dataset(
    connection: Connection, table: Table, spec: _Dataset, entry: Any
) -> list[Record]:
    """Read only manifest IDs in chunks and verify canonical content fingerprints."""
    identifiers, expected_checksum = _membership(spec, entry)
    indexed: dict[str, Record] = {}
    for offset in range(0, len(identifiers), _CHUNK_SIZE):
        chunk = identifiers[offset : offset + _CHUNK_SIZE]
        rows = (
            connection.execute(select(table).where(table.c[spec.primary_key].in_(chunk)))
            .mappings()
            .all()
        )
        allowed = set(chunk)
        for row in rows:
            identifier = row[spec.primary_key]
            if identifier not in allowed or identifier in indexed:
                raise ValueError(f"Unexpected or duplicated extracted ID: {spec.name}")
            indexed[identifier] = dict(row)
    if set(indexed) != set(identifiers) or len(indexed) != entry["count"]:
        raise ValueError(f"Missing records or extracted count mismatch: {spec.name}")
    records = [indexed[identifier] for identifier in identifiers]
    digest = hashlib.sha256()
    try:
        for record in records:
            digest.update((_canonical_record(record) + "\n").encode("utf-8"))
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(f"Cannot canonically serialize extracted dataset: {spec.name}") from None
    if digest.hexdigest() != expected_checksum:
        raise ValueError(f"Extracted dataset checksum mismatch: {spec.name}")
    return records


def extract_postgres_batch(batch_id: str, claim_token: str) -> dict[str, list[dict[str, Any]]]:
    """Return the five original structured datasets for a valid active batch claim.

    Read settings from the environment when called. Use a PostgreSQL read-only,
    READ COMMITTED transaction, parameterized membership queries, and the initial
    loader's exact column definitions and canonical serializer. Preserve aware
    datetime values, JSON structures, identifiers, and SQL nulls without masking.
    Validate ownership both before extraction and again against current metadata
    before returning; subsequent pipeline stages must also verify ownership.

    Consume these records within the same future Airflow processing task. Never
    return this large result from a TaskFlow task or place it in Airflow XCom.
    No database or SeaweedFS records are modified, and no lease is renewed.

    Raises:
        ValueError: For invalid UUIDs/configuration, ownership, manifest membership,
            missing records, or content checksum mismatches.
        RuntimeError: For sanitized database errors.
    """
    identifier = _uuid_string(batch_id, "batch_id")
    token = _uuid_string(claim_token, "claim_token")
    url = _connection_url()
    metadata = MetaData()
    tables = {spec.name: _source_table(spec, metadata) for spec in _DATASETS}
    batches = Table(
        "ingestion_batches",
        metadata,
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
                    branch = _owned_manifest(connection, batches, identifier, token)
                    extracted = {
                        spec.name: _extract_dataset(
                            connection, tables[spec.name], spec, branch["datasets"].get(spec.name)
                        )
                        for spec in _DATASETS
                    }
                    if _owned_manifest(connection, batches, identifier, token) != branch:
                        raise ValueError("PostgreSQL batch manifest changed during extraction")
    except SQLAlchemyError:
        raise RuntimeError("PostgreSQL batch extraction failed; database details omitted") from None
    for name, records in extracted.items():
        logger.info("dataset=%s records=%d verification=passed", name, len(records))
    return extracted

"""Read verified completed curated outputs without changing pipeline state."""

import hashlib
import json
from collections.abc import Iterator
from contextlib import ExitStack, closing, contextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID as PythonUUID

from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import Column, DateTime, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Connection

from cx_multiagent.database.claim_batch import _connection_url
from cx_multiagent.database.complete_batch import _validate_manifest
from cx_multiagent.ingestion.extract_seaweedfs import (
    _finite_float,
    _reject_constant,
    _unique_object,
)
from cx_multiagent.storage.s3_client import create_s3_client

type Record = dict[str, Any]
_PRIMARY_KEYS = {
    "support_tickets": "ticket_id",
    "call_transcripts": "call_id",
    "survey_responses": "survey_id",
    "product_releases": "release_id",
}
_ID_PREFIXES = {
    "support_tickets": "tkt_",
    "call_transcripts": "cal_",
    "survey_responses": "sur_",
    "product_releases": "rel_",
}


def _batch_table() -> Table:
    """Declare existing batch metadata without creating database objects."""
    return Table(
        "ingestion_batches",
        MetaData(),
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("batch_kind", Text()),
        Column("processing_version", Text()),
        Column("status", Text()),
        Column("created_at", DateTime(timezone=True)),
        Column("completed_at", DateTime(timezone=True)),
        Column("output_manifest", JSONB(none_as_null=True)),
        schema="pipeline",
    )


@contextmanager
def _read_connection() -> Iterator[Connection]:
    """Reuse environment-based psycopg URL creation and enforce read-only SQL."""
    from sqlalchemy.exc import SQLAlchemyError

    try:
        with ExitStack() as resources:
            engine = create_engine(
                _connection_url(),
                echo=False,
                hide_parameters=True,
                isolation_level="READ COMMITTED",
                connect_args={"connect_timeout": 10},
            )
            resources.callback(engine.dispose)
            with engine.connect() as connection:
                connection = connection.execution_options(postgresql_readonly=True)
                with connection.begin():
                    yield connection
    except SQLAlchemyError:
        raise RuntimeError(
            "Completed curated batch metadata read failed; database details omitted"
        ) from None


def _completed_manifest(connection: Connection, table: Table, identifier: PythonUUID) -> Record:
    """Require completed state and validate the current seven-dataset output manifest."""
    batch = (
        connection.execute(
            select(table).where(table.c.batch_id == identifier, table.c.status == "completed")
        )
        .mappings()
        .one_or_none()
    )
    if batch is None:
        raise ValueError("Requested batch does not exist or is not completed")
    return _validate_manifest(batch["output_manifest"], identifier, batch["processing_version"])


def list_completed_curated_batches() -> list[dict[str, Any]]:
    """Return completed batch IDs, versions, completion times, and selected counts.

    Order by completed_at, created_at, and batch_id. Validate each stored output
    manifest, but do not download objects or imply indexing/privacy verification.
    Return no records, source membership, claim tokens, or full output manifests.
    No completed batches returns an empty list. Configuration comes from the five
    CX_POSTGRES environment variables through the existing URL.create helper.
    """
    table = _batch_table()
    metadata: list[dict[str, Any]] = []
    with _read_connection() as connection:
        batches = connection.execute(
            select(table)
            .where(table.c.status == "completed")
            .order_by(table.c.completed_at, table.c.created_at, table.c.batch_id)
        ).mappings()
        for batch in batches:
            manifest = _validate_manifest(
                batch["output_manifest"], batch["batch_id"], batch["processing_version"]
            )
            completed_at = batch["completed_at"]
            if (
                not isinstance(completed_at, datetime)
                or completed_at.tzinfo is None
                or completed_at.utcoffset() is None
            ):
                raise ValueError("Completed batch has an invalid completion timestamp")
            metadata.append(
                {
                    "batch_id": str(batch["batch_id"]),
                    "processing_version": batch["processing_version"],
                    "completed_at": completed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                    "datasets": {
                        name: {"count": manifest["datasets"][name]["count"]}
                        for name in _PRIMARY_KEYS
                    },
                }
            )
    return metadata


def _parse_records(payload: bytearray, dataset: str, expected_count: int) -> list[Record]:
    """Parse verified UTF-8 JSONL, rejecting invalid objects, numbers, and IDs."""
    try:
        lines = payload.decode("utf-8").split("\n")
    except UnicodeError:
        raise ValueError(f"{dataset}: curated object is not valid UTF-8") from None
    if lines and lines[-1] == "":
        lines.pop()
    records: list[Record] = []
    identifiers: set[str] = set()
    prefix = _ID_PREFIXES[dataset]
    for position, line in enumerate(lines, 1):
        try:
            record = json.loads(
                line,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
                parse_float=_finite_float,
            )
            if not isinstance(record, dict):
                raise ValueError("JSONL record must be an object")
            identifier = record.get(_PRIMARY_KEYS[dataset])
            if (
                not isinstance(identifier, str)
                or not identifier.startswith(prefix)
                or not identifier[len(prefix) :].strip()
                or identifier in identifiers
            ):
                raise ValueError("Missing, invalid, or duplicate primary identifier")
            identifiers.add(identifier)
            records.append(record)
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise ValueError(
                f"{dataset}, record {position}: invalid curated JSONL or primary ID"
            ) from None
    if len(records) != expected_count:
        raise ValueError(f"{dataset}: curated record count differs from output manifest")
    return records


def read_completed_curated_dataset(batch_id: str, dataset: str) -> list[dict[str, Any]]:
    """Read one supported curated dataset only after verifying its exact-byte SHA-256.

    Resolve the object from a completed batch's validated output_manifest. Never
    read raw or legacy objects. Preserve JSON types, nested transcripts, nulls,
    identifiers, and timestamp strings without masking or other transformations.
    Recheck completed state and unchanged output metadata before returning.
    Checksums establish integrity, not privacy safety; a separate privacy stage
    must run before indexing. Returned records must not be placed in Airflow XCom.

    Raises:
        ValueError: For invalid inputs, non-completed batches, invalid manifests,
            checksums, UTF-8/JSONL, counts, or primary IDs.
        RuntimeError: For sanitized database or S3 failures.
    """
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("batch_id must be a UUID string")
    try:
        identifier = PythonUUID(batch_id)
    except ValueError:
        raise ValueError("batch_id must be a UUID string") from None
    if not isinstance(dataset, str) or dataset not in _PRIMARY_KEYS:
        raise ValueError("Unsupported curated dataset")
    table = _batch_table()
    try:
        with _read_connection() as connection:
            manifest = _completed_manifest(connection, table, identifier)
            entry = manifest["datasets"][dataset]
            try:
                client = create_s3_client()
            except ValueError:
                raise ValueError("Check required SeaweedFS environment configuration") from None
            payload = bytearray()
            digest = hashlib.sha256()
            with closing(client):
                response = client.get_object(Bucket=manifest["bucket"], Key=entry["key"])
                with closing(response["Body"]) as body:
                    for chunk in body.iter_chunks(chunk_size=65536):
                        digest.update(chunk)
                        payload.extend(chunk)
            if digest.hexdigest() != entry["sha256"]:
                raise ValueError(f"{dataset}: curated object checksum mismatch")
            records = _parse_records(payload, dataset, entry["count"])
            if _completed_manifest(connection, table, identifier) != manifest:
                raise ValueError("Completed curated output metadata changed during reading")
    except (BotoCoreError, ClientError):
        raise RuntimeError("Curated dataset download failed; storage details omitted") from None
    return records

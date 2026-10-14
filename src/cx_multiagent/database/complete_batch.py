"""Verify curated publication and atomically complete an owned ingestion batch."""

import hashlib
import logging
import os
import re
from collections.abc import Mapping
from contextlib import ExitStack, closing
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import UUID as PythonUUID

from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import URL, Column, DateTime, MetaData, Table, Text, create_engine, func, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.exc import SQLAlchemyError

from cx_multiagent.storage.s3_client import create_s3_client

logger = logging.getLogger(__name__)
type Manifest = dict[str, Any]
_SOURCE_DATASETS = {
    "postgresql": (
        "customers",
        "product_releases",
        "customer_journey_events",
        "mobile_application_logs",
        "survey_responses",
    ),
    "seaweedfs": ("support_tickets", "call_transcripts"),
}
_DATASETS = tuple(name for names in _SOURCE_DATASETS.values() for name in names)


def _uuid_string(value: str, field: str) -> PythonUUID:
    """Parse a UUID without including its supplied value in error messages."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a UUID string")
    try:
        return PythonUUID(value)
    except ValueError:
        raise ValueError(f"{field} must be a UUID string") from None


def _connection_url() -> URL:
    """Build a psycopg URL from required environment configuration."""
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


def _batch_table() -> Table:
    """Declare only existing metadata columns used by completion."""
    return Table(
        "ingestion_batches",
        MetaData(),
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("producer_key", Text()),
        Column("processing_version", Text()),
        Column("status", Text()),
        Column("claim_token", UUID(as_uuid=True)),
        Column("lease_expires_at", DateTime(timezone=True)),
        Column("completed_at", DateTime(timezone=True)),
        Column("source_manifest", JSONB(none_as_null=True)),
        Column("output_manifest", JSONB(none_as_null=True)),
        schema="pipeline",
    )


def _validate_manifest(manifest: Any, batch_id: PythonUUID, version: str) -> Manifest:
    """Validate the exporter's exact manifest shape and immutable object paths."""
    if not isinstance(manifest, dict) or set(manifest) != {
        "batch_id",
        "processing_version",
        "bucket",
        "datasets",
    }:
        raise ValueError("Output manifest has an unexpected structure")
    if not isinstance(version, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version) is None:
        raise ValueError("Batch processing_version is not a safe S3 path component")
    if manifest["batch_id"] != str(batch_id) or manifest["processing_version"] != version:
        raise ValueError("Output manifest batch identity or processing_version does not match")
    if manifest["bucket"] != "cx-curated":
        raise ValueError("Output manifest bucket must be cx-curated")
    datasets = manifest["datasets"]
    if not isinstance(datasets, dict) or set(datasets) != set(_DATASETS):
        raise ValueError("Output manifest must contain exactly seven datasets")
    entries: Manifest = {}
    for name in _DATASETS:
        entry = datasets[name]
        if not isinstance(entry, dict) or set(entry) != {"key", "count", "sha256"}:
            raise ValueError(f"{name}: unexpected output dataset manifest structure")
        if entry["key"] != f"batches/{batch_id}/{version}/{name}.jsonl":
            raise ValueError(f"{name}: unexpected curated object path")
        if type(entry["count"]) is not int or entry["count"] < 0:
            raise ValueError(f"{name}: output count must be a nonnegative integer")
        checksum = entry["sha256"]
        if not isinstance(checksum, str) or re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
            raise ValueError(f"{name}: output checksum must be lowercase SHA-256 hexadecimal")
        entries[name] = dict(entry)
    return {
        "batch_id": str(batch_id),
        "processing_version": version,
        "bucket": "cx-curated",
        "datasets": entries,
    }


def _validate_batch(batch: Mapping[str, Any] | None, token: PythonUUID, manifest: Manifest) -> None:
    """Check lifecycle, ownership, both source branches, and exact source/output counts."""
    if batch is None:
        raise ValueError("Batch does not exist")
    _validate_manifest(manifest, batch["batch_id"], batch["processing_version"])
    if batch["status"] == "completed":
        _validate_manifest(batch["output_manifest"], batch["batch_id"], batch["processing_version"])
        if batch["output_manifest"] != manifest:
            raise ValueError("Completed batch has conflicting output metadata")
        if batch["completed_at"] is None:
            raise ValueError("Completed batch is missing its completion timestamp")
    elif batch["status"] == "processing":
        if batch["claim_token"] != token:
            raise ValueError("Batch ownership does not match the supplied claim token")
        expiry = batch["lease_expires_at"]
        if (
            not isinstance(expiry, datetime)
            or expiry.tzinfo is None
            or expiry.utcoffset() is None
            or expiry <= datetime.now(UTC)
        ):
            raise ValueError("Batch processing lease has expired or is invalid")
        if batch["output_manifest"] is not None and batch["output_manifest"] != manifest:
            raise ValueError("Processing batch has conflicting output metadata")
    else:
        raise ValueError("Batch status does not permit completion")
    source = batch["source_manifest"]
    if not isinstance(source, dict):
        raise ValueError("Batch source manifest must be an object")
    for branch_name, names in _SOURCE_DATASETS.items():
        branch = source.get(branch_name)
        if not isinstance(branch, dict) or branch.get("status") != "loaded":
            raise ValueError("Both primary source manifest branches must be loaded")
        datasets = branch.get("datasets")
        if not isinstance(datasets, dict):
            raise ValueError("Source dataset manifest is missing")
        for name in names:
            entry = datasets.get(name)
            if (
                not isinstance(entry, dict)
                or type(entry.get("count")) is not int
                or entry["count"] < 0
                or entry["count"] != manifest["datasets"][name]["count"]
            ):
                raise ValueError(f"{name}: curated count does not match source manifest")


def _verify_objects(client: BaseClient, manifest: Manifest) -> None:
    """Download and hash every curated object, closing its body on all paths."""
    for name, entry in manifest["datasets"].items():
        response = client.get_object(Bucket=manifest["bucket"], Key=entry["key"])
        digest = hashlib.sha256()
        with closing(response["Body"]) as body:
            for chunk in body.iter_chunks(chunk_size=65536):
                digest.update(chunk)
        if digest.hexdigest() != entry["sha256"]:
            raise ValueError(f"{name}: stored curated object checksum mismatch")


def complete_batch(
    batch_id: str, claim_token: str, output_manifest: dict[str, Any]
) -> dict[str, str]:
    """Verify publication and complete a batch in a short locked SQL transaction.

    Validate the exporter manifest against database versions and source counts.
    Verify all seven S3 objects before acquiring the final row lock. Recheck
    current lifecycle, ownership, lease, versions, and manifest consistency under
    SELECT FOR UPDATE; only the completion fields are updated. Matching completed
    retries still verify S3 content but leave all database fields unchanged. Their
    claim token has already been cleared, so active-ownership checks apply only
    to processing batches. No lease renewal or expired-claim recovery is performed.

    S3 verification and PostgreSQL completion are not a distributed transaction.
    Immutable batch-specific objects and the single-writer ownership/lease protocol
    are required to prevent changes between verification and commit. No S3 writes,
    completion markers, source-table changes, or source-manifest changes occur.

    Raises:
        ValueError: For invalid identifiers, manifests, ownership, lease, lifecycle,
            counts, checksums, or metadata changes during verification.
        RuntimeError: For sanitized PostgreSQL or S3 errors.
    """
    identifier = _uuid_string(batch_id, "batch_id")
    token = _uuid_string(claim_token, "claim_token")
    url = _connection_url()
    table = _batch_table()
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
            # End the unlocked metadata read before S3 verification.
            with engine.connect() as connection, connection.begin():
                snapshot = (
                    connection.execute(select(table).where(table.c.batch_id == identifier))
                    .mappings()
                    .one_or_none()
                )
                if snapshot is None:
                    raise ValueError("Batch does not exist")
                manifest = _validate_manifest(
                    output_manifest, identifier, snapshot["processing_version"]
                )
                _validate_batch(snapshot, token, manifest)
                snapshot = deepcopy(dict(snapshot))
            try:
                client = create_s3_client()
            except ValueError:
                raise ValueError("Check required SeaweedFS environment configuration") from None
            with closing(client):
                _verify_objects(client, manifest)
            with engine.begin() as connection:
                current = (
                    connection.execute(
                        select(table).where(table.c.batch_id == identifier).with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                _validate_batch(current, token, manifest)
                if current is None:
                    raise ValueError("Batch does not exist")
                if (
                    current["source_manifest"] != snapshot["source_manifest"]
                    or current["processing_version"] != snapshot["processing_version"]
                    or current["producer_key"] != snapshot["producer_key"]
                ):
                    raise ValueError("Batch metadata changed during publication verification")
                if current["status"] != "completed":
                    if snapshot["status"] != "processing":
                        raise ValueError("Completed batch cannot transition back to processing")
                    if current["output_manifest"] != snapshot["output_manifest"]:
                        raise ValueError("Output metadata changed during publication verification")
                    # Also guard the lease using the database clock at the update statement.
                    updated = connection.execute(
                        table.update()
                        .where(
                            table.c.batch_id == identifier,
                            table.c.status == "processing",
                            table.c.claim_token == token,
                            table.c.lease_expires_at > func.clock_timestamp(),
                        )
                        .values(
                            status="completed",
                            completed_at=datetime.now(UTC),
                            output_manifest=manifest,
                            claim_token=None,
                            lease_expires_at=None,
                        )
                        .returning(table.c.batch_id)
                    ).scalar_one_or_none()
                    if updated is None:
                        raise ValueError("Batch claim expired or changed before completion")
                result = {
                    "batch_id": str(identifier),
                    "producer_key": current["producer_key"],
                    "status": "completed",
                }
    except SQLAlchemyError:
        raise RuntimeError("PostgreSQL batch completion failed; transaction rolled back") from None
    except (BotoCoreError, ClientError):
        raise RuntimeError(
            "Curated S3 verification failed; batch completion was not committed"
        ) from None
    logger.info(
        "batch_status=completed datasets=%d records=%d",
        len(manifest["datasets"]),
        sum(entry["count"] for entry in manifest["datasets"].values()),
    )
    return result

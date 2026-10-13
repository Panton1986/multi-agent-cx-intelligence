"""Verify the initial parallel-source delivery and mark its batch ready."""

import hashlib
import json
import logging
import os
from contextlib import ExitStack, closing
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from sqlalchemy import URL, Column, DateTime, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.exc import SQLAlchemyError

from cx_multiagent.storage.s3_client import create_s3_client

logger = logging.getLogger(__name__)
type Manifest = dict[str, Any]

_POSTGRES_COUNTS = {
    "customers": 500,
    "product_releases": 20,
    "customer_journey_events": 15000,
    "mobile_application_logs": 5000,
    "survey_responses": 1000,
}
_S3_BASELINE = {
    "support_tickets": (
        "ticket_id",
        1500,
        "25a14b7e1d414a857c60256ea6033966bce256de0e33092eea5586910b84b9c3",
    ),
    "call_transcripts": (
        "call_id",
        500,
        "c54b56dc062be1e0c8bc1af92073159ff42d0177080b20a85a45bb544708913f",
    ),
}


def _reject_constant(value: str) -> None:
    """Reject nonstandard JSON numeric constants."""
    raise ValueError("Nonstandard JSON number")


def _unique_object(pairs: list[tuple[str, Any]]) -> Manifest:
    """Reject duplicate object keys rather than silently replacing values."""
    result: Manifest = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


def _verify_s3_dataset(client: BaseClient, name: str) -> Manifest:
    """Verify original object bytes, JSONL objects, unique IDs, and baseline count."""
    primary_key, expected_count, expected_checksum = _S3_BASELINE[name]
    key = f"synthetic/initial/v1/{name}.jsonl"
    response = client.get_object(Bucket="cx-raw", Key=key)
    digest = hashlib.sha256()
    payload = bytearray()
    with closing(response["Body"]) as body:
        for chunk in body.iter_chunks(chunk_size=65536):
            digest.update(chunk)
            payload.extend(chunk)
    checksum = digest.hexdigest()
    if checksum != expected_checksum:
        raise ValueError(f"Initial S3 baseline checksum mismatch: {name}")
    try:
        lines = payload.decode("utf-8").split("\n")
    except UnicodeError:
        raise ValueError(f"S3 dataset is not valid UTF-8: {name}") from None
    if lines and lines[-1] == "":
        lines.pop()
    identifiers: set[str] = set()
    for line_number, line in enumerate(lines, 1):
        try:
            record = json.loads(
                line, parse_constant=_reject_constant, object_pairs_hook=_unique_object
            )
            if not isinstance(record, dict):
                raise ValueError("JSONL record must be an object")
            identifier = record.get(primary_key)
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValueError("Missing primary identifier")
            if identifier in identifiers:
                raise ValueError("Duplicate primary identifier")
            identifiers.add(identifier)
        except (ValueError, TypeError, RecursionError):
            raise ValueError(f"Invalid S3 JSONL record: {name}, line {line_number}") from None
    if len(identifiers) != expected_count:
        raise ValueError(f"Unexpected initial S3 record count: {name}")
    return {
        "status": "loaded",
        "bucket": "cx-raw",
        "key": key,
        "count": len(identifiers),
        "sha256": checksum,
    }


def _verify_postgres_manifest(manifest: Manifest) -> None:
    """Require the loader's completed SQL branch and expected initial counts."""
    branch = manifest.get("postgresql")
    if not isinstance(branch, dict) or branch.get("status") != "loaded":
        raise ValueError("Initial PostgreSQL manifest branch is not loaded")
    datasets = branch.get("datasets")
    if not isinstance(datasets, dict):
        raise ValueError("Initial PostgreSQL dataset manifest is missing")
    for name, expected_count in _POSTGRES_COUNTS.items():
        entry = datasets.get(name)
        if (
            not isinstance(entry, dict)
            or type(entry.get("count")) is not int
            or entry["count"] != expected_count
        ):
            raise ValueError(f"Unexpected initial PostgreSQL manifest count: {name}")


def _verified_s3_branch(manifest: Manifest, verified: Manifest, *, ready: bool) -> Manifest:
    """Merge verified S3 metadata while rejecting conflicting completed information."""
    branch = deepcopy(manifest.get("seaweedfs", {}))
    if not isinstance(branch, dict):
        raise ValueError("SeaweedFS manifest branch must be an object")
    datasets = branch.setdefault("datasets", {})
    if not isinstance(datasets, dict):
        raise ValueError("SeaweedFS dataset manifest must be an object")
    if ready and branch.get("status") != "loaded":
        raise ValueError("Ready batch does not have a loaded SeaweedFS manifest")
    for name, expected in verified.items():
        entry = datasets.get(name, {})
        if not isinstance(entry, dict):
            raise ValueError(f"Invalid SeaweedFS dataset manifest: {name}")
        if ready or branch.get("status") == "loaded" or entry.get("status") == "loaded":
            if any(entry.get(field) != value for field, value in expected.items()):
                raise ValueError(
                    f"Stored SeaweedFS manifest conflicts with verified source: {name}"
                )
        datasets[name] = {**entry, **expected}
    branch["status"] = "loaded"
    return branch


def finalize_initial_batch() -> dict[str, int]:
    """Verify the S3 baseline and SQL manifest, then atomically finalize readiness.

    Read environment configuration only when called. Hash unmodified S3 bytes and
    close response bodies before locking the database batch. Require the initial
    loader's completed PostgreSQL manifest; do not read or modify business tables.
    Preserve all existing manifest information except the verified S3 readiness
    fields. An already-ready matching batch succeeds without changing ready_at.
    Processing, completed, failed, or incompatible batches are never reset.

    Raises:
        ValueError: For missing configuration, invalid sources/manifests, missing
            batches, or a batch state that cannot be finalized.
        RuntimeError: For sanitized S3 or database failures.
    """
    project_root = Path(__file__).resolve().parents[3]
    load_dotenv(dotenv_path=project_root / ".env", override=False)
    password = os.environ.get("CX_POSTGRES_PASSWORD")
    if not password:
        raise ValueError("Set CX_POSTGRES_PASSWORD before finalizing the initial batch")
    url = URL.create(
        "postgresql+psycopg",
        username="cx_app",
        password=password,
        host="127.0.0.1",
        port=5433,
        database="cx_intelligence",
    )
    batches = Table(
        "ingestion_batches",
        MetaData(),
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("producer_key", Text()),
        Column("batch_kind", Text()),
        Column("schema_version", Text()),
        Column("processing_version", Text()),
        Column("status", Text()),
        Column("ready_at", DateTime(timezone=True)),
        Column("source_manifest", JSONB(none_as_null=True)),
        schema="pipeline",
    )
    try:
        with ExitStack() as resources:
            try:
                client = create_s3_client()
            except ValueError:
                raise ValueError("Check required SeaweedFS environment configuration") from None
            resources.callback(client.close)
            verified = {name: _verify_s3_dataset(client, name) for name in _S3_BASELINE}
            engine = create_engine(
                url, echo=False, hide_parameters=True, connect_args={"connect_timeout": 10}
            )
            resources.callback(engine.dispose)
            with engine.begin() as connection:
                batch = (
                    connection.execute(
                        select(batches)
                        .where(batches.c.producer_key == "synthetic-initial-v1")
                        .with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                if batch is None:
                    raise ValueError("Initial ingestion batch does not exist")
                if batch["status"] not in {"loading", "ready"}:
                    raise ValueError("Initial batch cannot be finalized in its current state")
                if any(
                    batch[field] != value
                    for field, value in (
                        ("batch_kind", "initial"),
                        ("schema_version", "v1"),
                        ("processing_version", "v1"),
                    )
                ):
                    raise ValueError("Initial batch has incompatible delivery versions")
                manifest = batch["source_manifest"]
                if not isinstance(manifest, dict):
                    raise ValueError("Initial source manifest must be an object")
                _verify_postgres_manifest(manifest)
                already_ready = batch["status"] == "ready"
                branch = _verified_s3_branch(manifest, verified, ready=already_ready)
                if already_ready:
                    if batch["ready_at"] is None:
                        raise ValueError("Ready batch is missing its readiness timestamp")
                else:
                    updated = deepcopy(manifest)
                    updated["seaweedfs"] = branch
                    connection.execute(
                        batches.update()
                        .where(batches.c.batch_id == batch["batch_id"])
                        .values(source_manifest=updated, status="ready", ready_at=datetime.now(UTC))
                    )
    except (BotoCoreError, ClientError):
        raise RuntimeError(
            "SeaweedFS verification failed; initial batch was not finalized"
        ) from None
    except SQLAlchemyError:
        raise RuntimeError(
            "PostgreSQL finalization failed; metadata transaction rolled back"
        ) from None
    counts = {name: entry["count"] for name, entry in verified.items()}
    for name, count in counts.items():
        logger.info("dataset=%s records=%d", name, count)
    logger.info("batch_status=ready")
    return counts

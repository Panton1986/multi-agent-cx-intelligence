"""Record a sanitized processing failure only for the current active batch owner."""

import logging
import os
from contextlib import ExitStack
from datetime import UTC, datetime
from uuid import UUID as PythonUUID

from sqlalchemy import URL, Column, DateTime, MetaData, Table, Text, create_engine, func, select
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
_FAILURE_SUMMARIES = {
    "extraction": "Batch extraction failed; explicit recovery is required.",
    "validation": "Batch validation failed; explicit recovery is required.",
    "masking": "Batch masking failed; explicit recovery is required.",
    "publication": "Batch publication failed; explicit recovery is required.",
    "completion": "Batch completion failed; explicit recovery is required.",
    "unknown": "Batch processing failed; explicit recovery is required.",
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
    """Construct a psycopg URL from required environment settings."""
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


def fail_batch(batch_id: str, claim_token: str, failed_stage: str) -> dict[str, str]:
    """Mark an actively owned processing batch failed in one locked transaction.

    Accept only predefined stage names and store a fixed generic summary, never
    an Airflow exception message. Lock the row with SELECT FOR UPDATE, verify the
    claim and active lease, and guard the update against database-clock expiry.
    Clear only active ownership fields along with setting the failure metadata.
    Preserve all other metadata, source records, and any partial curated objects.

    Already-failed batches are rejected without updates; they require an explicit
    separate recovery procedure. Loading, ready, and completed batches cannot be
    failed here. No retry, lease renewal, expired-lease recovery, or S3 operation
    is performed. Connection settings are read only when this function is called.

    Raises:
        ValueError: For invalid inputs/configuration, missing batches, forbidden
            lifecycle states, mismatched ownership, or invalid/expired leases.
        RuntimeError: For sanitized SQLAlchemy failures; writes are rolled back.
    """
    identifier = _uuid_string(batch_id, "batch_id")
    token = _uuid_string(claim_token, "claim_token")
    if not isinstance(failed_stage, str) or failed_stage not in _FAILURE_SUMMARIES:
        raise ValueError(
            "failed_stage must be extraction, validation, masking, publication, completion, or unknown"
        )
    url = _connection_url()
    batches = Table(
        "ingestion_batches",
        MetaData(),
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("producer_key", Text()),
        Column("status", Text()),
        Column("claim_token", UUID(as_uuid=True)),
        Column("lease_expires_at", DateTime(timezone=True)),
        Column("failed_stage", Text()),
        Column("error_summary", Text()),
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
            with engine.begin() as connection:
                batch = (
                    connection.execute(
                        select(batches).where(batches.c.batch_id == identifier).with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                if batch is None:
                    raise ValueError("Batch does not exist")
                if batch["status"] == "failed":
                    raise ValueError("Batch is already failed and requires explicit recovery")
                if batch["status"] != "processing":
                    raise ValueError("Only a processing batch can be failed by this handler")
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
                # The final predicate also blocks lease expiry between the read and update.
                updated = (
                    connection.execute(
                        batches.update()
                        .where(
                            batches.c.batch_id == identifier,
                            batches.c.status == "processing",
                            batches.c.claim_token == token,
                            batches.c.lease_expires_at > func.clock_timestamp(),
                        )
                        .values(
                            status="failed",
                            failed_stage=failed_stage,
                            error_summary=_FAILURE_SUMMARIES[failed_stage],
                            claim_token=None,
                            lease_expires_at=None,
                        )
                        .returning(
                            batches.c.batch_id,
                            batches.c.producer_key,
                            batches.c.status,
                            batches.c.failed_stage,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if updated is None:
                    raise ValueError("Batch claim expired or changed before recording failure")
                result = {
                    "batch_id": str(updated["batch_id"]),
                    "producer_key": updated["producer_key"],
                    "status": updated["status"],
                    "failed_stage": updated["failed_stage"],
                }
    except SQLAlchemyError:
        raise RuntimeError(
            "PostgreSQL batch failure update failed; transaction rolled back"
        ) from None
    logger.info("batch_status=failed failed_stage=%s", failed_stage)
    return result

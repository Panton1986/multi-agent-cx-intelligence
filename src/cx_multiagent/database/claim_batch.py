"""Claim a ready parallel-source batch without extracting its business records."""

import hashlib
import logging
import os
from collections.abc import Mapping
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID as PythonUUID
from uuid import uuid4

from sqlalchemy import (
    URL,
    Column,
    DateTime,
    Integer,
    MetaData,
    Table,
    Text,
    create_engine,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def _connection_url() -> URL:
    """Read required environment settings without exposing configuration values."""
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
    """Declare existing metadata columns without creating database objects."""
    return Table(
        "ingestion_batches",
        MetaData(),
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("producer_key", Text()),
        Column("processing_version", Text()),
        Column("status", Text()),
        Column("created_at", DateTime(timezone=True)),
        Column("ready_at", DateTime(timezone=True)),
        Column("started_at", DateTime(timezone=True)),
        Column("claim_token", UUID(as_uuid=True)),
        Column("airflow_run_id", Text()),
        Column("lease_expires_at", DateTime(timezone=True)),
        Column("attempt_count", Integer()),
        Column("source_manifest", JSONB(none_as_null=True)),
        schema="pipeline",
    )


def _claim_metadata(batch: Mapping[str, Any]) -> dict[str, str]:
    """Return only small claim identifiers suitable for Airflow XCom."""
    fields = ("batch_id", "producer_key", "claim_token", "processing_version")
    if any(batch[field] is None or not str(batch[field]).strip() for field in fields):
        raise ValueError("Batch is missing required claim metadata")
    return {field: str(batch[field]) for field in fields}


def _dependencies_completed(
    connection: Connection, table: Table, manifest: Mapping[str, Any]
) -> bool:
    """Require all declared dependency batches to have completed processing."""
    dependencies = manifest.get("dependencies", [])
    if not isinstance(dependencies, list):
        raise ValueError("Batch dependencies must be a list of UUID strings")
    try:
        if any(not isinstance(value, str) for value in dependencies):
            raise ValueError("Invalid dependency type")
        identifiers = {PythonUUID(value) for value in dependencies}
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Batch dependencies must be a list of UUID strings") from None
    if not identifiers:
        return True
    completed = set(
        connection.execute(
            select(table.c.batch_id).where(
                table.c.batch_id.in_(identifiers), table.c.status == "completed"
            )
        ).scalars()
    )
    return completed == identifiers


def claim_next_ready_batch(airflow_run_id: str, lease_minutes: int = 60) -> dict[str, str] | None:
    """Atomically claim the oldest eligible ready batch or reuse an active claim.

    Read connection settings from the environment at invocation. Serialize calls
    for the same Airflow run using a transaction-scoped advisory lock, then use
    row locks to reuse its existing claim or select ready work with SKIP LOCKED.
    Both source branches must be loaded and declared dependencies completed.
    An expired claim requires separate recovery and is never reclaimed here.
    No manifest, source record, or S3 object is changed or returned.

    Raises:
        ValueError: For invalid inputs/configuration, invalid ready manifests,
            ambiguous ownership, or an expired claim owned by this run.
        RuntimeError: For sanitized database failures; the claim is rolled back.
    """
    if not isinstance(airflow_run_id, str) or not airflow_run_id.strip():
        raise ValueError("airflow_run_id must be a nonempty string")
    if type(lease_minutes) is not int or lease_minutes <= 0:
        raise ValueError("lease_minutes must be a positive integer")
    try:
        lease_duration = timedelta(minutes=lease_minutes)
        datetime.now(UTC) + lease_duration
    except OverflowError:
        raise ValueError("lease_minutes exceeds the supported timestamp range") from None
    url = _connection_url()
    table = _batch_table()
    # A stable signed bigint avoids Python's randomized hash and exposes no run text in SQL.
    lock_key = int.from_bytes(
        hashlib.sha256(f"cx-batch-claim:{airflow_run_id}".encode()).digest()[:8],
        byteorder="big",
        signed=True,
    )
    result: dict[str, str] | None = None
    reused = False
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
                connection.execute(select(func.pg_advisory_xact_lock(lock_key)))
                owned = (
                    connection.execute(
                        select(table)
                        .where(
                            table.c.status == "processing", table.c.airflow_run_id == airflow_run_id
                        )
                        .order_by(table.c.batch_id)
                        .with_for_update()
                    )
                    .mappings()
                    .all()
                )
                now = datetime.now(UTC)
                if len(owned) > 1:
                    raise ValueError("Airflow run owns multiple processing batches")
                if owned:
                    expiry = owned[0]["lease_expires_at"]
                    if expiry is None or expiry <= now:
                        raise ValueError(
                            "Existing processing claim requires expired-lease recovery"
                        )
                    result = _claim_metadata(owned[0])
                    reused = True
                else:
                    skipped: list[PythonUUID] = []
                    while True:
                        candidate = (
                            connection.execute(
                                select(table)
                                .where(table.c.status == "ready", table.c.batch_id.not_in(skipped))
                                .order_by(table.c.ready_at, table.c.created_at, table.c.batch_id)
                                .limit(1)
                                .with_for_update(skip_locked=True)
                            )
                            .mappings()
                            .one_or_none()
                        )
                        if candidate is None:
                            break
                        manifest = candidate["source_manifest"]
                        if not isinstance(manifest, dict) or any(
                            not isinstance(manifest.get(branch), dict)
                            or manifest[branch].get("status") != "loaded"
                            for branch in ("postgresql", "seaweedfs")
                        ):
                            raise ValueError(
                                "Ready batch must confirm both source branches are loaded"
                            )
                        if not _dependencies_completed(connection, table, manifest):
                            skipped.append(candidate["batch_id"])
                            continue
                        now = datetime.now(UTC)
                        token = uuid4()
                        claimed = (
                            connection.execute(
                                table.update()
                                .where(
                                    table.c.batch_id == candidate["batch_id"],
                                    table.c.status == "ready",
                                )
                                .values(
                                    status="processing",
                                    started_at=now,
                                    claim_token=token,
                                    airflow_run_id=airflow_run_id,
                                    lease_expires_at=now + lease_duration,
                                    attempt_count=table.c.attempt_count + 1,
                                )
                                .returning(
                                    table.c.batch_id,
                                    table.c.producer_key,
                                    table.c.claim_token,
                                    table.c.processing_version,
                                )
                            )
                            .mappings()
                            .one()
                        )
                        result = _claim_metadata(claimed)
                        break
    except SQLAlchemyError:
        raise RuntimeError("PostgreSQL batch claim failed; transaction rolled back") from None
    if result is None:
        logger.info("batch_claim=no_eligible_ready_batch")
    else:
        logger.info("batch_status=processing claim_reused=%s", reused)
    return result

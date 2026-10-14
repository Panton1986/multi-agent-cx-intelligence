"""Manual batch ETL from parallel PostgreSQL and SeaweedFS primary sources."""

import logging
from datetime import UTC, datetime
from uuid import UUID as PythonUUID

from airflow.sdk import dag, get_current_context, task

logger = logging.getLogger(__name__)


def _read_batch_kind(batch_id: str, claim_token: str) -> str:
    """Read the actual batch kind under current ownership without changing metadata."""
    from contextlib import ExitStack

    from sqlalchemy import Column, DateTime, MetaData, Table, Text, create_engine, select
    from sqlalchemy.dialects.postgresql import UUID
    from sqlalchemy.exc import SQLAlchemyError

    from cx_multiagent.database.claim_batch import _connection_url

    try:
        identifier = PythonUUID(batch_id)
        token = PythonUUID(claim_token)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Claim metadata must contain valid batch and claim UUID strings") from None
    batches = Table(
        "ingestion_batches",
        MetaData(),
        Column("batch_id", UUID(as_uuid=True), primary_key=True),
        Column("batch_kind", Text()),
        Column("status", Text()),
        Column("claim_token", UUID(as_uuid=True)),
        Column("lease_expires_at", DateTime(timezone=True)),
        schema="pipeline",
    )
    try:
        with ExitStack() as resources:
            engine = create_engine(
                _connection_url(),
                echo=False,
                hide_parameters=True,
                connect_args={"connect_timeout": 10},
            )
            resources.callback(engine.dispose)
            with engine.connect() as connection:
                connection = connection.execution_options(postgresql_readonly=True)
                with connection.begin():
                    batch = (
                        connection.execute(select(batches).where(batches.c.batch_id == identifier))
                        .mappings()
                        .one_or_none()
                    )
                    if batch is None:
                        raise ValueError("Claimed batch does not exist")
                    if batch["status"] != "processing" or batch["claim_token"] != token:
                        raise ValueError("Batch does not have the supplied active ownership")
                    expiry = batch["lease_expires_at"]
                    if (
                        not isinstance(expiry, datetime)
                        or expiry.tzinfo is None
                        or expiry.utcoffset() is None
                        or expiry <= datetime.now(UTC)
                    ):
                        raise ValueError("Batch processing lease has expired or is invalid")
                    kind = batch["batch_kind"]
                    if kind not in {"initial", "incremental"}:
                        raise ValueError("Unsupported batch kind in PostgreSQL metadata")
                    return kind
    except SQLAlchemyError:
        raise RuntimeError("Unable to verify batch kind; database details omitted") from None


@task(retries=0, multiple_outputs=False, show_return_value_in_logs=False)
def claim_batch() -> dict[str, str] | None:
    """Claim one ready batch using a stable identifier for this Airflow DAG run."""
    from cx_multiagent.database.claim_batch import claim_next_ready_batch

    context = get_current_context()
    airflow_run_id = f"{context['dag'].dag_id}:{context['run_id']}"
    logger.info("DAG run started")
    claim = claim_next_ready_batch(airflow_run_id=airflow_run_id, lease_minutes=60)
    logger.info("Batch claimed" if claim is not None else "No READY batch")
    return claim


@task(retries=0, multiple_outputs=False, show_return_value_in_logs=False)
def process_batch(claim: dict[str, str] | None) -> dict[str, str]:
    """Process both sources in one task; return completion metadata only.

    Incremental processing is blocked at validation until historical-reference
    lookup exists. The in-memory validator's partial incremental checks must not
    be mistaken for full historical validation. No extracted or masked dataset,
    JSONL payload, or output manifest is returned through TaskFlow XCom.
    """
    if claim is None:
        logger.info("No batch to process")
        return {"status": "no_work"}
    if not isinstance(claim, dict) or any(
        not isinstance(claim.get(field), str) or not claim[field].strip()
        for field in ("batch_id", "claim_token")
    ):
        raise ValueError("Invalid lightweight batch claim metadata")
    batch_id = claim["batch_id"]
    claim_token = claim["claim_token"]
    stage = "extraction"
    try:
        # Import application modules only during task execution, never during DAG parsing.
        from cx_multiagent.database.complete_batch import complete_batch
        from cx_multiagent.ingestion.extract_postgres import extract_postgres_batch
        from cx_multiagent.ingestion.extract_seaweedfs import extract_seaweedfs_batch
        from cx_multiagent.preprocessing.export_curated_batch import export_curated_batch
        from cx_multiagent.preprocessing.mask_batch import mask_batch_records
        from cx_multiagent.preprocessing.validate_batch import validate_batch_records

        logger.info("Processing stage=%s", stage)
        batch_kind = _read_batch_kind(batch_id, claim_token)
        postgres_records = extract_postgres_batch(batch_id, claim_token)
        seaweedfs_records = extract_seaweedfs_batch(batch_id, claim_token)

        stage = "validation"
        logger.info("Processing stage=%s", stage)
        if batch_kind == "incremental":
            raise ValueError("Incremental processing requires historical relationship validation")
        validate_batch_records(postgres_records, seaweedfs_records, batch_kind=batch_kind)

        stage = "masking"
        logger.info("Processing stage=%s", stage)
        masked_records = mask_batch_records(postgres_records, seaweedfs_records)

        stage = "publication"
        logger.info("Processing stage=%s", stage)
        output_manifest = export_curated_batch(batch_id, claim_token, masked_records)

        stage = "completion"
        logger.info("Processing stage=%s", stage)
        completion = complete_batch(batch_id, claim_token, output_manifest)
        logger.info("Batch completed")
        return completion
    except Exception:
        logger.error("Batch processing failed at stage=%s", stage)
        try:
            from cx_multiagent.database.fail_batch import fail_batch

            fail_batch(batch_id, claim_token, failed_stage=stage)
        except Exception:
            # Do not log raw exceptions or replace the original processing failure.
            logger.warning("Batch failure state could not be recorded; explicit recovery required")
        raise


@dag(
    dag_id="cx_batch_pipeline",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    doc_md=(
        "Manually triggered parallel-source batch ETL. Only lightweight claim and completion "
        "metadata enters XCom. Incremental batches are rejected at validation until historical "
        "lookup is implemented; explicit recovery is then required. No automatic retry or lease "
        "renewal is configured. Runtime and end-to-end checks are required before production use."
    ),
)
def cx_batch_pipeline() -> None:
    """Define claim -> process dependencies without executing database or S3 work."""
    process_batch(claim_batch())


cx_batch_pipeline_dag = cx_batch_pipeline()

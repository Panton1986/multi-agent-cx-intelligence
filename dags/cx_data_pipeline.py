"""Manually triggered validation and upload of the initial synthetic CX datasets."""

import json
import logging
from pathlib import Path

from airflow.sdk import dag, task

from cx_multiagent.ingestion.upload_raw import upload_initial_datasets
from cx_multiagent.preprocessing.curate_raw import curate_initial_datasets
from cx_multiagent.preprocessing.validate_raw import validate_raw_datasets

INPUT_DIRECTORY = Path("/opt/airflow/data/generated")
EXPECTED_RECORD_COUNTS: dict[str, int] = {
    "customers": 500,
    "product_releases": 20,
    "customer_journey_events": 15000,
    "mobile_application_logs": 5000,
    "support_tickets": 1500,
    "call_transcripts": 500,
    "survey_responses": 1000,
}

logger = logging.getLogger(__name__)


@task(multiple_outputs=False)
def validate_initial_data() -> dict[str, int]:
    """Validate each JSONL input and return its verified record count."""
    record_counts: dict[str, int] = {}

    for dataset_name, expected_count in EXPECTED_RECORD_COUNTS.items():
        file_path = INPUT_DIRECTORY / f"{dataset_name}.jsonl"
        if not file_path.is_file():
            raise FileNotFoundError(f"Required JSONL file not found: {file_path}")

        record_count = 0
        with file_path.open("r", encoding="utf-8") as input_file:
            for line_number, line in enumerate(input_file, start=1):
                if not line.strip():
                    raise ValueError(f"Blank JSONL record in {file_path} at line {line_number}")
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"Invalid JSON in {file_path} at line {line_number}"
                    ) from error
                if not isinstance(record, dict):
                    raise ValueError(
                        f"Expected a JSON object in {file_path} at line {line_number}"
                    )
                record_count += 1

        if record_count == 0:
            raise ValueError(f"Required JSONL file is empty: {file_path}")
        if record_count != expected_count:
            raise ValueError(
                f"Incorrect record count for {dataset_name}: "
                f"expected {expected_count}, found {record_count}"
            )

        record_counts[dataset_name] = record_count
        logger.info("Validated %s: %d records", dataset_name, record_count)

    logger.info("Initial data validation completed for %d datasets", len(record_counts))
    return record_counts


@task(multiple_outputs=False, trigger_rule="all_success")
def upload_to_seaweedfs() -> dict[str, str]:
    """Upload validated initial datasets and return their destination S3 URIs."""
    uploaded_files = upload_initial_datasets()
    logger.info("Successfully uploaded %d files to SeaweedFS", len(uploaded_files))
    return uploaded_files


@task(multiple_outputs=False, trigger_rule="all_success")
def validate_raw_data() -> dict[str, int]:
    """Validate the uploaded S3 datasets and return their record counts."""
    record_counts = validate_raw_datasets()
    logger.info(
        "Validated %d S3 datasets containing %d total records",
        len(record_counts),
        sum(record_counts.values()),
    )
    return record_counts


@task(multiple_outputs=False)
def curate_raw_data() -> dict[str, int]:
    """Curate validated raw datasets and return their processed record counts."""
    record_counts = curate_initial_datasets()
    logger.info(
        "Curated %d datasets containing %d total records",
        len(record_counts),
        sum(record_counts.values()),
    )
    return record_counts


@dag(
    dag_id="cx_data_pipeline",
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def cx_data_pipeline() -> None:
    """Define the manually triggered initial data pipeline."""
    validation = validate_initial_data()
    upload = upload_to_seaweedfs()
    raw_validation = validate_raw_data()
    curation = curate_raw_data()
    validation >> upload >> raw_validation >> curation


cx_data_pipeline_dag = cx_data_pipeline()

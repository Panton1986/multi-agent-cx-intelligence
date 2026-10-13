"""Upload the initial synthetic JSONL datasets to SeaweedFS raw storage."""

import logging
from pathlib import Path

from cx_multiagent.storage.s3_client import create_s3_client

INPUT_DIRECTORY = Path("/opt/airflow/data/generated")
DESTINATION_BUCKET = "cx-raw"
OBJECT_PREFIX = "synthetic/initial/v1/"
DATASET_FILENAMES: tuple[str, ...] = (
    "customers.jsonl",
    "product_releases.jsonl",
    "customer_journey_events.jsonl",
    "mobile_application_logs.jsonl",
    "support_tickets.jsonl",
    "call_transcripts.jsonl",
    "survey_responses.jsonl",
)

logger = logging.getLogger(__name__)


def upload_initial_datasets() -> dict[str, str]:
    """Validate all input files, upload them, and return their destination S3 URIs.

    Stable object keys overwrite previous uploads on repeated execution. The
    destination bucket must already exist. Upload errors propagate to the caller;
    completed uploads remain available if a subsequent upload fails.

    Raises:
        FileNotFoundError: If a required input is missing or is not a regular file.
        ValueError: If a required input file is empty.
    """
    file_paths = {filename: INPUT_DIRECTORY / filename for filename in DATASET_FILENAMES}

    # Validate the complete input set before creating a client or uploading any file.
    for file_path in file_paths.values():
        if not file_path.is_file():
            raise FileNotFoundError(f"Required JSONL file not found: {file_path}")
        if file_path.stat().st_size == 0:
            raise ValueError(f"Required JSONL file is empty: {file_path}")

    uploaded_files: dict[str, str] = {}
    client = create_s3_client()
    try:
        for filename, file_path in file_paths.items():
            object_key = f"{OBJECT_PREFIX}{filename}"
            client.upload_file(str(file_path), DESTINATION_BUCKET, object_key)
            destination_uri = f"s3://{DESTINATION_BUCKET}/{object_key}"
            uploaded_files[filename] = destination_uri
            logger.info("Uploaded %s to %s", filename, destination_uri)
    finally:
        client.close()

    logger.info("Initial dataset upload completed: %d files", len(uploaded_files))
    return uploaded_files

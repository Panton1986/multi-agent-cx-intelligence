"""Curate initial S3 datasets with demonstration-level PII masking."""

import json
import logging
import re
from collections.abc import Mapping
from contextlib import closing
from io import BytesIO

from botocore.client import BaseClient

from cx_multiagent.preprocessing.pii_masking import (
    mask_call_transcript,
    mask_customer_record,
    mask_free_text_fields,
    mask_text,
)
from cx_multiagent.storage.s3_client import create_s3_client

RAW_BUCKET = "cx-raw"
CURATED_BUCKET = "cx-curated"
OBJECT_PREFIX = "synthetic/initial/v1/"
DATASETS: tuple[str, ...] = (
    "customers",
    "product_releases",
    "customer_journey_events",
    "mobile_application_logs",
    "support_tickets",
    "call_transcripts",
    "survey_responses",
)
FREE_TEXT_FIELDS: dict[str, tuple[str, ...]] = {
    "support_tickets": ("subject", "description", "resolution"),
    "survey_responses": ("comment",),
    "mobile_application_logs": ("message", "stack_trace"),
    "product_releases": ("release_notes",),
}
_UTC_TIMESTAMP_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z"
)
logger = logging.getLogger(__name__)


def _mask_nested_value(value: object) -> object:
    """Copy nested properties and mask strings, preserving IDs and timestamps."""
    if isinstance(value, str):
        return value if _UTC_TIMESTAMP_PATTERN.fullmatch(value) else mask_text(value)
    if isinstance(value, Mapping):
        return {
            key: item
            if key.endswith("_id") or key.endswith("_at") or key == "timestamp"
            else _mask_nested_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_mask_nested_value(item) for item in value]
    return value


def _curate_record(dataset: str, record: Mapping[str, object]) -> dict[str, object]:
    """Apply the appropriate masking strategy without changing the raw record."""
    if dataset == "customers":
        return mask_customer_record(record)
    if dataset == "call_transcripts":
        return mask_call_transcript(record)

    curated = mask_free_text_fields(record, FREE_TEXT_FIELDS.get(dataset, ()))
    if dataset == "product_releases" and record.get("known_issues") is not None:
        issues = record["known_issues"]
        if not isinstance(issues, list):
            raise ValueError("known_issues must be a list")
        curated["known_issues"] = [
            mask_text(issue) if isinstance(issue, str) else issue for issue in issues
        ]
    elif dataset == "customer_journey_events" and record.get("properties") is not None:
        properties = record["properties"]
        if not isinstance(properties, Mapping):
            raise ValueError("properties must be a JSON object")
        curated["properties"] = _mask_nested_value(properties)
    return curated


def _read_and_curate_dataset(client: BaseClient, dataset: str) -> tuple[bytes, int]:
    """Read one raw S3 object and build masked UTF-8 JSONL entirely in memory."""
    response = client.get_object(Bucket=RAW_BUCKET, Key=f"{OBJECT_PREFIX}{dataset}.jsonl")
    count = 0
    with closing(response["Body"]) as body, BytesIO() as output:
        for line_number, line in enumerate(body.iter_lines(), start=1):
            context = f"{dataset}, line {line_number}"
            try:
                record = json.loads(line.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as error:
                raise ValueError(f"{context}: invalid UTF-8 JSONL record") from error
            if not isinstance(record, dict):
                raise ValueError(f"{context}: each record must be a JSON object")
            try:
                curated = _curate_record(dataset, record)
                serialized = json.dumps(curated, ensure_ascii=False, allow_nan=False)
            except ValueError as error:
                raise ValueError(f"{context}: record could not be curated") from error
            output.write((serialized + "\n").encode("utf-8"))
            count += 1
        if count == 0:
            raise ValueError(f"{dataset}: raw dataset is empty")
        return output.getvalue(), count


def curate_initial_datasets() -> dict[str, int]:
    """Mask all seven raw S3 datasets and return their processed record counts.

    Read only from cx-raw and overwrite stable keys in cx-curated. Buckets must
    already exist. Process one dataset at a time in memory without local files.
    Response bodies and the client close on success or failure. Errors propagate;
    earlier completed curated datasets remain if a later dataset fails.

    This applies basic demonstration-level masking, not production anonymization.
    """
    record_counts: dict[str, int] = {}
    client = create_s3_client()
    try:
        for dataset in DATASETS:
            content, count = _read_and_curate_dataset(client, dataset)
            client.put_object(
                Bucket=CURATED_BUCKET,
                Key=f"{OBJECT_PREFIX}{dataset}.jsonl",
                Body=content,
                ContentType="application/x-ndjson; charset=utf-8",
            )
            record_counts[dataset] = count
            logger.info("Curated %s: %d records", dataset, count)
    finally:
        client.close()
    return record_counts

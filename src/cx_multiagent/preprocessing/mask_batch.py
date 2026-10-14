"""Apply baseline PII masking to in-memory parallel-source batch records."""

import logging
import re
from copy import deepcopy
from typing import Any

from cx_multiagent.preprocessing.pii_masking import (
    mask_call_transcript,
    mask_customer_record,
    mask_free_text_fields,
    mask_text,
)

logger = logging.getLogger(__name__)
type Record = dict[str, Any]

_POSTGRES_DATASETS = (
    "customers",
    "product_releases",
    "customer_journey_events",
    "mobile_application_logs",
    "survey_responses",
)
_SEAWEEDFS_DATASETS = ("support_tickets", "call_transcripts")
_FREE_TEXT_FIELDS = {
    "support_tickets": ("subject", "description", "resolution"),
    "survey_responses": ("comment",),
    "mobile_application_logs": ("message", "stack_trace"),
    "product_releases": ("release_notes",),
}
_UTC_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z")


def _mask_nested(value: Any) -> Any:
    """Mask nested string values without granting exemptions based on key names.

    Recurse through every dictionary and list, including values under identifier,
    category, and timestamp-like keys. Valid project IDs and category codes do
    not match the PII patterns and remain unchanged. Whole UTC timestamp strings
    are preserved by value shape; datetime objects are returned without conversion.
    This operates on an independent deep copy and preserves dictionary keys,
    containers, numeric values, and nulls.
    """
    if isinstance(value, str):
        return value if _UTC_TIMESTAMP.fullmatch(value) else mask_text(value)
    if isinstance(value, dict):
        return {key: _mask_nested(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_mask_nested(item) for item in value]
    return value


def _mask_record(dataset: str, original: Record) -> Record:
    """Copy a record deeply before applying its existing dataset masking rules."""
    record = deepcopy(original)
    if dataset == "customers":
        return mask_customer_record(record)
    if dataset == "call_transcripts":
        return mask_call_transcript(record)
    masked = mask_free_text_fields(record, _FREE_TEXT_FIELDS.get(dataset, ()))
    if dataset == "product_releases" and record.get("known_issues") is not None:
        issues = record["known_issues"]
        if not isinstance(issues, list):
            raise ValueError("known_issues must be a list")
        # Keep existing issue codes unchanged; mask PII in descriptive issue strings.
        masked["known_issues"] = [
            mask_text(issue) if isinstance(issue, str) else issue for issue in issues
        ]
    if dataset == "customer_journey_events" and record.get("properties") is not None:
        properties = record["properties"]
        if not isinstance(properties, dict):
            raise ValueError("properties must be an object")
        masked["properties"] = _mask_nested(properties)
    return masked


def mask_batch_records(
    postgres_records: dict[str, list[dict[str, Any]]],
    seaweedfs_records: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Return independent masked copies of all seven parallel-source datasets.

    Require exactly the dataset names assigned to each primary source; empty lists
    are allowed. Reuse existing customer pseudonyms, free-text masking, and call
    transcript helpers. Mask release issue descriptions and nested journey text,
    while preserving schema identifiers, timestamps, categories, and numeric values.
    Nested properties have no key-based exemptions: all nested string values are
    considered for masking except whole UTC timestamp strings. Valid project IDs
    and category codes remain unchanged; PII-bearing strings under misleading
    identifier/category keys are masked. Validate contracts before this stage.
    Aware PostgreSQL datetimes remain datetime objects for future serialization.

    Customer pseudonyms depend only on customer_id, and redaction placeholders
    remain unchanged on repeated masking. Inputs and their nested containers are
    never modified or shared with the output. This performs no cross-dataset
    validation, storage I/O, or Airflow operations. Consume records within the same
    processing task; do not pass the datasets through Airflow XCom.

    Regex masking is baseline protection and cannot guarantee removal of every
    possible PII format. Validate record contracts separately with validate_batch.

    Raises:
        ValueError: If dataset mappings, record lists, or structures needed by
            the masking helpers are invalid. Errors never include record content.
    """
    for source, supplied, expected in (
        ("PostgreSQL", postgres_records, _POSTGRES_DATASETS),
        ("SeaweedFS", seaweedfs_records, _SEAWEEDFS_DATASETS),
    ):
        if not isinstance(supplied, dict) or set(supplied) != set(expected):
            raise ValueError(f"{source}: missing or unexpected dataset names")
        for dataset in expected:
            if not isinstance(supplied[dataset], list):
                raise ValueError(f"{dataset}: records must be a list")
            if any(not isinstance(record, dict) for record in supplied[dataset]):
                raise ValueError(f"{dataset}: each record must be a dictionary")
    combined = {**postgres_records, **seaweedfs_records}
    masked: dict[str, list[Record]] = {}
    for dataset in (*_POSTGRES_DATASETS, *_SEAWEEDFS_DATASETS):
        records: list[Record] = []
        for position, record in enumerate(combined[dataset], 1):
            try:
                records.append(_mask_record(dataset, record))
            except (ValueError, TypeError, OverflowError, RecursionError):
                raise ValueError(
                    f"{dataset}, record {position}: invalid masking structure"
                ) from None
        masked[dataset] = records
    for dataset, records in masked.items():
        logger.info("dataset=%s records=%d", dataset, len(records))
    return masked

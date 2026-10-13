"""Validate initial synthetic JSONL records and relationships directly from S3."""

import json
import logging
import math
from contextlib import closing
from typing import Any, Never

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from cx_multiagent.storage.s3_client import create_s3_client

type Record = dict[str, Any]
type DatasetIndex = dict[str, dict[str, Record]]

BUCKET = "cx-raw"
PREFIX = "synthetic/initial/v1/"
PRIMARY_IDS: dict[str, str] = {
    "customers": "customer_id",
    "product_releases": "release_id",
    "customer_journey_events": "event_id",
    "mobile_application_logs": "log_id",
    "support_tickets": "ticket_id",
    "call_transcripts": "call_id",
    "survey_responses": "survey_id",
}
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "customers": (
        "customer_id",
        "full_name",
        "email",
        "created_at",
        "country_code",
        "preferred_language",
        "customer_segment",
        "account_status",
    ),
    "product_releases": (
        "release_id",
        "version",
        "platform",
        "released_at",
        "release_notes",
        "known_issues",
        "rollout_percentage",
    ),
    "customer_journey_events": (
        "event_id",
        "customer_id",
        "occurred_at",
        "session_id",
        "device_id",
        "channel",
        "event_type",
        "outcome",
    ),
    "mobile_application_logs": (
        "log_id",
        "customer_id",
        "occurred_at",
        "session_id",
        "device_id",
        "release_id",
        "platform",
        "os_version",
        "log_level",
        "log_type",
        "message",
    ),
    "support_tickets": (
        "ticket_id",
        "customer_id",
        "opened_at",
        "channel",
        "issue_type",
        "subject",
        "description",
        "status",
        "priority",
        "escalation_level",
    ),
    "call_transcripts": (
        "call_id",
        "customer_id",
        "started_at",
        "ended_at",
        "duration_seconds",
        "issue_type",
        "outcome",
        "transcript",
    ),
    "survey_responses": (
        "survey_id",
        "customer_id",
        "submitted_at",
        "survey_type",
        "csat_score",
    ),
}
OPTIONAL_FIELDS = {
    "comment",
    "ticket_id",
    "call_id",
    "event_id",
    "session_id",
    "device_id",
    "closed_at",
    "resolution",
    "release_id",
    "error_code",
    "properties",
    "stack_trace",
}
FIELD_TYPES: dict[str, type] = {
    "csat_score": int,
    "escalation_level": int,
    "duration_seconds": int,
    "rollout_percentage": int,
    "known_issues": list,
    "transcript": list,
    "properties": dict,
}
FOREIGN_KEYS = {
    "customer_id": "customers",
    "release_id": "product_releases",
    "ticket_id": "support_tickets",
    "call_id": "call_transcripts",
    "event_id": "customer_journey_events",
}
FAILURE_LOG_TYPES = {"payment_failure", "app_crash", "login_problem"}
logger = logging.getLogger(__name__)


def _reject_json_constant(value: str) -> Never:
    """Reject nonstandard JSON numeric constants such as NaN and Infinity."""
    raise ValueError(f"Invalid JSON numeric constant: {value}")


def _read_dataset(client: BaseClient, dataset: str) -> list[Record]:
    """Read UTF-8 JSON objects, closing the S3 body even when parsing fails."""
    key = f"{PREFIX}{dataset}.jsonl"
    try:
        response = client.get_object(Bucket=BUCKET, Key=key)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "NoSuchBucket", "404"}:
            raise ValueError(
                f"{dataset}: required S3 object is missing: s3://{BUCKET}/{key}"
            ) from error
        raise

    records: list[Record] = []
    with closing(response["Body"]) as body:
        for line_number, line in enumerate(body.iter_lines(), start=1):
            context = f"{dataset}, line {line_number}"
            try:
                record = json.loads(line.decode("utf-8"), parse_constant=_reject_json_constant)
            except (ValueError, UnicodeDecodeError) as error:
                raise ValueError(f"{context}: invalid UTF-8 JSONL record") from error
            if not isinstance(record, dict):
                raise ValueError(f"{context}: each JSONL record must be a JSON object")
            records.append(record)
    if not records:
        raise ValueError(f"{dataset}: S3 dataset is empty")
    return records


def _require_fields(record: Record, fields: tuple[str, ...], context: str) -> None:
    """Require the named fields to be present and non-null."""
    missing = [field for field in fields if record.get(field) is None]
    if missing:
        raise ValueError(f"{context}: missing or null required fields: {', '.join(missing)}")


def _validate_transcript(record: Record, context: str) -> None:
    """Validate required fields and ordered offsets within conversation turns."""
    if record["duration_seconds"] <= 0 or not record["transcript"]:
        raise ValueError(f"{context}: call duration and transcript must be nonempty and positive")
    previous_offset = 0.0
    for position, turn in enumerate(record["transcript"], start=1):
        turn_context = f"{context}, transcript turn {position}"
        if not isinstance(turn, dict):
            raise ValueError(f"{turn_context}: turn must be an object")
        _require_fields(turn, ("speaker", "text", "offset_seconds"), turn_context)
        if turn["speaker"] not in ("customer", "agent") or not isinstance(turn["text"], str):
            raise ValueError(f"{turn_context}: invalid speaker or text")
        offset = turn["offset_seconds"]
        if (
            type(offset) not in (int, float)
            or not math.isfinite(offset)
            or not previous_offset <= offset <= record["duration_seconds"]
        ):
            raise ValueError(f"{turn_context}: offset must be ordered and within call duration")
        previous_offset = offset


def _validate_fields(dataset: str, record: Record, context: str) -> None:
    """Check schema fields, their JSON types, and conditional requirements."""
    _require_fields(record, REQUIRED_FIELDS[dataset], context)
    for field in set(REQUIRED_FIELDS[dataset]) | OPTIONAL_FIELDS:
        value = record.get(field)
        if value is None:
            continue
        expected_type = FIELD_TYPES.get(field, str)
        if type(value) is not expected_type:
            raise ValueError(f"{context}: {field} must have type {expected_type.__name__}")
        if field.endswith("_id") and not value.strip():
            raise ValueError(f"{context}: {field} must be a nonempty identifier")
    if record.get("session_id") is not None:
        _require_fields(record, ("device_id",), context)

    if dataset == "survey_responses":
        if not 1 <= record["csat_score"] <= 5:
            raise ValueError(f"{context}: csat_score must be an integer from 1 to 5")
        survey_type = record["survey_type"]
        if survey_type not in {"post_support", "post_journey", "general"}:
            raise ValueError(f"{context}: unsupported survey_type")
        if survey_type == "post_support" and all(
            record.get(field) is None for field in ("ticket_id", "call_id")
        ):
            raise ValueError(f"{context}: post_support requires ticket_id or call_id")
        if survey_type == "post_journey":
            _require_fields(record, ("event_id",), context)
    elif dataset == "support_tickets":
        if record["status"] in {"resolved", "closed"}:
            _require_fields(record, ("closed_at", "resolution"), context)
        if not 0 <= record["escalation_level"] <= 3:
            raise ValueError(f"{context}: escalation_level must be between 0 and 3")
        if record["status"] == "escalated" and record["escalation_level"] == 0:
            raise ValueError(f"{context}: escalated tickets require a positive escalation_level")
    elif dataset == "customer_journey_events":
        if record["outcome"] not in {"success", "failure"}:
            raise ValueError(f"{context}: event outcome must be success or failure")
        if record["channel"] not in {"web", "mobile"}:
            raise ValueError(f"{context}: event channel must be web or mobile")
        if record["channel"] == "mobile":
            _require_fields(record, ("release_id",), context)
        if record["outcome"] == "failure":
            _require_fields(record, ("error_code",), context)
        elif record.get("error_code") is not None:
            raise ValueError(f"{context}: successful events cannot have error_code")
        if record["event_type"] == "app_crash" and record["outcome"] != "failure":
            raise ValueError(f"{context}: app_crash must have failure outcome")
        if record["event_type"] == "journey_completed" and record["outcome"] != "success":
            raise ValueError(f"{context}: journey_completed must have success outcome")
    elif dataset == "mobile_application_logs":
        if record["platform"] not in {"android", "ios"}:
            raise ValueError(f"{context}: mobile platform must be android or ios")
        if record["log_type"] in FAILURE_LOG_TYPES:
            _require_fields(record, ("error_code",), context)
    elif dataset == "call_transcripts":
        _validate_transcript(record, context)
    elif dataset == "product_releases":
        if not all(isinstance(issue, str) for issue in record["known_issues"]):
            raise ValueError(f"{context}: known_issues must contain strings")


def _index_dataset(dataset: str, records: list[Record]) -> dict[str, Record]:
    """Validate individual records and index unique primary identifiers."""
    primary_id = PRIMARY_IDS[dataset]
    indexed: dict[str, Record] = {}
    for position, record in enumerate(records, start=1):
        context = f"{dataset}, line {position}"
        _validate_fields(dataset, record, context)
        identifier = record[primary_id]
        if identifier in indexed:
            raise ValueError(f"{context}: duplicate {primary_id}: {identifier}")
        indexed[identifier] = record
    return indexed


def _validate_relationships(dataset: str, record: Record, indexes: DatasetIndex) -> None:
    """Resolve references and check customer, session, device, and release agreement."""
    context = f"{dataset}, {PRIMARY_IDS[dataset]}={record[PRIMARY_IDS[dataset]]}"
    for field, target_dataset in FOREIGN_KEYS.items():
        value = record.get(field)
        if value is None or field == PRIMARY_IDS[dataset]:
            continue
        target = indexes[target_dataset].get(value)
        if target is None:
            raise ValueError(f"{context}: orphan {field}: {value}")
        if target.get("customer_id") is not None and (
            target["customer_id"] != record.get("customer_id")
        ):
            raise ValueError(f"{context}: {field} references a different customer")
        for correlation in ("session_id", "device_id", "release_id"):
            if (
                record.get(correlation) is not None
                and target.get(correlation) is not None
                and record[correlation] != target[correlation]
            ):
                raise ValueError(f"{context}: {correlation} conflicts with linked {field}")

    release_id = record.get("release_id")
    if release_id is not None:
        platform = indexes["product_releases"][release_id]["platform"]
        if dataset == "customer_journey_events":
            allowed = {"android", "ios"} if record["channel"] == "mobile" else {"web"}
            if platform not in allowed:
                raise ValueError(f"{context}: release platform conflicts with event channel")
        elif dataset == "mobile_application_logs" and record["platform"] != platform:
            raise ValueError(f"{context}: log platform conflicts with release platform")
    if dataset == "mobile_application_logs" and record.get("event_id") is not None:
        event = indexes["customer_journey_events"][record["event_id"]]
        if event["channel"] != "mobile":
            raise ValueError(f"{context}: mobile log references a web event")


def _validate_session(record: Record, sessions: dict[str, Record], context: str) -> None:
    """Track one customer, device, and supplied release per session across datasets."""
    session_id = record.get("session_id")
    if session_id is None:
        return
    owner = sessions.setdefault(session_id, {})
    for field in ("customer_id", "device_id", "release_id"):
        value = record.get(field)
        if value is None:
            continue
        if field in owner and owner[field] != value:
            raise ValueError(f"{context}: session {session_id} has conflicting {field}")
        owner[field] = value


def validate_raw_datasets() -> dict[str, int]:
    """Read and validate all seven raw S3 datasets, returning their record counts.

    No local data files are read and no expected counts are imposed. Validation
    checks JSON objects, schema fields, unique IDs, foreign keys, customer/session
    consistency, CSAT, and event outcomes. Response bodies and the client close
    even on failure. S3 transport and authorization errors propagate unchanged.

    Raises:
        ValueError: If an object is missing, empty, malformed, or fails validation.
    """
    datasets: dict[str, list[Record]] = {}
    current_dataset = "customers"
    client = create_s3_client()
    try:
        for current_dataset in PRIMARY_IDS:
            datasets[current_dataset] = _read_dataset(client, current_dataset)
            logger.info(
                "Dataset %s: %d records; validation pending",
                current_dataset,
                len(datasets[current_dataset]),
            )
        indexes: DatasetIndex = {}
        for current_dataset, records in datasets.items():
            indexes[current_dataset] = _index_dataset(current_dataset, records)
        sessions: dict[str, Record] = {}
        for current_dataset, records in datasets.items():
            for position, record in enumerate(records, start=1):
                _validate_relationships(current_dataset, record, indexes)
                _validate_session(record, sessions, f"{current_dataset}, line {position}")
    except ValueError as error:
        logger.error(
            "Dataset %s: validation failed (%d loaded records): %s",
            current_dataset,
            len(datasets.get(current_dataset, [])),
            error,
        )
        raise
    finally:
        client.close()

    record_counts = {dataset: len(records) for dataset, records in datasets.items()}
    for dataset, count in record_counts.items():
        logger.info("Dataset %s: %d records; validation passed", dataset, count)
    return record_counts

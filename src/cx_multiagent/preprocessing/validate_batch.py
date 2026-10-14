"""Validate in-memory records from parallel PostgreSQL and SeaweedFS sources."""

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

from cx_multiagent.preprocessing.validate_raw import (
    FOREIGN_KEYS,
    PRIMARY_IDS,
    REQUIRED_FIELDS,
    _validate_fields,
    _validate_session,
)

logger = logging.getLogger(__name__)
type Record = dict[str, Any]
type Indexes = dict[str, dict[str, Record]]

_POSTGRES = {
    "customers",
    "product_releases",
    "customer_journey_events",
    "mobile_application_logs",
    "survey_responses",
}
_SEAWEEDFS = {"support_tickets", "call_transcripts"}
_OPTIONAL = {
    "customers": (),
    "product_releases": (),
    "customer_journey_events": ("error_code", "release_id", "ticket_id", "properties"),
    "mobile_application_logs": ("event_id", "error_code", "stack_trace"),
    "survey_responses": ("comment", "ticket_id", "call_id", "event_id", "session_id", "device_id"),
    "support_tickets": ("closed_at", "resolution", "session_id", "device_id", "release_id"),
    "call_transcripts": ("ticket_id", "session_id", "device_id"),
}
_TIMESTAMPS = {
    "customers": ("created_at",),
    "product_releases": ("released_at",),
    "customer_journey_events": ("occurred_at",),
    "mobile_application_logs": ("occurred_at",),
    "survey_responses": ("submitted_at",),
    "support_tickets": ("opened_at", "closed_at"),
    "call_transcripts": ("started_at", "ended_at"),
}
_ENUMS = {
    "customers": {
        "customer_segment": {"new", "regular", "premium"},
        "account_status": {"active", "inactive", "closed"},
    },
    "product_releases": {"platform": {"android", "ios", "web"}},
    "customer_journey_events": {
        "channel": {"web", "mobile"},
        "outcome": {"success", "failure"},
        "event_type": {
            "session_started",
            "login_attempt",
            "product_view",
            "checkout_started",
            "payment_attempt",
            "app_crash",
            "support_contact",
            "journey_completed",
        },
    },
    "mobile_application_logs": {
        "platform": {"android", "ios"},
        "log_level": {"DEBUG", "INFO", "WARNING", "ERROR"},
        "log_type": {"payment_failure", "app_crash", "login_problem", "activity"},
    },
    "support_tickets": {
        "channel": {"email", "chat", "phone", "in_app"},
        "issue_type": {"payment_failure", "app_crash", "login_problem", "other"},
        "status": {"open", "in_progress", "escalated", "resolved", "closed"},
        "priority": {"low", "medium", "high", "urgent"},
    },
    "call_transcripts": {
        "issue_type": {"payment_failure", "app_crash", "login_problem", "other"},
        "outcome": {"resolved", "follow_up", "escalated"},
    },
    "survey_responses": {"survey_type": {"post_support", "post_journey", "general"}},
}
_PREFIXES = {
    "customer_id": "cus_",
    "release_id": "rel_",
    "event_id": "evt_",
    "log_id": "log_",
    "survey_id": "sur_",
    "ticket_id": "tkt_",
    "call_id": "cal_",
    "session_id": "ses_",
    "device_id": "dev_",
}
_SEMVER = re.compile(
    r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)"
    r"(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
)


def _timestamp(value: Any, context: str, field: str) -> datetime:
    """Accept aware SQL datetimes or ISO 8601 UTC JSON strings ending in Z."""
    try:
        if isinstance(value, str):
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", value) is None:
                raise ValueError("Invalid UTC timestamp format")
            value = datetime.fromisoformat(value)
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Timestamp must be timezone-aware")
        return value.astimezone(UTC)
    except (ValueError, TypeError, OverflowError):
        raise ValueError(f"{context}: invalid {field} timestamp") from None


def _record(dataset: str, original: Any, context: str) -> Record:
    """Validate a record using raw business rules and a private timestamp-normalized copy."""
    if not isinstance(original, dict):
        raise ValueError(f"{context}: record must be an object")
    allowed = set(REQUIRED_FIELDS[dataset]) | set(_OPTIONAL[dataset])
    if set(original) - allowed:
        raise ValueError(f"{context}: unexpected record fields")
    record = dict(original)
    times: dict[str, datetime] = {}
    for field in _TIMESTAMPS[dataset]:
        if record.get(field) is not None:
            times[field] = _timestamp(record[field], context, field)
            record[field] = times[field].isoformat().replace("+00:00", "Z")
    try:
        _validate_fields(dataset, record, context)
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(
            f"{context}: required fields, field types, or business rules failed"
        ) from None
    for field, values in _ENUMS[dataset].items():
        if record[field] not in values:
            raise ValueError(f"{context}: unsupported {field}")
    for field, prefix in _PREFIXES.items():
        value = record.get(field)
        if value is not None and (not value.startswith(prefix) or not value[len(prefix) :].strip()):
            raise ValueError(f"{context}: invalid {field} prefix or identifier")
    if record.get("error_code") is not None and not record["error_code"].strip():
        raise ValueError(f"{context}: error_code must be nonempty when supplied")
    for field in ("known_issues", "properties", "transcript"):
        if record.get(field) is not None:
            try:
                json.dumps(record[field], allow_nan=False)
            except (ValueError, TypeError, OverflowError, RecursionError):
                raise ValueError(f"{context}: {field} must contain finite JSON values") from None
    if dataset == "customers":
        if re.fullmatch(r"[A-Z]{2}", record["country_code"]) is None:
            raise ValueError(f"{context}: invalid country_code")
        if not record["full_name"].strip() or not record["preferred_language"].strip():
            raise ValueError(f"{context}: name and language must be nonempty")
        if re.fullmatch(r"[^@\s]+@example\.com", record["email"]) is None:
            raise ValueError(f"{context}: invalid synthetic email address")
    elif dataset == "product_releases":
        if not 1 <= record["rollout_percentage"] <= 100:
            raise ValueError(f"{context}: rollout_percentage must be between 1 and 100")
        if _SEMVER.fullmatch(record["version"]) is None:
            raise ValueError(f"{context}: invalid semantic version")
    elif dataset == "support_tickets" and "closed_at" in times:
        if times["closed_at"] <= times["opened_at"]:
            raise ValueError(f"{context}: closure must follow opening")
    elif dataset == "call_transcripts":
        elapsed = (times["ended_at"] - times["started_at"]).total_seconds()
        if elapsed <= 0 or abs(elapsed - record["duration_seconds"]) > 0.5:
            raise ValueError(f"{context}: call timestamps and duration disagree")
    record.update(times)
    return record


def _relationships(
    dataset: str, record: Record, indexes: Indexes, initial: bool, context: str
) -> bool:
    """Validate available references; report references requiring historical lookup."""
    deferred = False
    for field, target_dataset in FOREIGN_KEYS.items():
        value = record.get(field)
        if value is None or field == PRIMARY_IDS[dataset]:
            continue
        target = indexes[target_dataset].get(value)
        if target is None:
            if initial:
                raise ValueError(f"{context}: unresolved {field} reference")
            deferred = True
            continue
        if target.get("customer_id") is not None and target["customer_id"] != record.get(
            "customer_id"
        ):
            raise ValueError(f"{context}: {field} references a different customer")
        for correlation in ("session_id", "device_id", "release_id"):
            if (
                record.get(correlation) is not None
                and target.get(correlation) is not None
                and record[correlation] != target[correlation]
            ):
                raise ValueError(f"{context}: {correlation} conflicts with linked {field}")
    activity_field = {
        "customer_journey_events": "occurred_at",
        "mobile_application_logs": "occurred_at",
        "support_tickets": "opened_at",
        "call_transcripts": "started_at",
        "survey_responses": "submitted_at",
    }.get(dataset)
    customer = indexes["customers"].get(record.get("customer_id"))
    if activity_field and customer and record[activity_field] < customer["created_at"]:
        raise ValueError(f"{context}: activity precedes account creation")
    release = indexes["product_releases"].get(record.get("release_id"))
    if release:
        if activity_field and record[activity_field] < release["released_at"]:
            raise ValueError(f"{context}: activity precedes release availability")
        if dataset == "customer_journey_events":
            platforms = {"android", "ios"} if record["channel"] == "mobile" else {"web"}
            if release["platform"] not in platforms:
                raise ValueError(f"{context}: release platform conflicts with channel")
        if dataset == "mobile_application_logs" and record["platform"] != release["platform"]:
            raise ValueError(f"{context}: release platform conflicts with mobile log")
    if dataset == "mobile_application_logs":
        event = indexes["customer_journey_events"].get(record.get("event_id"))
        if event and (event["channel"] != "mobile" or record["occurred_at"] < event["occurred_at"]):
            raise ValueError(f"{context}: log must follow a linked mobile event")
    if dataset == "call_transcripts":
        ticket = indexes["support_tickets"].get(record.get("ticket_id"))
        if ticket:
            outcomes = {
                "resolved": "resolved",
                "closed": "resolved",
                "escalated": "escalated",
                "open": "follow_up",
                "in_progress": "follow_up",
            }
            if (
                record["outcome"] != outcomes[ticket["status"]]
                or record["issue_type"] != ticket["issue_type"]
            ):
                raise ValueError(f"{context}: call issue or outcome conflicts with ticket")
            if record["started_at"] < ticket["opened_at"]:
                raise ValueError(f"{context}: call precedes ticket opening")
            if (
                ticket["status"] in {"resolved", "closed"}
                and record["ended_at"] > ticket["closed_at"]
            ):
                raise ValueError(f"{context}: call ends after linked ticket closure")
    if dataset == "survey_responses":
        for field, target_dataset, timestamp in (
            ("event_id", "customer_journey_events", "occurred_at"),
            ("call_id", "call_transcripts", "ended_at"),
            ("ticket_id", "support_tickets", "opened_at"),
        ):
            target = indexes[target_dataset].get(record.get(field))
            if target:
                earliest = target[timestamp]
                if target_dataset == "support_tickets" and target["status"] in {
                    "resolved",
                    "closed",
                }:
                    earliest = target["closed_at"]
                if record["submitted_at"] < earliest:
                    raise ValueError(f"{context}: survey precedes linked activity or resolution")
        call = indexes["call_transcripts"].get(record.get("call_id"))
        ticket = indexes["support_tickets"].get(call.get("ticket_id")) if call else None
        if ticket and ticket["status"] in {"resolved", "closed"}:
            if record["submitted_at"] < ticket["closed_at"]:
                raise ValueError(f"{context}: survey precedes linked call's ticket resolution")
    return deferred


def validate_batch_records(
    postgres_records: dict[str, list[dict[str, Any]]],
    seaweedfs_records: dict[str, list[dict[str, Any]]],
    batch_kind: str = "initial",
) -> dict[str, int]:
    """Validate seven extracted datasets without I/O, masking, or input mutation.

    Initial batches require every supplied reference to resolve in these records.
    Incremental batches check links when targets are supplied, but absent targets
    remain unverified: separate historical lookup is required before full pipeline
    acceptance. Uniqueness, session/device history, and chronology beyond supplied
    records also require that lookup. Logs mark affected datasets as having
    unverified historical references; returned counts do not certify those links.

    Supply all seven dataset names in their proper source mapping, using empty
    lists for datasets with no records. No initial record counts or fixed date
    cutoff are imposed. Consume records within the same future processing task;
    do not transfer the datasets through Airflow XCom.

    Raises:
        ValueError: If source mappings, record contracts, timestamps, identifiers,
            nested structures, or verifiable relationships are invalid.
    """
    if not isinstance(batch_kind, str) or batch_kind not in {"initial", "incremental"}:
        raise ValueError("batch_kind must be initial or incremental")
    for label, supplied, expected in (
        ("PostgreSQL", postgres_records, _POSTGRES),
        ("SeaweedFS", seaweedfs_records, _SEAWEEDFS),
    ):
        if not isinstance(supplied, dict) or set(supplied) != expected:
            raise ValueError(f"{label}: missing or unexpected dataset names")
    datasets = {**postgres_records, **seaweedfs_records}
    indexes: Indexes = {}
    unique_emails: set[str] = set()
    unique_versions: set[tuple[str, str]] = set()
    for dataset in PRIMARY_IDS:
        records = datasets[dataset]
        if not isinstance(records, list):
            raise ValueError(f"{dataset}: records must be a list")
        indexed: dict[str, Record] = {}
        for position, original in enumerate(records, 1):
            context = f"{dataset}, record {position}"
            record = _record(dataset, original, context)
            identifier = record[PRIMARY_IDS[dataset]]
            if identifier in indexed:
                raise ValueError(f"{context}: duplicate primary identifier")
            indexed[identifier] = record
            if dataset == "customers":
                if record["email"] in unique_emails:
                    raise ValueError(f"{context}: duplicate customer email")
                unique_emails.add(record["email"])
            if dataset == "product_releases":
                version = (record["platform"], record["version"])
                if version in unique_versions:
                    raise ValueError(f"{context}: duplicate version within platform")
                unique_versions.add(version)
        indexes[dataset] = indexed
    sessions: dict[str, Record] = {}
    devices: dict[str, str] = {}
    deferred: set[str] = set()
    for dataset, indexed in indexes.items():
        for position, record in enumerate(indexed.values(), 1):
            context = f"{dataset}, record {position}"
            if _relationships(dataset, record, indexes, batch_kind == "initial", context):
                deferred.add(dataset)
            try:
                _validate_session(record, sessions, context)
            except ValueError:
                raise ValueError(
                    f"{context}: inconsistent session ownership, device, or release"
                ) from None
            release = indexes["product_releases"].get(record.get("release_id"))
            platform = release["platform"] if release else record.get("platform")
            if dataset == "customer_journey_events" and record["channel"] == "web":
                platform = "web"
            device = record.get("device_id")
            if device is not None and platform is not None:
                if device in devices and devices[device] != platform:
                    raise ValueError(f"{context}: device has conflicting platforms")
                devices[device] = platform
    counts = {dataset: len(indexes[dataset]) for dataset in PRIMARY_IDS}
    for dataset, count in counts.items():
        result = "passed_with_unverified_historical_references" if dataset in deferred else "passed"
        logger.info("dataset=%s records=%d validation=%s", dataset, count, result)
    return counts

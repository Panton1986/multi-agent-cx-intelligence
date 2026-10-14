"""Build structured RAG text documents; privacy approval remains a separate stage."""

import math
import re
from copy import deepcopy
from datetime import datetime
from typing import Any

type Record = dict[str, Any]

_PRIMARY_KEYS = {
    "support_tickets": "ticket_id",
    "call_transcripts": "call_id",
    "survey_responses": "survey_id",
    "product_releases": "release_id",
}
_ID_PREFIXES = {
    "customer_id": "cus_",
    "ticket_id": "tkt_",
    "call_id": "cal_",
    "survey_id": "sur_",
    "release_id": "rel_",
    "event_id": "evt_",
    "session_id": "ses_",
    "device_id": "dev_",
}
_METADATA_FIELDS = {
    "support_tickets": (
        "customer_id",
        "issue_type",
        "status",
        "priority",
        "escalation_level",
        "channel",
        "opened_at",
        "closed_at",
        "session_id",
        "device_id",
        "release_id",
    ),
    "call_transcripts": (
        "customer_id",
        "ticket_id",
        "issue_type",
        "outcome",
        "started_at",
        "ended_at",
        "duration_seconds",
        "session_id",
        "device_id",
    ),
    "survey_responses": (
        "customer_id",
        "survey_type",
        "csat_score",
        "submitted_at",
        "ticket_id",
        "call_id",
        "event_id",
        "session_id",
        "device_id",
    ),
    "product_releases": ("version", "platform", "released_at", "rollout_percentage"),
}
_CATEGORIES = {
    "issue_type": {"payment_failure", "app_crash", "login_problem", "other"},
    "status": {"open", "in_progress", "escalated", "resolved", "closed"},
    "priority": {"low", "medium", "high", "urgent"},
    "channel": {"email", "chat", "phone", "in_app"},
    "outcome": {"resolved", "follow_up", "escalated"},
    "survey_type": {"post_support", "post_journey", "general"},
    "platform": {"android", "ios", "web"},
}
_INTEGER_RANGES = {
    "escalation_level": (0, 3),
    "csat_score": (1, 5),
    "rollout_percentage": (1, 100),
    "duration_seconds": (1, None),
}


def _identifier(value: Any, field: str, context: str) -> str:
    """Require an existing project ID without rewriting it or disclosing its value."""
    prefix = _ID_PREFIXES[field]
    if (
        not isinstance(value, str)
        or re.fullmatch(re.escape(prefix) + r"[A-Za-z0-9_-]+", value) is None
    ):
        raise ValueError(f"{context}: invalid {field}")
    return value


def _text(record: Record, field: str, context: str, *, optional: bool = False) -> str | None:
    """Read a string without trimming or rewriting punctuation and placeholders."""
    value = record.get(field)
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{context}: {field} must be a string")
    return value if value.strip() else None


def _metadata(dataset: str, record: Record, context: str) -> Record:
    """Copy only allowlisted scalar metadata; reject malformed supplied values."""
    metadata: Record = {}
    for field in _METADATA_FIELDS[dataset]:
        if field not in record:
            continue
        value = record[field]
        if value is None:
            if field in {
                "ticket_id",
                "call_id",
                "event_id",
                "session_id",
                "device_id",
                "release_id",
                "closed_at",
            }:
                metadata[field] = None
                continue
            raise ValueError(f"{context}: metadata {field} cannot be null")
        if field in _ID_PREFIXES:
            _identifier(value, field, context)
        elif field in _CATEGORIES:
            if not isinstance(value, str) or value not in _CATEGORIES[field]:
                raise ValueError(f"{context}: invalid metadata category {field}")
        elif field in _INTEGER_RANGES:
            lower, upper = _INTEGER_RANGES[field]
            if type(value) is not int or value < lower or (upper is not None and value > upper):
                raise ValueError(f"{context}: invalid numeric metadata {field}")
        elif field.endswith("_at"):
            try:
                timestamp = value
                if isinstance(value, str):
                    if (
                        re.fullmatch(
                            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z",
                            value,
                        )
                        is None
                    ):
                        raise ValueError("Invalid UTC timestamp")
                    timestamp = datetime.fromisoformat(value)
                if (
                    not isinstance(timestamp, datetime)
                    or timestamp.tzinfo is None
                    or timestamp.utcoffset() is None
                ):
                    raise ValueError("Timestamp must be aware")
            except (ValueError, TypeError, OverflowError):
                raise ValueError(f"{context}: invalid metadata timestamp {field}") from None
        elif field == "version":
            if (
                not isinstance(value, str)
                or re.fullmatch(
                    r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", value
                )
                is None
            ):
                raise ValueError(f"{context}: invalid release version metadata")
        metadata[field] = deepcopy(value)
    return metadata


def _ticket_segments(record: Record, context: str) -> list[Record]:
    """Extract subject, description, and optional resolution in their fixed source order."""
    segments: list[Record] = []
    for field in ("subject", "description", "resolution"):
        text = _text(record, field, context, optional=field == "resolution")
        if text is not None:
            segments.append({"field": field, "text": text})
    return segments


def _call_segments(record: Record, context: str) -> list[Record]:
    """Keep ordered speaker-labeled turns and their original offsets as separate segments."""
    transcript = record.get("transcript")
    if not isinstance(transcript, list) or not transcript:
        raise ValueError(f"{context}: transcript must be a nonempty list of turns")
    segments: list[Record] = []
    previous = 0
    for index, turn in enumerate(transcript):
        turn_context = f"{context}, turn {index}"
        if not isinstance(turn, dict):
            raise ValueError(f"{turn_context}: turn must be an object")
        speaker = turn.get("speaker")
        if speaker not in ("customer", "agent"):
            raise ValueError(f"{turn_context}: invalid speaker")
        offset = turn.get("offset_seconds")
        if type(offset) not in (int, float):
            raise ValueError(f"{turn_context}: offset_seconds must be a number")
        try:
            valid = math.isfinite(offset) and offset >= previous
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError(f"{turn_context}: offsets must be finite, nonnegative, and ordered")
        if record.get("duration_seconds") is not None and offset > record["duration_seconds"]:
            raise ValueError(f"{turn_context}: offset exceeds call duration")
        previous = offset
        text = _text(turn, "text", turn_context)
        if text is not None:
            segments.append(
                {
                    "field": "transcript",
                    "text": text,
                    "speaker": speaker,
                    "turn_index": index,
                    "offset_seconds": offset,
                }
            )
    return segments


def _survey_segments(record: Record, context: str) -> list[Record]:
    """Extract only nonblank comments; never manufacture text from survey scores."""
    text = _text(record, "comment", context, optional=True)
    return [] if text is None else [{"field": "comment", "text": text}]


def _release_segments(record: Record, context: str) -> list[Record]:
    """Keep release notes and individual issues distinct, preserving original issue indices."""
    segments: list[Record] = []
    notes = _text(record, "release_notes", context)
    if notes is not None:
        segments.append({"field": "release_notes", "text": notes})
    issues = record.get("known_issues")
    if not isinstance(issues, list):
        raise ValueError(f"{context}: known_issues must be a list of strings")
    for index, issue in enumerate(issues):
        if not isinstance(issue, str):
            raise ValueError(f"{context}: known_issues must contain only strings")
        if issue.strip():
            segments.append({"field": "known_issues", "text": issue, "issue_index": index})
    return segments


def extract_rag_text(dataset: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract labeled segments and allowlisted metadata from verified curated records.

    Preserve input record order and exact primary IDs. Preserve text strings,
    punctuation, and masking placeholders without normalization or chunking.
    Transcript turn_index and known-issue issue_index are zero-based original
    positions; blank text is skipped without renumbering subsequent positions.
    Duplicate IDs are rejected even when a record would produce no document.

    Omit records with no nonblank segments. Metadata includes only supplied fields
    from the dataset allowlist, never whole records or unrelated free text. Output
    dictionaries/lists are independent, and inputs are not modified. This is text
    extraction, not complete business/relationship validation or privacy approval.
    A separate privacy stage must approve text and metadata before embedding or
    Qdrant publication; regex masking cannot guarantee complete PII removal.

    Raises:
        ValueError: For unsupported datasets, malformed records/metadata/text,
            invalid or duplicated primary IDs, or malformed transcript turns.
    """
    if not isinstance(dataset, str) or dataset not in _PRIMARY_KEYS:
        raise ValueError("Unsupported RAG dataset")
    if not isinstance(records, list):
        raise ValueError(f"{dataset}: records must be a list")
    extractors = {
        "support_tickets": _ticket_segments,
        "call_transcripts": _call_segments,
        "survey_responses": _survey_segments,
        "product_releases": _release_segments,
    }
    documents: list[Record] = []
    identifiers: set[str] = set()
    for position, record in enumerate(records, 1):
        context = f"{dataset}, record {position}"
        if not isinstance(record, dict):
            raise ValueError(f"{context}: record must be an object")
        field = _PRIMARY_KEYS[dataset]
        identifier = _identifier(record.get(field), field, context)
        if identifier in identifiers:
            raise ValueError(f"{context}: duplicate primary identifier")
        identifiers.add(identifier)
        metadata = _metadata(dataset, record, context)
        segments = extractors[dataset](record, context)
        if segments:
            documents.append(
                {
                    "dataset": dataset,
                    "source_record_id": identifier,
                    "segments": segments,
                    "metadata": metadata,
                }
            )
    return documents

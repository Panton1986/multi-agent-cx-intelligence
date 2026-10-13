"""Basic demonstration-level PII masking, not production-grade anonymization."""

import re
from collections.abc import Mapping, Sequence
from hashlib import sha256

_EMAIL_PATTERN = re.compile(
    r"(?<![\w.+-])[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+"
    r"@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+\b",
    re.IGNORECASE,
)
_PHONE_PATTERN = re.compile(
    r"""
    (?<![\w+])
    (?:
        (?:\+61[ \t.-]*(?:\(0\)[ \t.-]*)?|0)
        [23478](?:[ \t.-]*[0-9]){8}
        | \(0[23478]\)(?:[ \t.-]*[0-9]){8}
        | (?:1300|1800)(?:[ \t.-]*[0-9]){6}
        | 13(?:[ \t.-]*[0-9]){4}
    )
    (?!\w)
    (?![ \t.-]*[0-9])
    """,
    re.VERBOSE,
)
# Capture complete digit sequences before checking their length and checksum.
# Spaces and hyphens are permitted; longer sequences are not truncated to a card.
_CARD_PATTERN = re.compile(r"(?<!\w)[0-9](?:[ \t-]*[0-9]){12,}(?!\w)")
_PROTECTED_FIELDS = frozenset(
    {
        "customer_id",
        "ticket_id",
        "call_id",
        "event_id",
        "session_id",
        "device_id",
        "release_id",
        "survey_id",
        "log_id",
        "created_at",
        "released_at",
        "occurred_at",
        "opened_at",
        "closed_at",
        "started_at",
        "ended_at",
        "submitted_at",
    }
)


def _passes_luhn(digits: str) -> bool:
    """Check a 13-19 digit card candidate, rejecting all-zero sequences."""
    if not 13 <= len(digits) <= 19 or not any(digit != "0" for digit in digits):
        return False
    checksum = 0
    for position, character in enumerate(reversed(digits)):
        value = int(character)
        if position % 2 == 1:
            value *= 2
            if value > 9:
                value -= 9
        checksum += value
    return checksum % 10 == 0


def _mask_card(match: re.Match[str]) -> str:
    """Mask a numeric candidate only when its length and Luhn checksum pass."""
    candidate = match.group(0)
    digits = re.sub(r"[^0-9]", "", candidate)
    return "[CARD_REDACTED]" if _passes_luhn(digits) else candidate


def mask_text(text: str) -> str:
    """Mask emails, Australian phone numbers, and Luhn-valid payment cards.

    Phone detection covers domestic mobile/landline numbers, +61 variants,
    parenthesized area codes, and 13/1300/1800 service numbers. Card detection
    accepts spaces and hyphens. These patterns provide basic demonstration
    coverage and cannot identify every PII format or guarantee anonymization.
    """
    masked = _EMAIL_PATTERN.sub("[EMAIL_REDACTED]", text)
    masked = _CARD_PATTERN.sub(_mask_card, masked)
    return _PHONE_PATTERN.sub("[PHONE_REDACTED]", masked)


def mask_customer_record(customer: Mapping[str, object]) -> dict[str, object]:
    """Return a copy with a redacted name and deterministic synthetic email.

    Preserve customer_id and every other field unchanged. Derive the synthetic
    email from the customer ID's SHA-256 digest without using the original email.

    Raises:
        ValueError: If customer_id is missing or is not a nonempty string.
    """
    customer_id = customer.get("customer_id")
    if not isinstance(customer_id, str) or not customer_id.strip():
        raise ValueError("customer_id must be a nonempty string")
    masked = dict(customer)
    digest = sha256(customer_id.encode("utf-8")).hexdigest()[:32]
    masked["full_name"] = "[REDACTED]"
    masked["email"] = f"customer-{digest}@example.invalid"
    return masked


def mask_free_text_fields(
    record: Mapping[str, object], fields: Sequence[str]
) -> dict[str, object]:
    """Return a copy with masking applied to specified string fields only.

    Identifiers and schema timestamp fields remain unchanged even if requested.
    Missing fields, non-string values, and all other fields remain unchanged.
    """
    masked = dict(record)
    for field in fields:
        if field in _PROTECTED_FIELDS:
            continue
        value = masked.get(field)
        if isinstance(value, str):
            masked[field] = mask_text(value)
    return masked


def mask_call_transcript(record: Mapping[str, object]) -> dict[str, object]:
    """Return a copy with PII masked in each conversation turn's text.

    Copy the transcript list and each turn without modifying the input. Preserve
    all other record and turn fields, including identifiers, timestamps, speakers,
    and offsets.

    Raises:
        ValueError: If transcript is missing, empty, or not a list of mappings,
            or a turn has an invalid speaker, text, or offset_seconds.
    """
    transcript = record.get("transcript")
    if not isinstance(transcript, list) or not transcript:
        raise ValueError("transcript must be a nonempty list of conversation turns")

    masked_turns: list[dict[str, object]] = []
    for position, turn in enumerate(transcript, start=1):
        if not isinstance(turn, Mapping):
            raise ValueError(f"Transcript turn {position} must be a mapping")
        if turn.get("speaker") not in ("customer", "agent"):
            raise ValueError(f"Transcript turn {position}: speaker must be customer or agent")
        text = turn.get("text")
        if not isinstance(text, str):
            raise ValueError(f"Transcript turn {position}: text must be a string")
        offset = turn.get("offset_seconds")
        if type(offset) not in (int, float) or not 0 <= offset < float("inf"):
            raise ValueError(
                f"Transcript turn {position}: offset_seconds must be a finite nonnegative number"
            )

        masked_turn = dict(turn)
        masked_turn["text"] = mask_text(text)
        masked_turns.append(masked_turn)

    masked_record = dict(record)
    masked_record["transcript"] = masked_turns
    return masked_record

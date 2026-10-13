"""Generate fictional customer feedback linked to existing Stage I records."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random
from typing import NotRequired, TypedDict


type Record = Mapping[str, object]


class SurveyResponse(TypedDict):
    """A survey response matching the documented synthetic data schema."""

    survey_id: str
    customer_id: str
    submitted_at: str
    survey_type: str
    csat_score: int
    comment: NotRequired[str]
    ticket_id: NotRequired[str]
    call_id: NotRequired[str]
    event_id: NotRequired[str]
    session_id: NotRequired[str]
    device_id: NotRequired[str]


@dataclass(frozen=True)
class _SurveySource:
    """Validated source context for one eligible survey."""

    customer_id: str
    earliest_at: datetime
    context: str
    topic: str
    references: dict[str, str]


def _required_string(record: Record, field: str) -> str:
    """Read a required nonempty string from an input record."""
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Each input record must contain a nonempty {field}")
    return value


def _utc_timestamp(record: Record, field: str) -> datetime:
    """Parse a required ISO 8601 timestamp with an explicit UTC timezone."""
    try:
        value = datetime.fromisoformat(_required_string(record, field).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO 8601 UTC timestamp") from error
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must have an explicit UTC timezone")
    return value.astimezone(UTC)


def _index_records(
    records: Sequence[Record], id_field: str, customers: Mapping[str, datetime]
) -> dict[str, Record]:
    """Index unique source records and reject orphan customer references."""
    indexed: dict[str, Record] = {}
    for record in records:
        identifier = _required_string(record, id_field)
        if identifier in indexed:
            raise ValueError(f"{id_field} values must be unique")
        if _required_string(record, "customer_id") not in customers:
            raise ValueError(f"Every {id_field} record must reference an existing customer")
        indexed[identifier] = record
    return indexed


def _session_references(record: Record) -> dict[str, str]:
    """Copy optional session/device identifiers without inventing values."""
    references = {
        field: _required_string(record, field)
        for field in ("session_id", "device_id")
        if record.get(field) is not None
    }
    if "session_id" in references and "device_id" not in references:
        raise ValueError("A source session_id requires device_id")
    return references


def generate_survey_responses(
    customers: Sequence[Record],
    tickets: Sequence[Record],
    calls: Sequence[Record],
    events: Sequence[Record],
    count: int = 1000,
    seed: int = 42,
) -> list[SurveyResponse]:
    """Return exactly count context-dependent synthetic survey responses.

    Rotate across available survey types and use distinct sources within each
    type before reuse. General surveys reference customers only. Linked calls
    also reference their ticket when present, and all links share one customer.
    Preserve available session/device IDs without changing source records.

    Submission is strictly after account creation and source activity, including
    ticket closure for resolved cases, and no later than 2026-10-11 UTC. Skip
    sources with no valid submission window. Scores favor the relevant outcome
    but permit variation; fictional comments acknowledge both score and context.
    Randomness uses a local seeded generator. Zero count returns an empty list.

    Args:
        customers: Previously generated customer records.
        tickets: Previously generated support tickets.
        calls: Previously generated call transcripts.
        events: Previously generated journey events.
        count: Number of survey responses to return.
        seed: Seed for selection, timestamps, scores, and feedback variations.

    Raises:
        ValueError: If count is negative, required input fields are invalid,
            identifiers are duplicated, linked records conflict or are orphaned,
            or no source can produce a valid survey before the cutoff.
    """
    if count < 0:
        raise ValueError("count must not be negative")
    if count == 0:
        return []

    rng = Random(seed)
    cutoff = datetime(2026, 10, 11, 23, 59, 59, tzinfo=UTC)
    customer_created: dict[str, datetime] = {}
    for customer in customers:
        customer_id = _required_string(customer, "customer_id")
        if customer_id in customer_created:
            raise ValueError("customer_id values must be unique")
        customer_created[customer_id] = _utc_timestamp(customer, "created_at")
    ticket_lookup = _index_records(tickets, "ticket_id", customer_created)
    call_lookup = _index_records(calls, "call_id", customer_created)
    event_lookup = _index_records(events, "event_id", customer_created)
    pools: dict[str, list[_SurveySource]] = {
        "post_support": [], "post_journey": [], "general": []
    }
    topics = {
        "payment_failure": "Payment processing",
        "app_crash": "Application stability",
        "login_problem": "Signing in",
        "other": "Account help and support responsiveness",
    }
    status_context = {
        "open": "unresolved", "in_progress": "unresolved", "escalated": "escalated",
        "resolved": "resolved", "closed": "resolved",
    }
    ticket_times: dict[str, datetime] = {}

    def add_source(
        survey_type: str, customer_id: str, activity_at: datetime,
        context: str, topic: str, references: dict[str, str],
    ) -> None:
        """Add a source only when there is time for a strictly later survey."""
        earliest_at = max(customer_created[customer_id], activity_at) + timedelta(seconds=1)
        if earliest_at <= cutoff:
            pools[survey_type].append(
                _SurveySource(customer_id, earliest_at, context, topic, references)
            )

    for ticket_id, ticket in ticket_lookup.items():
        status = _required_string(ticket, "status")
        issue_type = _required_string(ticket, "issue_type")
        if status not in status_context or issue_type not in topics:
            raise ValueError("Unsupported ticket status or issue_type")
        activity_at = _utc_timestamp(ticket, "opened_at")
        if status in {"resolved", "closed"}:
            closed_at = _utc_timestamp(ticket, "closed_at")
            if closed_at <= activity_at:
                raise ValueError("Ticket closure must follow opening")
            activity_at = closed_at
        ticket_times[ticket_id] = activity_at
        references = _session_references(ticket)
        references["ticket_id"] = ticket_id
        add_source(
            "post_support", _required_string(ticket, "customer_id"), activity_at,
            status_context[status], topics[issue_type], references,
        )

    call_context = {"resolved": "resolved", "follow_up": "unresolved", "escalated": "escalated"}
    for call_id, call in call_lookup.items():
        customer_id = _required_string(call, "customer_id")
        outcome = _required_string(call, "outcome")
        issue_type = _required_string(call, "issue_type")
        if outcome not in call_context or issue_type not in topics:
            raise ValueError("Unsupported call outcome or issue_type")
        activity_at = _utc_timestamp(call, "ended_at")
        if activity_at <= _utc_timestamp(call, "started_at"):
            raise ValueError("Call end must follow call start")
        references = _session_references(call)
        references["call_id"] = call_id
        if call.get("ticket_id") is not None:
            ticket_id = _required_string(call, "ticket_id")
            ticket = ticket_lookup.get(ticket_id)
            if ticket is None or _required_string(ticket, "customer_id") != customer_id:
                raise ValueError("Call ticket references must resolve to the same customer")
            if call_context[outcome] != status_context[_required_string(ticket, "status")]:
                raise ValueError("Call outcome must agree with the linked ticket status")
            if issue_type != _required_string(ticket, "issue_type"):
                raise ValueError("Call and ticket issue types must agree")
            for field, value in _session_references(ticket).items():
                if field in references and references[field] != value:
                    raise ValueError("Call and ticket session/device references conflict")
                references.setdefault(field, value)
            references["ticket_id"] = ticket_id
            activity_at = max(activity_at, ticket_times[ticket_id])
        add_source(
            "post_support", customer_id, activity_at,
            call_context[outcome], topics[issue_type], references,
        )

    event_topics = {
        "payment_attempt": "Payment processing", "app_crash": "Application stability",
        "login_attempt": "Signing in", "journey_completed": "Purchasing experience",
        "support_contact": "Support responsiveness",
    }
    for event_id, event in event_lookup.items():
        outcome = _required_string(event, "outcome")
        if outcome not in {"success", "failure"}:
            raise ValueError("Event outcomes must be success or failure")
        event_type = _required_string(event, "event_type")
        references = _session_references(event)
        references["event_id"] = event_id
        add_source(
            "post_journey", _required_string(event, "customer_id"),
            _utc_timestamp(event, "occurred_at"), outcome,
            event_topics.get(event_type, "Application navigation"), references,
        )
    for customer_id, created_at in customer_created.items():
        add_source("general", customer_id, created_at, "general", "Overall experience", {})

    available_types = [survey_type for survey_type, sources in pools.items() if sources]
    if not available_types:
        raise ValueError("No eligible survey sources exist before the cutoff")
    rng.shuffle(available_types)
    for sources in pools.values():
        rng.shuffle(sources)
    positions = dict.fromkeys(available_types, 0)
    score_weights = {
        "escalated": (45, 35, 12, 6, 2), "unresolved": (25, 35, 25, 10, 5),
        "resolved": (3, 5, 12, 40, 40), "failure": (45, 35, 12, 6, 2),
        "success": (3, 5, 12, 35, 45), "general": (10, 15, 25, 30, 20),
    }
    context_text = {
        "resolved": "My support case was resolved.",
        "escalated": "My support case needed specialist escalation.",
        "unresolved": "My support case still needs follow-up.",
        "failure": "This journey step failed.",
        "success": "This journey step worked.",
        "general": "This feedback is about my overall customer experience.",
    }
    feedback = {
        "negative": ("The process was frustrating.", "The experience fell short of expectations."),
        "mixed": ("There is room for improvement.", "The experience was acceptable, but uneven."),
        "positive": ("The experience was smooth.", "The process was clear and reassuring."),
        "positive_problem": (
            "Despite the problem, the guidance was helpful.",
            "I appreciated the communication even though more work was needed.",
        ),
        "negative_success": (
            "The outcome was positive, but the process was frustrating.",
            "Despite the result, the effort involved was disappointing.",
        ),
    }
    surveys: list[SurveyResponse] = []
    for index in range(count):
        survey_type = available_types[index % len(available_types)]
        sources = pools[survey_type]
        source = sources[positions[survey_type] % len(sources)]
        positions[survey_type] += 1
        score = rng.choices((1, 2, 3, 4, 5), weights=score_weights[source.context], k=1)[0]
        tone = "negative" if score <= 2 else "mixed" if score == 3 else "positive"
        if score <= 2 and source.context in {"resolved", "success"}:
            tone = "negative_success"
        elif score >= 4 and source.context in {"escalated", "unresolved", "failure"}:
            tone = "positive_problem"
        delay = rng.randint(0, min(604800, int((cutoff - source.earliest_at).total_seconds())))
        submitted_at = source.earliest_at + timedelta(seconds=delay)
        survey: SurveyResponse = {
            "survey_id": f"sur_{index + 1:06d}",
            "customer_id": source.customer_id,
            "submitted_at": submitted_at.isoformat().replace("+00:00", "Z"),
            "survey_type": survey_type,
            "csat_score": score,
            "comment": (
                f"{source.topic}: {context_text[source.context]} {rng.choice(feedback[tone])}"
            ),
        }
        for field, value in source.references.items():
            survey[field] = value
        surveys.append(survey)

    return surveys

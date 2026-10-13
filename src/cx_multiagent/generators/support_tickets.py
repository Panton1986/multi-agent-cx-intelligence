"""Generate synthetic support tickets using existing customers and journey events."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from random import Random
from typing import NotRequired, TypedDict


class SupportTicket(TypedDict):
    """A support ticket matching the documented synthetic data schema."""

    ticket_id: str
    customer_id: str
    opened_at: str
    channel: str
    issue_type: str
    subject: str
    description: str
    status: str
    priority: str
    escalation_level: int
    closed_at: NotRequired[str]
    resolution: NotRequired[str]
    session_id: NotRequired[str]
    device_id: NotRequired[str]
    release_id: NotRequired[str]


type TicketSource = tuple[dict[str, str], datetime, str]


def _required_string(record: Mapping[str, object], field: str) -> str:
    """Read a required nonempty string without modifying an input record."""
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Each input record must contain a nonempty {field}")
    return value


def _utc_timestamp(record: Mapping[str, object], field: str) -> datetime:
    """Parse an ISO 8601 timestamp with an explicit UTC timezone."""
    try:
        value = datetime.fromisoformat(_required_string(record, field).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO 8601 UTC timestamp") from error
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must have an explicit UTC timezone")
    return value.astimezone(UTC)


def generate_support_tickets(
    customers: Sequence[Mapping[str, object]],
    events: Sequence[Mapping[str, object]],
    count: int = 1500,
    seed: int = 42,
) -> list[SupportTicket]:
    """Return exactly count tickets without changing customers or events.

    Prefer distinct payment, crash, login failure, and support contact events.
    Exhaust eligible events before reusing them. Support contacts inherit the
    latest preceding failure category in their session when available. If no
    relevant events exist, create general customer-only tickets without invented
    session, device, or release references. Supplied release references are
    assumed valid in the source dataset because releases are not an input here.

    Every ticket opens after its source event and customer creation time. Only
    resolved or closed tickets contain closed_at and a final resolution. The
    first five tickets cover all statuses; further statuses use weighted sampling.
    Zero count returns an empty list. A local RNG preserves global random state.

    Args:
        customers: Previously generated customer records.
        events: Previously generated journey events.
        count: Number of support tickets to return.
        seed: Seed for deterministic sampling and ticket details.

    Raises:
        ValueError: If count is negative, customers are empty for positive count,
            input identifiers are duplicated, references are orphaned, or required
            input fields and session/device relationships are invalid.
    """
    if count < 0:
        raise ValueError("count must not be negative")
    if count == 0:
        return []
    if not customers:
        raise ValueError("customers must be nonempty for a positive count")

    rng = Random(seed)
    customer_created: dict[str, datetime] = {}
    for customer in customers:
        customer_id = _required_string(customer, "customer_id")
        if customer_id in customer_created:
            raise ValueError("customer_id values must be unique")
        customer_created[customer_id] = _utc_timestamp(customer, "created_at")

    failure_types = {
        "payment_attempt": "payment_failure",
        "app_crash": "app_crash",
        "login_attempt": "login_problem",
    }
    relevant: list[TicketSource] = []
    event_ids: set[str] = set()
    session_owners: dict[str, tuple[str, str, str | None]] = {}
    parsed_events: list[tuple[dict[str, str], datetime]] = []
    for event in events:
        source = {
            field: _required_string(event, field)
            for field in ("event_id", "customer_id", "event_type", "outcome")
        }
        if source["event_id"] in event_ids:
            raise ValueError("event_id values must be unique")
        event_ids.add(source["event_id"])
        if source["customer_id"] not in customer_created:
            raise ValueError("Every event must reference an existing customer")
        if source["outcome"] not in {"success", "failure"}:
            raise ValueError("Event outcomes must be success or failure")
        for field in ("session_id", "device_id", "release_id"):
            if event.get(field) is not None:
                source[field] = _required_string(event, field)
        if "session_id" in source:
            if "device_id" not in source:
                raise ValueError("An event session_id requires device_id")
            owner = (source["customer_id"], source["device_id"], source.get("release_id"))
            if session_owners.setdefault(source["session_id"], owner) != owner:
                raise ValueError("Each session must have one customer, device, and release")
        parsed_events.append((source, _utc_timestamp(event, "occurred_at")))

    session_issues: dict[str, str] = {}
    for source, event_at in sorted(parsed_events, key=lambda item: item[1]):
        event_type = source["event_type"]
        issue_type = failure_types.get(event_type) if source["outcome"] == "failure" else None
        session_id = source.get("session_id")
        if issue_type is not None:
            if session_id is not None:
                session_issues[session_id] = issue_type
            relevant.append((source, event_at, issue_type))
        elif event_type == "support_contact":
            relevant.append((source, event_at, session_issues.get(session_id or "", "other")))
    rng.shuffle(relevant)

    ticket_text = {
        "payment_failure": (
            "Payment could not be completed",
            "The customer reported a failed payment attempt and requested checkout assistance.",
            "Verified payment status and helped the customer complete a successful retry.",
        ),
        "app_crash": (
            "Application closed unexpectedly",
            "The customer reported an application crash that interrupted their journey.",
            "Provided recovery steps and confirmed the application could resume normally.",
        ),
        "login_problem": (
            "Unable to sign in",
            "The customer reported a rejected login attempt and requested account access help.",
            "Guided the customer through session recovery and confirmed successful sign-in.",
        ),
        "other": (
            "Assistance with account or product use",
            "The customer requested clarification about account settings or product navigation.",
            "Explained the relevant settings and confirmed the customer's question was answered.",
        ),
    }
    statuses = ("open", "in_progress", "escalated", "resolved", "closed")
    channels = ("email", "chat", "phone", "in_app")
    customer_ids = list(customer_created)
    tickets: list[SupportTicket] = []

    for index in range(count):
        if relevant:
            source, event_at, issue_type = (
                relevant[index] if index < len(relevant) else rng.choice(relevant)
            )
            customer_id = source["customer_id"]
            earliest_open = max(event_at, customer_created[customer_id])
        else:
            source = {}
            customer_id = rng.choice(customer_ids)
            earliest_open = customer_created[customer_id]
            issue_type = "other"
        opened_at = earliest_open + timedelta(seconds=rng.randint(60, 86400))
        status = (
            statuses[index]
            if index < len(statuses)
            else rng.choices(statuses, weights=(20, 25, 15, 25, 15), k=1)[0]
        )
        subject, description, resolution = ticket_text[issue_type]
        ticket: SupportTicket = {
            "ticket_id": f"tkt_{index + 1:06d}",
            "customer_id": customer_id,
            "opened_at": opened_at.isoformat().replace("+00:00", "Z"),
            "channel": rng.choice(channels),
            "issue_type": issue_type,
            "subject": subject,
            "description": description,
            "status": status,
            "priority": rng.choices(
                ("low", "medium", "high", "urgent"),
                weights=(5, 25, 50, 20) if issue_type == "app_crash" else (20, 50, 25, 5),
                k=1,
            )[0],
            "escalation_level": rng.randint(1, 3) if status == "escalated" else 0,
        }
        for field in ("session_id", "device_id", "release_id"):
            if field in source:
                ticket[field] = source[field]
        if status in {"resolved", "closed"}:
            closed_at = opened_at + timedelta(seconds=rng.randint(3600, 259200))
            ticket["closed_at"] = closed_at.isoformat().replace("+00:00", "Z")
            ticket["resolution"] = resolution
        tickets.append(ticket)

    return tickets

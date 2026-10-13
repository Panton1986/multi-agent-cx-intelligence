"""Generate fictional support calls linked to existing customer tickets."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from random import Random
from typing import NotRequired, TypedDict


class TranscriptTurn(TypedDict):
    """One chronological turn in a fictional support conversation."""

    speaker: str
    text: str
    offset_seconds: int


class CallTranscript(TypedDict):
    """A call record matching the documented synthetic data schema."""

    call_id: str
    customer_id: str
    started_at: str
    ended_at: str
    duration_seconds: int
    issue_type: str
    outcome: str
    transcript: list[TranscriptTurn]
    ticket_id: NotRequired[str]
    session_id: NotRequired[str]
    device_id: NotRequired[str]


type TicketRecord = tuple[dict[str, str], datetime, datetime | None]


def _required_string(record: Mapping[str, object], field: str) -> str:
    """Read a required nonempty string from an input record."""
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Each input record must contain a nonempty {field}")
    return value


def _utc_timestamp(record: Mapping[str, object], field: str) -> datetime:
    """Parse a timestamp with an explicit ISO 8601 UTC timezone."""
    try:
        value = datetime.fromisoformat(_required_string(record, field).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO 8601 UTC timestamp") from error
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must have an explicit UTC timezone")
    return value.astimezone(UTC)


def generate_call_transcripts(
    customers: Sequence[Mapping[str, object]],
    tickets: Sequence[Mapping[str, object]],
    count: int = 500,
    seed: int = 42,
) -> list[CallTranscript]:
    """Return exactly count fictional calls linked to existing support tickets.

    Use distinct eligible tickets before sampling them again. Calls last 60 to
    1,200 seconds, start strictly after opening, and end no later than closure
    for resolved/closed tickets. Exclude terminal tickets whose window cannot
    fit a 60-second call after opening. Outcomes reflect ticket status.
    Reuse available session/device IDs and leave all inputs unchanged.

    Dialogue, turn offsets, and unique call references vary conversations.
    Do not copy customer names or free-text ticket content. A local seeded RNG
    preserves global random state. Zero count returns an empty list.

    Args:
        customers: Previously generated customer records.
        tickets: Previously generated support tickets.
        count: Number of calls to return.
        seed: Seed for deterministic selection, timing, and dialogue.

    Raises:
        ValueError: If count is negative, input fields or timestamps are invalid,
            identifiers are duplicated, ticket customer references are orphaned,
            or no eligible tickets exist for a positive count.
    """
    if count < 0:
        raise ValueError("count must not be negative")
    if count == 0:
        return []

    rng = Random(seed)
    customer_created: dict[str, datetime] = {}
    for customer in customers:
        customer_id = _required_string(customer, "customer_id")
        if customer_id in customer_created:
            raise ValueError("customer_id values must be unique")
        customer_created[customer_id] = _utc_timestamp(customer, "created_at")

    outcomes = {
        "resolved": "resolved",
        "closed": "resolved",
        "escalated": "escalated",
        "open": "follow_up",
        "in_progress": "follow_up",
    }
    issue_types = {"payment_failure", "app_crash", "login_problem", "other"}
    eligible: list[TicketRecord] = []
    ticket_ids: set[str] = set()
    for ticket in tickets:
        source = {
            field: _required_string(ticket, field)
            for field in ("ticket_id", "customer_id", "issue_type", "status")
        }
        if source["ticket_id"] in ticket_ids:
            raise ValueError("ticket_id values must be unique")
        ticket_ids.add(source["ticket_id"])
        if source["customer_id"] not in customer_created:
            raise ValueError("Every ticket must reference an existing customer")
        if source["issue_type"] not in issue_types or source["status"] not in outcomes:
            raise ValueError("Ticket issue_type or status is unsupported")
        for field in ("session_id", "device_id"):
            if ticket.get(field) is not None:
                source[field] = _required_string(ticket, field)
        if "session_id" in source and "device_id" not in source:
            raise ValueError("A ticket session_id requires device_id")
        opened_at = _utc_timestamp(ticket, "opened_at")
        if opened_at < customer_created[source["customer_id"]]:
            raise ValueError("Ticket opening must not precede customer account creation")
        closed_at = None
        if source["status"] in {"resolved", "closed"}:
            closed_at = _utc_timestamp(ticket, "closed_at")
            if closed_at <= opened_at:
                raise ValueError("Ticket closure must follow opening")
            if int((closed_at - opened_at).total_seconds()) < 61:
                continue
        eligible.append((source, opened_at, closed_at))
    if not eligible:
        raise ValueError("At least one eligible support ticket is required for a positive count")
    rng.shuffle(eligible)

    # Variants for problem, diagnostic question, response, and troubleshooting.
    dialogue = {
        "payment_failure": (
            ("My payment failed at checkout.", "Checkout stopped when I tried to pay."),
            ("Did checkout show an error?", "Did an order confirmation appear?"),
            ("I saw a payment failure message.", "The order was not confirmed."),
            ("Let's check order status first.", "Let's verify status before retrying."),
        ),
        "app_crash": (
            ("The app closed unexpectedly.", "The app stopped when I resumed it."),
            ("Were you returning to the app?", "Which screen was open when it stopped?"),
            ("I was returning to checkout.", "It stopped on the checkout screen."),
            ("Let's restart and reopen checkout.", "Let's close and relaunch the app."),
        ),
        "login_problem": (
            ("I cannot sign in to my account.", "Sign-in keeps returning an error."),
            ("Does it mention session expiry?", "Does selecting sign-in show an error?"),
            ("It returns to sign-in with an error.", "My sign-in attempt is rejected."),
            ("Let's start a fresh sign-in session.", "Let's use the secure recovery flow."),
        ),
        "other": (
            ("I need help with account settings.", "I have a product navigation question."),
            ("Which option are you looking for?", "Which part of navigation is unclear?"),
            ("I need notification preferences.", "I cannot locate notification settings."),
            ("Let's open profile settings.", "Let's review the account settings menu."),
        ),
    }
    result_text = {
        "resolved": (
            ("That worked; I can continue now.", "I have checked; the issue is resolved."),
            ("We have confirmed the resolution.", "I will record the successful recovery."),
        ),
        "follow_up": (
            ("I still need help after those steps.", "The issue remains when I try again."),
            ("I will arrange a follow-up review.", "We will continue investigating."),
        ),
        "escalated": (
            ("Those steps did not fix the issue.", "I still cannot continue after retrying."),
            ("I will escalate this to a specialist.", "A specialist will review next steps."),
        ),
    }
    greetings = ("Thank you for calling support.", "Welcome to customer support.")
    acknowledgements = ("Thank you for your help.", "Thank you for explaining the next steps.")
    calls: list[CallTranscript] = []

    for index in range(count):
        source, opened_at, closed_at = (
            eligible[index] if index < len(eligible) else rng.choice(eligible)
        )
        maximum_duration = 1200
        maximum_delay = 86400
        if closed_at is not None:
            window_seconds = int((closed_at - opened_at).total_seconds())
            maximum_duration = min(maximum_duration, window_seconds - 1)
        duration = rng.randint(60, maximum_duration)
        if closed_at is not None:
            maximum_delay = min(maximum_delay, window_seconds - duration)
        started_at = opened_at + timedelta(seconds=rng.randint(1, maximum_delay))
        ended_at = started_at + timedelta(seconds=duration)
        call_id = f"cal_{index + 1:06d}"
        outcome = outcomes[source["status"]]
        problem, question, response, troubleshooting = dialogue[source["issue_type"]]
        customer_result, agent_result = result_text[outcome]
        turns = (
            ("agent", f"{rng.choice(greetings)} Your call reference is {call_id}."),
            ("customer", rng.choice(problem)),
            ("agent", rng.choice(question)),
            ("customer", rng.choice(response)),
            ("agent", rng.choice(troubleshooting)),
            ("customer", rng.choice(customer_result)),
            ("agent", rng.choice(agent_result)),
            ("customer", rng.choice(acknowledgements)),
        )
        offsets = [0, *sorted(rng.sample(range(1, duration), len(turns) - 1))]
        transcript: list[TranscriptTurn] = [
            {"speaker": speaker, "text": text, "offset_seconds": offset}
            for (speaker, text), offset in zip(turns, offsets, strict=True)
        ]
        call: CallTranscript = {
            "call_id": call_id,
            "customer_id": source["customer_id"],
            "ticket_id": source["ticket_id"],
            "started_at": started_at.isoformat().replace("+00:00", "Z"),
            "ended_at": ended_at.isoformat().replace("+00:00", "Z"),
            "duration_seconds": duration,
            "issue_type": source["issue_type"],
            "outcome": outcome,
            "transcript": transcript,
        }
        for field in ("session_id", "device_id"):
            if field in source:
                call[field] = source[field]
        calls.append(call)

    return calls

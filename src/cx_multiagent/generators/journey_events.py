"""Generate complete synthetic customer journey sessions without external I/O."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from random import Random
from typing import NotRequired, TypedDict


class JourneyEvent(TypedDict):
    """A journey event matching the documented synthetic data schema."""

    event_id: str
    customer_id: str
    occurred_at: str
    session_id: str
    device_id: str
    channel: str
    event_type: str
    outcome: str
    release_id: str
    error_code: NotRequired[str]
    properties: NotRequired[dict[str, str]]


def _required_string(record: Mapping[str, object], field: str) -> str:
    """Read a required nonempty string without changing the input record."""
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Each input record must contain a nonempty {field}")
    return value


def _utc_timestamp(record: Mapping[str, object], field: str) -> datetime:
    """Parse a required ISO 8601 timestamp with an explicit UTC timezone."""
    value = _required_string(record, field)
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO 8601 UTC timestamp") from error
    if timestamp.tzinfo is None or timestamp.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must have an explicit UTC timezone")
    return timestamp.astimezone(UTC)


def generate_journey_events(
    customers: Sequence[Mapping[str, object]],
    releases: Sequence[Mapping[str, object]],
    count: int = 15000,
    seed: int = 42,
) -> list[JourneyEvent]:
    """Return exactly count events grouped into complete chronological sessions.

    Reuse supplied customer and release IDs, leaving inputs unchanged. Each
    customer/platform pair reuses one device; each session has one release.
    Timestamps follow account creation and release availability and remain
    within 2026-10-11 UTC. Randomness uses a local seeded generator.

    Cycle through seven scenarios without truncating a purchase or retry flow.
    Fill short remainders with complete support-contact sessions. A one-record
    remainder represents a standalone support contact, with no purchase flow.
    Small counts cannot cover every scenario. Mobile crash sessions require an
    eligible Android or iOS release; all other sessions may use any platform.
    Successful events omit error_code, and all events omit ticket_id.

    Args:
        customers: Previously generated customer records.
        releases: Previously generated product release records.
        count: Number of events; zero returns an empty list without input validation.
        seed: Seed for deterministic scenario details and timestamps.

    Raises:
        ValueError: If count is negative, required input fields are invalid,
            identifiers are duplicated, or a scheduled scenario cannot fit before
            the cutoff using the supplied customers and releases.
    """
    if count < 0:
        raise ValueError("count must not be negative")
    if count == 0:
        return []
    if not customers or not releases:
        raise ValueError("customers and releases must be nonempty for a positive count")

    customer_records = [
        (_required_string(customer, "customer_id"), _utc_timestamp(customer, "created_at"))
        for customer in customers
    ]
    release_records = [
        (
            _required_string(release, "release_id"),
            _required_string(release, "platform"),
            _utc_timestamp(release, "released_at"),
        )
        for release in releases
    ]
    if len({record[0] for record in customer_records}) != len(customer_records):
        raise ValueError("customer_id values must be unique")
    if len({record[0] for record in release_records}) != len(release_records):
        raise ValueError("release_id values must be unique")
    if any(platform not in {"android", "ios", "web"} for _, platform, _ in release_records):
        raise ValueError("Release platforms must be android, ios, or web")

    rng = Random(seed)
    cutoff = datetime(2026, 10, 11, 23, 59, 59, tzinfo=UTC)
    # Each candidate starts strictly after both referenced records' timestamps.
    candidates = [
        (customer_id, release_id, platform, max(created_at, released_at) + timedelta(seconds=1))
        for customer_id, created_at in customer_records
        for release_id, platform, released_at in release_records
        if max(created_at, released_at) + timedelta(seconds=1) <= cutoff
    ]
    purchase = (
        ("session_started", None),
        ("login_attempt", None),
        ("product_view", None),
        ("checkout_started", None),
        ("payment_attempt", None),
        ("journey_completed", None),
    )
    support = (("session_started", None), ("support_contact", None))
    scenarios = (
        ("successful_purchase", purchase),
        ("payment_failure", purchase[:4] + (("payment_attempt", "PAYMENT_DECLINED"),)),
        (
            "payment_retry",
            purchase[:4] + (("payment_attempt", "PAYMENT_GATEWAY_TIMEOUT"),) + purchase[4:],
        ),
        ("login_failure", (("session_started", None), ("login_attempt", "LOGIN_INVALID_PASSWORD"))),
        (
            "login_retry",
            purchase[:1] + (("login_attempt", "LOGIN_SESSION_EXPIRED"),) + purchase[1:],
        ),
        ("mobile_crash", purchase[:3] + (("app_crash", "APP_UNHANDLED_EXCEPTION"),)),
        ("support_contact", support),
    )
    eligible_cache: dict[tuple[int, bool], list[tuple[str, str, str, datetime]]] = {}
    devices: dict[tuple[str, str], str] = {}
    events: list[JourneyEvent] = []
    session_number = 0

    while len(events) < count:
        scenario_name, steps = scenarios[session_number % len(scenarios)]
        remaining = count - len(events)
        if len(steps) > remaining:
            scenario_name = "support_contact"
            steps = support if remaining >= 2 else (("support_contact", None),)

        mobile_only = scenario_name == "mobile_crash"
        cache_key = (len(steps), mobile_only)
        if cache_key not in eligible_cache:
            latest_start = cutoff - timedelta(seconds=len(steps) - 1)
            eligible_cache[cache_key] = [
                candidate
                for candidate in candidates
                if candidate[3] <= latest_start
                and (not mobile_only or candidate[2] in {"android", "ios"})
            ]
        eligible = eligible_cache[cache_key]
        if not eligible:
            raise ValueError(
                f"No eligible customer/release pair for {scenario_name} before the cutoff"
            )

        customer_id, release_id, platform, earliest_start = rng.choice(eligible)
        available_seconds = int((cutoff - earliest_start).total_seconds())
        gaps = [rng.randint(5, 60) for _ in range(len(steps) - 1)]
        if sum(gaps) > available_seconds:
            gaps = [1] * (len(steps) - 1)
        start_offset = rng.randint(0, available_seconds - sum(gaps))
        occurred_at = earliest_start + timedelta(seconds=start_offset)
        session_number += 1
        session_id = f"ses_{session_number:06d}"
        device_key = (customer_id, platform)
        if device_key not in devices:
            devices[device_key] = f"dev_{len(devices) + 1:06d}"

        for step_index, (event_type, error_code) in enumerate(steps):
            if step_index > 0:
                occurred_at += timedelta(seconds=gaps[step_index - 1])
            event: JourneyEvent = {
                "event_id": f"evt_{len(events) + 1:06d}",
                "customer_id": customer_id,
                "occurred_at": occurred_at.isoformat().replace("+00:00", "Z"),
                "session_id": session_id,
                "device_id": devices[device_key],
                "channel": "web" if platform == "web" else "mobile",
                "event_type": event_type,
                "outcome": "failure" if error_code is not None else "success",
                "release_id": release_id,
                "properties": {"scenario": scenario_name, "platform": platform},
            }
            if error_code is not None:
                event["error_code"] = error_code
            events.append(event)

    return events

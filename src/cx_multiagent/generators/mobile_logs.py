"""Generate synthetic mobile diagnostics linked to existing journey events."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from random import Random
from typing import NotRequired, TypedDict


class MobileLog(TypedDict):
    """A mobile log matching the documented synthetic data schema."""

    log_id: str
    customer_id: str
    occurred_at: str
    session_id: str
    device_id: str
    release_id: str
    platform: str
    os_version: str
    log_level: str
    log_type: str
    message: str
    event_id: NotRequired[str]
    error_code: NotRequired[str]
    stack_trace: NotRequired[str]


type MobileEvent = tuple[dict[str, str], str, datetime]


def _required_string(record: Mapping[str, object], field: str) -> str:
    """Read a required nonempty string without modifying the input."""
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


def generate_mobile_logs(
    events: Sequence[Mapping[str, object]],
    releases: Sequence[Mapping[str, object]],
    count: int = 5000,
    seed: int = 42,
) -> list[MobileLog]:
    """Return exactly count logs referencing eligible existing mobile events.

    Ignore web events and mobile events after 2026-10-11 UTC. Preserve all linked
    identifiers and the release platform, without modifying inputs. Keep one OS
    version per device. Cover each available log type first when count permits,
    then sample events with replacement. Missing source failure categories are
    not invented. Diagnostic text and crash traces are entirely synthetic.

    Timestamps follow the event and release by up to 30 seconds, bounded by the
    cutoff. A local seeded random generator leaves global random state unchanged.
    Zero count returns an empty list without validating inputs.

    Args:
        events: Previously generated journey events.
        releases: Previously generated product releases.
        count: Number of logs to return.
        seed: Seed for sampling, timestamps, and OS versions.

    Raises:
        ValueError: If count is negative, input references or required fields are
            invalid, relationships conflict, or no eligible mobile events exist.
    """
    if count < 0:
        raise ValueError("count must not be negative")
    if count == 0:
        return []

    rng = Random(seed)
    cutoff = datetime(2026, 10, 11, 23, 59, 59, tzinfo=UTC)
    release_lookup: dict[str, tuple[str, datetime]] = {}
    for release in releases:
        release_id = _required_string(release, "release_id")
        if release_id in release_lookup:
            raise ValueError("release_id values must be unique")
        platform = _required_string(release, "platform")
        if platform not in {"android", "ios", "web"}:
            raise ValueError("Release platforms must be android, ios, or web")
        release_lookup[release_id] = (platform, _utc_timestamp(release, "released_at"))

    failure_types = {
        "payment_attempt": "payment_failure",
        "app_crash": "app_crash",
        "login_attempt": "login_problem",
    }
    candidates: list[MobileEvent] = []
    grouped: dict[str, list[MobileEvent]] = {}
    event_ids: set[str] = set()
    sessions: dict[str, tuple[str, str, str]] = {}
    device_platforms: dict[str, str] = {}
    fields = (
        "event_id", "customer_id", "session_id", "device_id", "release_id", "event_type", "outcome"
    )

    for event in events:
        channel = _required_string(event, "channel")
        if channel == "web":
            continue
        if channel != "mobile":
            raise ValueError("Event channels must be mobile or web")
        source = {field: _required_string(event, field) for field in fields}
        if source["event_id"] in event_ids:
            raise ValueError("Mobile event_id values must be unique")
        event_ids.add(source["event_id"])
        release = release_lookup.get(source["release_id"])
        if release is None or release[0] not in {"android", "ios"}:
            raise ValueError("Mobile events must reference an existing Android or iOS release")
        platform, released_at = release
        event_at = _utc_timestamp(event, "occurred_at")
        if event_at < released_at:
            raise ValueError("Mobile event timestamps must not precede their release")
        session_key = (source["customer_id"], source["device_id"], source["release_id"])
        if sessions.setdefault(source["session_id"], session_key) != session_key:
            raise ValueError("Each session must have one customer, device, and release")
        if device_platforms.setdefault(source["device_id"], platform) != platform:
            raise ValueError("Each device must have a consistent platform")
        outcome = source["outcome"]
        if outcome not in {"success", "failure"}:
            raise ValueError("Event outcomes must be success or failure")
        if source["event_type"] == "app_crash" and outcome != "failure":
            raise ValueError("app_crash events must fail")
        if source["event_type"] == "journey_completed" and outcome != "success":
            raise ValueError("journey_completed events must succeed")
        log_type = "activity"
        if outcome == "failure":
            log_type = failure_types.get(source["event_type"], "")
            if not log_type:
                raise ValueError("Unsupported failed mobile event type")
        source["log_type"] = log_type
        if event_at <= cutoff:
            candidate = (source, platform, event_at)
            candidates.append(candidate)
            grouped.setdefault(log_type, []).append(candidate)

    if not candidates:
        raise ValueError("At least one eligible mobile event is required for a positive count")

    diagnostics = {
        "payment_failure": ("PAYMENT_REQUEST_FAILED", "Payment failed; checkout state retained."),
        "app_crash": ("APP_UNHANDLED_EXCEPTION", "Unhandled exception in screen lifecycle."),
        "login_problem": ("AUTHENTICATION_REJECTED", "Authentication rejected; retry required."),
    }
    os_versions = {"android": ("13", "14", "15", "16"), "ios": ("17.6", "18.0", "18.5", "18.6")}
    stack_traces = {
        "android": (
            "java.lang.IllegalStateException: synthetic screen state failure\n"
            "    at com.example.cx.ScreenController.onResume(ScreenController.java:87)\n"
            "    at com.example.cx.MainActivity.onResume(MainActivity.java:42)"
        ),
        "ios": (
            "SyntheticScreenStateError: invalid screen state\n"
            "0 CXApp ScreenController.resume() + 87\n"
            "1 CXApp ApplicationCoordinator.activate() + 42"
        ),
    }
    available_types = [
        log_type
        for log_type in ("payment_failure", "app_crash", "login_problem", "activity")
        if log_type in grouped
    ]
    device_os: dict[str, str] = {}
    logs: list[MobileLog] = []

    for index in range(count):
        pool = grouped[available_types[index]] if index < len(available_types) else candidates
        source, platform, event_at = rng.choice(pool)
        device_id = source["device_id"]
        if device_id not in device_os:
            device_os[device_id] = rng.choice(os_versions[platform])
        delay = rng.randint(0, min(30, int((cutoff - event_at).total_seconds())))
        occurred_at = event_at + timedelta(seconds=delay)
        log_type = source["log_type"]
        log: MobileLog = {
            "log_id": f"log_{index + 1:06d}",
            "customer_id": source["customer_id"],
            "occurred_at": occurred_at.isoformat().replace("+00:00", "Z"),
            "session_id": source["session_id"],
            "device_id": device_id,
            "release_id": source["release_id"],
            "platform": platform,
            "os_version": device_os[device_id],
            "log_level": "INFO" if log_type == "activity" else "ERROR",
            "log_type": log_type,
            "message": "Journey activity processed successfully.",
            "event_id": source["event_id"],
        }
        if log_type != "activity":
            log["error_code"], log["message"] = diagnostics[log_type]
            if log_type == "app_crash":
                log["stack_trace"] = stack_traces[platform]
        logs.append(log)

    return logs

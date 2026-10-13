"""Generate reproducible synthetic product releases for Stage I."""

from datetime import UTC, datetime, timedelta
from random import Random
from typing import TypedDict


class ProductRelease(TypedDict):
    """A product release matching the documented synthetic data schema."""

    release_id: str
    version: str
    platform: str
    released_at: str
    release_notes: str
    known_issues: list[str]
    rollout_percentage: int


def generate_releases(count: int = 20, seed: int = 42) -> list[ProductRelease]:
    """Return synthetic releases with unique versions within each platform.

    Releases are ordered chronologically within the inclusive UTC date range
    2026-01-01 through 2026-09-30. Platforms rotate across Android, iOS, and web.
    Issue categories rotate alongside issue-free releases; the default count
    includes all three requested issue categories. App crashes affect mobile
    releases only. Identical arguments reproduce identical records without
    changing global random state.

    Args:
        count: Number of releases to generate; zero returns an empty list.
        seed: Seed for a local random number generator.

    Raises:
        ValueError: If count is negative.
    """
    if count < 0:
        raise ValueError("count must not be negative")

    rng = Random(seed)
    start_at = datetime(2026, 1, 1, tzinfo=UTC)
    end_at = datetime(2026, 9, 30, 23, 59, 59, tzinfo=UTC)
    maximum_offset = int((end_at - start_at).total_seconds())
    release_offsets = sorted(rng.randint(0, maximum_offset) for _ in range(count))
    platforms = ("android", "ios", "web")
    platform_versions = dict.fromkeys(platforms, 0)
    issue_cycle = ("payment_failure", "app_crash", "login_problem", None)
    improvements = (
        "Improved checkout progress indicators and payment status messages.",
        "Improved session recovery and sign-in guidance.",
        "Reduced screen loading times and improved navigation responsiveness.",
        "Improved accessibility labels and keyboard or touch navigation.",
        "Added clearer support contact options and ticket status updates.",
    )
    issue_notes = {
        "payment_failure": "Some payment attempts fail during gateway timeouts.",
        "app_crash": "The application may crash when resuming after a long background pause.",
        "login_problem": "Some expired sessions require a second sign-in attempt.",
    }
    releases: list[ProductRelease] = []

    for index, offset in enumerate(release_offsets):
        platform = platforms[index % len(platforms)]
        version_index = platform_versions[platform]
        minor, patch = divmod(version_index, 10)
        platform_versions[platform] += 1
        released_at = start_at + timedelta(seconds=offset)
        issue = issue_cycle[index % len(issue_cycle)]
        if platform == "web" and issue == "app_crash":
            issue = "login_problem"
        known_issues = [issue] if issue is not None else []
        release_notes = f"{platform.capitalize()} update. {rng.choice(improvements)}"
        if issue is not None:
            release_notes += f" Known issue: {issue_notes[issue]}"

        releases.append(
            {
                "release_id": f"rel_{index + 1:04d}",
                "version": f"1.{minor}.{patch}",
                "platform": platform,
                "released_at": released_at.isoformat().replace("+00:00", "Z"),
                "release_notes": release_notes,
                "known_issues": known_issues,
                "rollout_percentage": rng.choice((10, 25, 50, 75, 100)),
            }
        )

    return releases

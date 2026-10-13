"""Generate reproducible synthetic customers for Stage I data engineering."""

from datetime import UTC, datetime, timedelta

from faker import Faker


def generate_customers(count: int = 500, seed: int = 42) -> list[dict[str, str]]:
    """Return customer dictionaries matching the documented customer schema.

    Generate unique sequential IDs and example.com email addresses. Account
    creation times span 2025-01-01 through 2025-12-31, inclusive, in UTC.
    The same arguments produce the same records with the same Faker version.
    Randomness is isolated to a locally seeded Faker instance.

    Args:
        count: Number of customers to generate; zero returns an empty list.
        seed: Seed for the local Faker random number generator.

    Raises:
        ValueError: If count is negative.
    """
    if count < 0:
        raise ValueError("count must not be negative")

    fake = Faker("en_AU")
    fake.seed_instance(seed)

    start_at = datetime(2025, 1, 1, tzinfo=UTC)
    end_at = datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC)
    maximum_offset = int((end_at - start_at).total_seconds())
    country_languages = (
        ("AU", "en-AU"),
        ("NZ", "en-NZ"),
        ("GB", "en-GB"),
        ("US", "en-US"),
        ("CA", "en-CA"),
    )
    customers: list[dict[str, str]] = []

    for index in range(1, count + 1):
        full_name = fake.name()
        country_code, preferred_language = fake.random.choice(country_languages)
        created_at = start_at + timedelta(seconds=fake.random.randint(0, maximum_offset))
        customer_segment = fake.random.choices(
            ("new", "regular", "premium"), weights=(20, 65, 15), k=1
        )[0]
        account_status = fake.random.choices(
            ("active", "inactive", "closed"), weights=(85, 10, 5), k=1
        )[0]

        customers.append(
            {
                "customer_id": f"cus_{index:04d}",
                "full_name": full_name,
                "email": f"customer{index:04d}@example.com",
                "created_at": created_at.isoformat().replace("+00:00", "Z"),
                "country_code": country_code,
                "preferred_language": preferred_language,
                "customer_segment": customer_segment,
                "account_status": account_status,
            }
        )

    return customers

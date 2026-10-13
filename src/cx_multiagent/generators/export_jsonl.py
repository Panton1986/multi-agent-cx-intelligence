"""Export all seven synthetic datasets as UTF-8 JSONL files."""

import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

from cx_multiagent.generators.call_transcripts import generate_call_transcripts
from cx_multiagent.generators.customers import generate_customers
from cx_multiagent.generators.journey_events import generate_journey_events
from cx_multiagent.generators.mobile_logs import generate_mobile_logs
from cx_multiagent.generators.releases import generate_releases
from cx_multiagent.generators.support_tickets import generate_support_tickets
from cx_multiagent.generators.survey_responses import generate_survey_responses


def write_jsonl(records: Iterable[Mapping[str, object]], output_path: Path) -> int:
    """Overwrite a UTF-8 JSONL file and return the number of records written.

    Preserve nested objects and arrays through JSON serialization. Create parent
    directories when needed. Serialization and filesystem errors propagate.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    record_count = 0
    with output_path.open("w", encoding="utf-8", newline="\n") as output_file:
        for record in records:
            line = json.dumps(dict(record), ensure_ascii=False, allow_nan=False)
            output_file.write(line + "\n")
            record_count += 1
    return record_count


def export_datasets(seed: int = 42) -> dict[str, tuple[int, Path]]:
    """Generate default-size datasets and overwrite their project JSONL files.

    Build all datasets in dependency order before writing to data/generated/
    under the project root. Pass the same seed to every generator. Return each
    dataset's record count and output path. Errors propagate; files written
    before a filesystem failure are not rolled back.
    """
    customers = generate_customers(seed=seed)
    releases = generate_releases(seed=seed)
    events = generate_journey_events(customers, releases, seed=seed)
    mobile_logs = generate_mobile_logs(events, releases, seed=seed)
    tickets = generate_support_tickets(customers, events, seed=seed)
    calls = generate_call_transcripts(customers, tickets, seed=seed)
    surveys = generate_survey_responses(customers, tickets, calls, events, seed=seed)

    datasets: tuple[tuple[str, Sequence[Mapping[str, object]]], ...] = (
        ("customers", customers),
        ("product_releases", releases),
        ("customer_journey_events", events),
        ("mobile_application_logs", mobile_logs),
        ("support_tickets", tickets),
        ("call_transcripts", calls),
        ("survey_responses", surveys),
    )
    project_root = Path(__file__).resolve().parents[3]
    output_directory = project_root / "data" / "generated"
    summary: dict[str, tuple[int, Path]] = {}
    for dataset_name, records in datasets:
        output_path = output_directory / f"{dataset_name}.jsonl"
        record_count = write_jsonl(records, output_path)
        summary[dataset_name] = (record_count, output_path)
    return summary


def main() -> None:
    """Export all datasets with seed 42 and print their counts and paths."""
    for dataset_name, (record_count, output_path) in export_datasets(seed=42).items():
        print(f"{dataset_name}: {record_count} records -> {output_path}")


if __name__ == "__main__":
    main()

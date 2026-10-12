"""Manage the S3 buckets required by the data engineering pipeline."""

from typing import TypedDict

from botocore.exceptions import ClientError

from cx_multiagent.storage.s3_client import create_s3_client

REQUIRED_BUCKETS: tuple[str, ...] = ("cx-raw", "cx-quarantine", "cx-curated")


class BucketResults(TypedDict):
    """Bucket names grouped by whether they were created or already existed."""

    created: list[str]
    existing: list[str]


def ensure_buckets() -> BucketResults:
    """Create missing required buckets and report created and existing names.

    Repeated calls skip buckets that already exist. A bucket created concurrently
    and reported as already owned by this client is also considered existing.
    Successful creations are not rolled back if a later operation fails.

    Raises:
        ValueError: If required S3 environment configuration is missing.
        ClientError: If an S3 request fails unexpectedly, including access denial
            or a bucket name owned by another account.
        BotoCoreError: If the SDK encounters a connection or other client error.
    """
    client = create_s3_client()
    results: BucketResults = {"created": [], "existing": []}

    try:
        response = client.list_buckets()
        existing_buckets = {bucket["Name"] for bucket in response.get("Buckets", [])}

        for bucket_name in REQUIRED_BUCKETS:
            if bucket_name in existing_buckets:
                results["existing"].append(bucket_name)
                continue

            try:
                client.create_bucket(Bucket=bucket_name)
            except ClientError as error:
                error_code = error.response.get("Error", {}).get("Code")
                if error_code != "BucketAlreadyOwnedByYou":
                    raise
                results["existing"].append(bucket_name)
            else:
                results["created"].append(bucket_name)

            existing_buckets.add(bucket_name)

        return results
    finally:
        client.close()

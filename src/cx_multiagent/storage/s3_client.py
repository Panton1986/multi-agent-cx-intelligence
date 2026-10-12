"""Create S3 clients for local SeaweedFS storage."""

import os
from pathlib import Path

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from dotenv import load_dotenv


def create_s3_client() -> BaseClient:
    """Create a SeaweedFS S3 client using environment configuration.

    Load the project's local .env file without overriding existing environment
    variables. Client creation does not perform S3 operations.

    Raises:
        ValueError: If a required environment variable is missing or empty.
    """
    project_root = Path(__file__).resolve().parents[3]
    load_dotenv(dotenv_path=project_root / ".env", override=False)

    required_variables = (
        "SEAWEEDFS_S3_ENDPOINT",
        "SEAWEEDFS_S3_ACCESS_KEY",
        "SEAWEEDFS_S3_SECRET_KEY",
    )
    configuration = {name: os.getenv(name, "") for name in required_variables}
    missing_variables = [name for name, value in configuration.items() if not value.strip()]
    if missing_variables:
        raise ValueError("Missing required environment variables: " + ", ".join(missing_variables))

    return boto3.client(
        "s3",
        endpoint_url=configuration["SEAWEEDFS_S3_ENDPOINT"],
        aws_access_key_id=configuration["SEAWEEDFS_S3_ACCESS_KEY"],
        aws_secret_access_key=configuration["SEAWEEDFS_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )

"""Unit tests for the SeaweedFS S3 bucket manager."""

from unittest.mock import Mock, call, patch

from cx_multiagent.storage.bucket_manager import ensure_buckets


def test_ensure_buckets_all_existing() -> None:
    """Skip bucket creation when every required bucket already exists."""
    client = Mock(spec=["list_buckets", "create_bucket", "close"])
    client.list_buckets.return_value = {
        "Buckets": [
            {"Name": "cx-raw"},
            {"Name": "cx-quarantine"},
            {"Name": "cx-curated"},
        ]
    }

    with patch(
        "cx_multiagent.storage.bucket_manager.create_s3_client", return_value=client
    ) as mock_create_s3_client:
        result = ensure_buckets()

    assert result["created"] == []
    assert result["existing"] == ["cx-raw", "cx-quarantine", "cx-curated"]
    mock_create_s3_client.assert_called_once_with()
    client.create_bucket.assert_not_called()
    client.list_buckets.assert_called_once_with()
    client.close.assert_called_once_with()


def test_ensure_buckets_creates_missing() -> None:
    """Create only missing buckets and report each bucket's status."""
    client = Mock(spec=["list_buckets", "create_bucket", "close"])
    client.list_buckets.return_value = {"Buckets": [{"Name": "cx-raw"}]}

    with patch(
        "cx_multiagent.storage.bucket_manager.create_s3_client", return_value=client
    ) as mock_create_s3_client:
        result = ensure_buckets()

    assert result == {
        "created": ["cx-quarantine", "cx-curated"],
        "existing": ["cx-raw"],
    }
    assert client.create_bucket.call_count == 2
    assert client.create_bucket.call_args_list == [
        call(Bucket="cx-quarantine"),
        call(Bucket="cx-curated"),
    ]
    mock_create_s3_client.assert_called_once_with()
    client.list_buckets.assert_called_once_with()
    client.close.assert_called_once_with()

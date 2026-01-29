from unittest import mock

import pytest
from mypy_boto3_s3 import S3Client


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for tests that interact with S3."""

    with mock.patch("mavedb.routers.score_sets.s3_client") as mock_s3_client_func:
        mock_s3 = mock.MagicMock(spec=S3Client)
        mock_s3_client_func.return_value = mock_s3
        yield mock_s3

"""Unit tests for S3Resource and RDSResource definitions and configurations."""

from unittest.mock import MagicMock, patch
import pytest

from dagster_project.resources import S3Resource, RDSResource


def test_s3_resource_configuration():
    s3 = S3Resource(
        bucket_name="my-bronze-bucket",
        prefix="bronze/",
        region_name="eu-west-1",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
    )

    assert s3.bucket_name == "my-bronze-bucket"
    assert s3.prefix == "bronze/"
    assert s3.region_name == "eu-west-1"
    assert s3.endpoint_url == "http://localhost:4566"


def test_s3_resource_list_objects():
    s3 = S3Resource(bucket_name="my-bucket")

    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "bronze/raw_data_1.csv",
                    "Size": 500,
                    "LastModified": MagicMock(isoformat=lambda: "2026-10-11T12:00:00Z"),
                    "ETag": '"etag-123"',
                },
                {
                    "Key": "bronze/ignored_folder/",
                    "Size": 0,
                    "LastModified": MagicMock(isoformat=lambda: "2026-10-11T12:00:00Z"),
                    "ETag": '"etag-000"',
                },
                {
                    "Key": "bronze/notes.txt",
                    "Size": 200,
                    "LastModified": MagicMock(isoformat=lambda: "2026-10-11T12:00:00Z"),
                    "ETag": '"etag-txt"',
                },
            ]
        }
    ]

    with patch.object(S3Resource, "get_client", return_value=mock_client):
        objects = s3.list_objects(prefix="bronze/", extension=".csv")
        assert len(objects) == 1
        assert objects[0]["key"] == "bronze/raw_data_1.csv"
        assert objects[0]["etag"] == "etag-123"


def test_rds_resource_url():
    rds = RDSResource(
        host="rds-instance.aws.internal",
        port=5432,
        database="production_db",
        username="dbadmin",
        password="secretpassword",
    )

    url = rds.get_connection_url()
    assert url == "postgresql+psycopg2://dbadmin:secretpassword@rds-instance.aws.internal:5432/production_db"


def test_rds_resource_connection_string_override():
    rds = RDSResource(connection_string="postgresql+psycopg2://custom:pass@custom-host:5433/custom_db")
    assert rds.get_connection_url() == "postgresql+psycopg2://custom:pass@custom-host:5433/custom_db"


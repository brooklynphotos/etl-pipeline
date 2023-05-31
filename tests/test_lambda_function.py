import json
import os
from unittest.mock import patch, MagicMock

import pytest

from event_examples import S3_RECORDS
from lambda_function import extract, transform


@patch("lambda_function.ExtractService")
@patch.dict(
    os.environ,
    {
        "S3_EXTRACT_PREFIX": "extract_prefix",
        "S3_BUCKET": "bucket",
        "S3_BACKUP_PREFIX": "backup_prefix",
    },
)
def test_extract(mock_extract_svc: MagicMock):
    event = {"source_url": "https://somewhere.com/some.zip"}
    saved_s3_url = "s3://bucket/extract_prefix/some.zip"
    mock_extract_svc().extract = MagicMock(return_value=saved_s3_url)
    assert extract(event, None) == {
        "statusCode": 200,
        "body": saved_s3_url,
    }

    [_, [args, _]] = mock_extract_svc.call_args_list

    assert args[1:] == ("bucket", "extract_prefix", "backup_prefix", "/tmp")


@patch("lambda_function.TransformerService")
@patch.dict(
    os.environ,
    {
        "S3_PREFIX": "extract_prefix",
        "S3_BUCKET": "bucket",
        "GLUE_DATABASE": "mydb",
        "GLUE_TABLE": "mytable",
    },
)
def test_transform(mock_svc: MagicMock):
    event = S3_RECORDS
    saved_s3_url = "s3://bucket/extract_prefix/some.zip"
    mock_svc().transform = MagicMock(return_value=saved_s3_url)
    assert transform(event, None) == {
        "statusCode": 200,
        "body": saved_s3_url,
    }

    [_, [args, _]] = mock_svc.call_args_list

    assert args == ("bucket", "extract_prefix", "mydb", "mytable")


@patch("lambda_function.TransformerService")
@patch.dict(
    os.environ,
    {
        "S3_PREFIX": "extract_prefix",
        "S3_BUCKET": "bucket",
        "GLUE_DATABASE": "mydb",
        "GLUE_TABLE": "mytable",
    },
)
def test_transform_not_csv(mock_svc: MagicMock):
    event = _clone_(S3_RECORDS)
    event["Records"][0]["s3"]["object"]["key"] = "Somethingelse.txt"
    saved_s3_url = "s3://bucket/extract_prefix/some.zip"
    mock_svc().transform = MagicMock(return_value=saved_s3_url)
    with pytest.raises(ValueError) as ve:
        transform(event, None)
    assert (
        str(ve.value)
        == f"Object changed is not a csv file (sourcebucket). Check configuration of event producer"
    )


def _clone_(obj: dict) -> dict:
    return json.loads(json.dumps(obj))

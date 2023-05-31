from unittest.mock import MagicMock, patch

import pytest

from services.extraction import ExtractService


@patch("services.extraction.requests.get")
@patch("services.extraction.BytesIO")
@patch("services.extraction.ZipFile")
def test_extract(mock_zipfile_cls: MagicMock, _: MagicMock, mock_req_get: MagicMock):
    s3_client = MagicMock()
    s3_bucket = "bucket1"
    s3_extract_prefix = "extract_prefix"
    s3_backup_prefix = "backup_prefix/"
    tmp_dir = "/tmp"
    mock_resp = MagicMock()
    # set up the request call
    mock_resp.status_code = 200
    mock_req_get.return_value = mock_resp

    # set up the zip
    unzipped_file = "abc.csv"
    mock_zip_handler = MagicMock()
    mock_zip_handler.namelist.return_value = [unzipped_file]
    mock_zipfile_cls.return_value.__enter__.return_value = mock_zip_handler

    svc = ExtractService(
        s3_client, s3_bucket, s3_extract_prefix, s3_backup_prefix, tmp_dir
    )
    zip_file = "xyz.zip"
    src_url = f"https://abc.com/{zip_file}"
    res = svc.extract(src_url)
    assert res == f"s3://{s3_bucket}/{s3_extract_prefix}/{unzipped_file}"
    [[upload_file_call_args, _]] = s3_client.upload_file.call_args_list
    assert upload_file_call_args == (
        f"{tmp_dir}/{unzipped_file}",
        s3_bucket,
        f"{s3_extract_prefix}/{unzipped_file}",
    )
    [[_, put_object_call_args]] = s3_client.put_object.call_args_list
    assert put_object_call_args["Bucket"] == s3_bucket
    assert put_object_call_args["Key"] == f"{s3_backup_prefix}{zip_file}"


@patch("services.extraction.requests.get")
@patch("services.extraction.BytesIO")
@patch("services.extraction.ZipFile")
def test_extract_check_request_bad(
    mock_zipfile_cls: MagicMock, _: MagicMock, mock_req_get: MagicMock
):
    s3_client = MagicMock()
    s3_bucket = "bucket1"
    s3_extract_prefix = "extract_prefix"
    s3_backup_prefix = "backup_prefix/"
    tmp_dir = "/tmp"
    mock_resp = MagicMock()
    # set up the request call
    mock_resp.status_code = 400
    mock_req_get.return_value = mock_resp

    svc = ExtractService(
        s3_client, s3_bucket, s3_extract_prefix, s3_backup_prefix, tmp_dir
    )
    zip_file = "xyz.zip"
    src_url = f"https://abc.com/{zip_file}"
    with pytest.raises(ValueError) as ve:
        svc.extract(src_url)

    assert (
        str(ve.value)
        == f"Failed to download the zip file from {src_url}: {mock_resp.status_code}"
    )


@patch("services.extraction.requests.get")
@patch("services.extraction.BytesIO")
@patch("services.extraction.ZipFile")
def test_extract_check_zip_has_csv(
    mock_zipfile_cls: MagicMock, _: MagicMock, mock_req_get: MagicMock
):
    s3_client = MagicMock()
    s3_bucket = "bucket1"
    s3_extract_prefix = "extract_prefix"
    s3_backup_prefix = "backup_prefix/"
    tmp_dir = "/tmp"
    mock_resp = MagicMock()
    # set up the request call
    mock_resp.status_code = 200
    mock_req_get.return_value = mock_resp

    # set up the zip
    unzipped_file = "abc.txt"
    mock_zip_handler = MagicMock()
    mock_zip_handler.namelist.return_value = [unzipped_file]
    mock_zipfile_cls.return_value.__enter__.return_value = mock_zip_handler

    svc = ExtractService(
        s3_client, s3_bucket, s3_extract_prefix, s3_backup_prefix, tmp_dir
    )
    zip_file = "xyz.zip"
    src_url = f"https://abc.com/{zip_file}"
    with pytest.raises(ValueError) as ve:
        svc.extract(src_url)

    assert str(ve.value) == "The zip file needs to have exactly one CSV file"


@patch("services.extraction.requests.get")
@patch("services.extraction.BytesIO")
@patch("services.extraction.ZipFile")
def test_extract_check_zip_has_one_csv(
    mock_zipfile_cls: MagicMock, _: MagicMock, mock_req_get: MagicMock
):
    s3_client = MagicMock()
    s3_bucket = "bucket1"
    s3_extract_prefix = "extract_prefix"
    s3_backup_prefix = "backup_prefix/"
    tmp_dir = "/tmp"
    mock_resp = MagicMock()
    # set up the request call
    mock_resp.status_code = 200
    mock_req_get.return_value = mock_resp

    # set up the zip
    mock_zip_handler = MagicMock()
    mock_zip_handler.namelist.return_value = ["abc.csv", "xyz.csv"]
    mock_zipfile_cls.return_value.__enter__.return_value = mock_zip_handler

    svc = ExtractService(
        s3_client, s3_bucket, s3_extract_prefix, s3_backup_prefix, tmp_dir
    )
    zip_file = "xyz.zip"
    src_url = f"https://abc.com/{zip_file}"
    with pytest.raises(ValueError) as ve:
        svc.extract(src_url)

    assert str(ve.value) == "too many values to unpack (expected 1)"

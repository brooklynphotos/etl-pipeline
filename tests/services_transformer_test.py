from unittest.mock import MagicMock, patch

import pytest

from services.transformer import TransformerService


@patch("services.transformer.wr")
def test_transform(mock_wrangler: MagicMock):
    s3_bucket = "bucket1"
    s3_prefix = "extract_prefix"
    db_name = "mydb"
    table_name = "mytable"

    src_bucket = "src_bucket"
    src_prefix = "src_prefix"

    mock_df = MagicMock()
    mock_wrangler.s3.read_csv.return_value = mock_df
    svc = TransformerService(s3_bucket, s3_prefix, db_name, table_name)
    res = svc.transform(src_bucket, src_prefix)

    assert res == f"s3://{s3_bucket}/{s3_prefix}"

    [[_, wr_args]] = mock_wrangler.s3.to_parquet.call_args_list

    assert wr_args == dict(
        df=mock_df,
        path=res,
        dataset=True,
        partition_cols=["Country"],
        mode="overwrite",
        database=db_name,
        table=table_name,
    )

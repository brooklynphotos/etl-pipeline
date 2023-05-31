""" Lambda function module
Each function represents the handler of a Lambda function
"""
import os
import boto3

from services.extraction import ExtractService
from services.transformer import TransformerService
import urllib.parse

s3_client = boto3.client("s3")


def extract(event, context):
    """Lambda handler for extracting data from remote source"""
    stored_url = ExtractService(
        s3_client,
        os.getenv("S3_BUCKET"),
        os.getenv("S3_EXTRACT_PREFIX"),
        os.getenv("S3_BACKUP_PREFIX"),
        "/tmp",
    ).extract(event["source_url"])
    return {"statusCode": 200, "body": stored_url}


def transform(event, context):
    """Lambda handler for extracting data from remote source"""
    record = event["Records"][0]["s3"]
    csv_bucket = record["bucket"]["name"]
    csv_key: str = urllib.parse.unquote_plus(record["object"]["key"])

    if not csv_key.endswith(".csv"):
        raise ValueError(
            f"Object changed is not a csv file ({csv_bucket}). Check configuration of event producer"
        )

    stored_url = TransformerService(
        os.getenv("S3_BUCKET"),
        os.getenv("S3_PREFIX"),
        os.getenv("GLUE_DATABASE"),
        os.getenv("GLUE_TABLE"),
    ).transform(csv_bucket, csv_key)
    return {"statusCode": 200, "body": stored_url}

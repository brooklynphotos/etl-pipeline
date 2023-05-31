import os.path
from io import BytesIO
from zipfile import ZipFile

import requests


class ExtractService:
    def __init__(
        self,
        s3_client,
        s3_bucket: str,
        s3_extract_prefix: str,
        s3_backup_prefix: str,
        tmp_dir: str,
    ):
        self._s3_bucket = s3_bucket
        self._s3_extract_prefix = s3_extract_prefix
        self._s3_backup_prefix = s3_backup_prefix
        self._s3_client = s3_client
        self._tmp_dir = tmp_dir

    def extract(self, source_url: str) -> str:
        """Saves unzipped version of the file in the given s3 url, also saves origin in backup
        @param source_url: url of the source data
        @return: s3 url of unzipped file
        """
        response = requests.get(
            source_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9"
            },
        )
        if response.status_code != 200:
            raise ValueError(
                f"Failed to download the zip file from {source_url}: {response.status_code}"
            )

        # extract zip file checking there's one and only one csv file
        with ZipFile(BytesIO(response.content), "r") as zip_fh:
            [csv_file] = zip_fh.namelist()
            if not csv_file.endswith(".csv"):
                raise ValueError("The zip file needs to have exactly one CSV file")
            zip_fh.extract(csv_file, self._tmp_dir)
            s3_file_name = f"{self._s3_extract_prefix}/{csv_file}"
            self._s3_client.upload_file(
                f"{self._tmp_dir}/{csv_file}", self._s3_bucket, s3_file_name
            )

        self._s3_client.put_object(
            Body=response.content,
            Bucket=self._s3_bucket,
            Key=f"{self._s3_backup_prefix}{os.path.basename(source_url)}",
        )
        return f"s3://{self._s3_bucket}/{s3_file_name}"

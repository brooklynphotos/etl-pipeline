import awswrangler as wr


class TransformerService:
    def __init__(
        self,
        s3_destination_bucket: str,
        s3_destination_prefix: str,
        database: str,
        table: str,
    ):
        """Setup for writing transformed data to destination
        @param s3_destination_bucket: bucket to store the parquet
        @param s3_destination_prefix: prefix
        @param database: glue catalog database
        @param table: glue catalog table
        """
        self._s3_destination_bucket = s3_destination_bucket
        self._s3_destination_prefix = s3_destination_prefix
        self._database = database
        self._table = table

    def transform(self, s3_source_bucket: str, s3_source_prefix: str) -> str:
        """Takes an s3 path describing a CSV file and converts it to parquet in an S3 path
        @param s3_source_bucket: s3 bucket hosting csv
        @param s3_source_prefix: prefix for that csv file

        @return s3 url of the newly creted file
        """
        csv_file_path = f"s3://{s3_source_bucket}/{s3_source_prefix}"
        df = wr.s3.read_csv(path=csv_file_path)
        s3_dest_url = (
            f"s3://{self._s3_destination_bucket}/{self._s3_destination_prefix}"
        )
        wr.s3.to_parquet(
            df=df,
            path=s3_dest_url,
            dataset=True,
            partition_cols=["Country"],
            mode="overwrite",
            database=self._database,
            table=self._table,
        )
        return s3_dest_url

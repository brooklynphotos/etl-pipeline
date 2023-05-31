from lambda_function import transform


def test_transform():
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "application-artifacts-gz"
                    },
                    "object": {
                        "key": "data_uncompressed/2m Sales Records.csv"
                    }
                }
            }
        ]
    }
    transform(event, None)
